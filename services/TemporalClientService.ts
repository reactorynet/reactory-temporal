/**
 * TemporalClientService — connection management and workflow lifecycle operations
 * against a Temporal cluster.
 *
 * Connections are cached per *resolved configuration* (address + namespace + auth),
 * not globally: a multi-tenant server talks to more than one cluster/namespace, and a
 * single cached client would silently route one tenant's work to another's namespace.
 *
 * All workflow operations are thin, typed wrappers over @temporalio/client so the
 * workflow steps (see ../workflow/steps) and the GraphQL resolvers share one
 * implementation of idempotent start, status mapping and result normalisation.
 */

import Reactory from '@reactorynet/reactory-core';
import { readFileSync } from 'fs';
import { createHash } from 'crypto';
import { Connection, Client, WorkflowHandle, WorkflowFailedError } from '@temporalio/client';
import { WorkflowExecutionAlreadyStartedError } from '@temporalio/common';
import {
  ITemporalConnectionConfig,
  ITemporalStartWorkflowInput,
  ITemporalStartWorkflowResult,
  ITemporalSignalWorkflowInput,
  ITemporalQueryWorkflowInput,
  ITemporalUpdateWorkflowInput,
  ITemporalDescribeResult,
  ITemporalOutcome,
  TemporalExecutionStatus,
} from '../types';

/** One cached cluster connection + client pair. */
interface CachedClient {
  client: Client;
  connection: Connection;
  /** The resolved config this entry was built from (for diagnostics). */
  resolved: ITemporalConnectionConfig;
}

const DEFAULT_ADDRESS = 'localhost:7233';
const DEFAULT_NAMESPACE = 'default';

/** Default cap on Temporal result payloads carried into workflow state. */
export const DEFAULT_MAX_RESULT_BYTES = 64 * 1024;

/**
 * Read a PEM value that may be supplied either as a filesystem path or as the PEM
 * contents themselves. The previous implementation passed the *path string* to
 * Buffer.from(), which handed Temporal the filename instead of the certificate.
 */
function readPem(value: string | undefined, label: string): Buffer | undefined {
  if (!value || value.trim().length === 0) return undefined;
  if (value.includes('-----BEGIN')) {
    return Buffer.from(value, 'utf8');
  }
  try {
    return readFileSync(value);
  } catch (error: any) {
    throw new Error(`Unable to read Temporal ${label} from "${value}": ${error.message}`);
  }
}

/** Map Temporal's status name onto the module's normalised status union. */
export function mapExecutionStatus(name: string | undefined): TemporalExecutionStatus {
  switch ((name || '').toUpperCase()) {
    case 'RUNNING':
      return 'RUNNING';
    case 'COMPLETED':
      return 'COMPLETED';
    case 'FAILED':
      return 'FAILED';
    case 'CANCELLED':
    case 'CANCELED':
      return 'CANCELED';
    case 'TERMINATED':
      return 'TERMINATED';
    case 'CONTINUED_AS_NEW':
      return 'CONTINUED_AS_NEW';
    case 'TIMED_OUT':
      return 'TIMED_OUT';
    default:
      return 'UNKNOWN';
  }
}

/** True when a status means the execution will never progress again. */
export function isTerminalStatus(status: TemporalExecutionStatus): boolean {
  return status !== 'RUNNING' && status !== 'UNKNOWN';
}

/**
 * Normalise a Temporal result into durable-safe workflow data. Workflow step
 * outputs are persisted into the engine's workflow instance, so an oversized or
 * non-serializable result must never reach them.
 */
export function normaliseResult(
  value: any,
  maxBytes: number = DEFAULT_MAX_RESULT_BYTES,
): { result?: any; truncated?: boolean; bytes?: number } {
  if (value === undefined || value === null) return { result: value ?? null };
  let serialized: string;
  try {
    serialized = JSON.stringify(value) ?? 'null';
  } catch {
    return { result: String(value) };
  }
  const bytes = Buffer.byteLength(serialized, 'utf8');
  if (bytes > maxBytes) {
    return { truncated: true, bytes };
  }
  return { result: JSON.parse(serialized) };
}

export class TemporalClientService {
  name: string = 'TemporalClientService';
  nameSpace: string = 'reactory';
  version: string = '1.0.0';

  /** Connections cached by a hash of the resolved connection config. */
  private clients: Map<string, CachedClient> = new Map();
  /** In-flight connects, so concurrent callers share one connection attempt. */
  private connecting: Map<string, Promise<CachedClient>> = new Map();
  private context: Reactory.Server.IReactoryContext | null = null;

  constructor(props?: any, context?: Reactory.Server.IReactoryContext) {
    this.context = context || null;
  }

  /**
   * Resolve the effective connection configuration.
   *
   * Resolution order (first hit wins per field):
   *   1. the explicit `config` argument
   *   2. the active partner's setting named `temporal/<connectionKey>` or `<connectionKey>`
   *      (shape: `{ data: { address, namespace, apiKey, tls } }` or a plain address string)
   *   3. named environment variables — `TEMPORAL_<KEY>_ADDRESS` / `_NAMESPACE` / `_API_KEY`
   *   4. the conventional `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` / `TEMPORAL_API_KEY`
   *   5. localhost:7233 / default
   */
  public resolveConnectionConfig(config?: ITemporalConnectionConfig): ITemporalConnectionConfig {
    const key = config?.connectionKey;
    const fromSetting = key ? this.readPartnerSetting(key) : undefined;
    const envPrefix = key ? `TEMPORAL_${key.toUpperCase().replace(/[^A-Z0-9]/g, '_')}` : null;

    const pick = (
      explicit: string | undefined,
      settingValue: string | undefined,
      namedEnv: string | undefined,
      fallbackEnv: string | undefined,
      def?: string,
    ): string | undefined => explicit || settingValue || namedEnv || fallbackEnv || def;

    return {
      connectionKey: key,
      address: pick(
        config?.address,
        fromSetting?.address,
        envPrefix ? process.env[`${envPrefix}_ADDRESS`] : undefined,
        process.env.TEMPORAL_ADDRESS,
        DEFAULT_ADDRESS,
      ),
      namespace: pick(
        config?.namespace,
        fromSetting?.namespace,
        envPrefix ? process.env[`${envPrefix}_NAMESPACE`] : undefined,
        process.env.TEMPORAL_NAMESPACE,
        DEFAULT_NAMESPACE,
      ),
      apiKey: pick(
        config?.apiKey,
        fromSetting?.apiKey,
        envPrefix ? process.env[`${envPrefix}_API_KEY`] : undefined,
        process.env.TEMPORAL_API_KEY,
      ),
      tls: config?.tls || fromSetting?.tls || this.tlsFromEnv(),
    };
  }

  /**
   * Initialise or return the cached Temporal Client for the resolved config.
   * Distinct configurations get distinct clients — see the class docblock.
   */
  async getClient(config?: ITemporalConnectionConfig): Promise<Client> {
    const resolved = this.resolveConnectionConfig(config);
    const cacheKey = this.cacheKey(resolved);

    const cached = this.clients.get(cacheKey);
    if (cached) return cached.client;

    const inFlight = this.connecting.get(cacheKey);
    if (inFlight) return (await inFlight).client;

    const attempt = this.connect(resolved, cacheKey);
    this.connecting.set(cacheKey, attempt);
    try {
      const entry = await attempt;
      return entry.client;
    } finally {
      this.connecting.delete(cacheKey);
    }
  }

  /**
   * Start a workflow, or re-attach to an already-running execution with the same
   * workflowId.
   *
   * Idempotency matters here: the Reactory workflow engine re-executes a step on
   * retry and after a process restart, so a start must not create a second
   * execution. With the default `workflowIdConflictPolicy: 'USE_EXISTING'` (or when
   * Temporal raises WorkflowExecutionAlreadyStartedError) the existing run is
   * adopted and `reattached: true` is returned.
   */
  async startWorkflow(
    input: ITemporalStartWorkflowInput,
    config?: ITemporalConnectionConfig,
  ): Promise<ITemporalStartWorkflowResult> {
    const client = await this.getClient(config);
    const options: Record<string, any> = {
      workflowId: input.workflowId,
      taskQueue: input.taskQueue,
      args: input.args || [],
      memo: input.memo,
      cronSchedule: input.cronSchedule,
      workflowExecutionTimeout: input.workflowExecutionTimeout,
      workflowRunTimeout: input.workflowRunTimeout,
      workflowTaskTimeout: input.workflowTaskTimeout,
      retry: input.retry,
    };

    // typedSearchAttributes supersedes searchAttributes (deprecated in @temporalio 1.12+),
    // but the legacy field is still accepted for callers that already use it.
    if (input.typedSearchAttributes) options.typedSearchAttributes = input.typedSearchAttributes;
    if (input.searchAttributes) options.searchAttributes = input.searchAttributes;

    // The two id policies are mutually exclusive in the Temporal SDK when the reuse
    // policy is TERMINATE_IF_RUNNING; prefer the conflict policy and only pass the
    // reuse policy when it is compatible.
    if (input.workflowIdConflictPolicy) {
      options.workflowIdConflictPolicy = input.workflowIdConflictPolicy;
    }
    if (input.workflowIdReusePolicy && input.workflowIdReusePolicy !== 'TERMINATE_IF_RUNNING') {
      options.workflowIdReusePolicy = input.workflowIdReusePolicy;
    } else if (input.workflowIdReusePolicy === 'TERMINATE_IF_RUNNING' && !input.workflowIdConflictPolicy) {
      options.workflowIdReusePolicy = input.workflowIdReusePolicy;
    }

    try {
      if (input.signalOnStart?.name) {
        const handle = await client.workflow.signalWithStart(input.workflowType, {
          ...options,
          signal: input.signalOnStart.name,
          signalArgs: input.signalOnStart.args || [],
        } as any);
        return {
          workflowId: handle.workflowId,
          // signalWithStart may have signalled a pre-existing chain, so this is the
          // run that received the signal rather than necessarily a new execution.
          runId: handle.signaledRunId || '',
          started: true,
          reattached: false,
          signalled: true,
        };
      }

      const handle = await client.workflow.start(input.workflowType, options as any);
      // With USE_EXISTING, Temporal returns a handle to the pre-existing run rather
      // than raising — compare against the requested run to detect the re-attach.
      return {
        workflowId: handle.workflowId,
        runId: handle.firstExecutionRunId || '',
        started: true,
        reattached: false,
      };
    } catch (error: any) {
      if (error instanceof WorkflowExecutionAlreadyStartedError || error?.name === 'WorkflowExecutionAlreadyStartedError') {
        const describe = await this.describeWorkflow(input.workflowId, undefined, config);
        return {
          workflowId: input.workflowId,
          runId: describe.runId,
          started: false,
          reattached: true,
        };
      }
      throw error;
    }
  }

  /**
   * Send a Signal to a running Temporal workflow. When `startIfNotRunning` details
   * are supplied by the caller, use startWorkflow with `signalOnStart` instead.
   */
  async signalWorkflow(
    input: ITemporalSignalWorkflowInput,
    config?: ITemporalConnectionConfig,
  ): Promise<void> {
    const client = await this.getClient(config);
    const handle: WorkflowHandle = client.workflow.getHandle(input.workflowId, input.runId);
    await handle.signal(input.signalName, ...(input.args || []));
  }

  /** Query a running or completed Temporal workflow. */
  async queryWorkflow<T = any>(
    input: ITemporalQueryWorkflowInput,
    config?: ITemporalConnectionConfig,
  ): Promise<T> {
    const client = await this.getClient(config);
    const handle: WorkflowHandle = client.workflow.getHandle(input.workflowId, input.runId);
    return await handle.query(input.queryType, ...(input.args || []));
  }

  /**
   * Execute a Workflow Update — the synchronous request/response primitive.
   * `waitForStage: 'ACCEPTED'` returns an update handle without awaiting the result.
   */
  async updateWorkflow<T = any>(
    input: ITemporalUpdateWorkflowInput,
    config?: ITemporalConnectionConfig,
  ): Promise<{ result?: T; updateId: string; accepted: boolean }> {
    const client = await this.getClient(config);
    const handle: any = client.workflow.getHandle(input.workflowId, input.runId);

    if (input.waitForStage === 'ACCEPTED') {
      const updateHandle = await handle.startUpdate(input.updateName, {
        args: input.args || [],
        waitForStage: 'ACCEPTED',
      });
      return { updateId: updateHandle.updateId, accepted: true };
    }

    const result = await handle.executeUpdate(input.updateName, { args: input.args || [] });
    return { result, updateId: '', accepted: true };
  }

  /** Cancel a running workflow execution (graceful). */
  async cancelWorkflow(
    workflowId: string,
    runId?: string,
    config?: ITemporalConnectionConfig,
  ): Promise<void> {
    const client = await this.getClient(config);
    const handle = client.workflow.getHandle(workflowId, runId);
    await handle.cancel();
  }

  /** Terminate a running workflow execution (immediate). */
  async terminateWorkflow(
    workflowId: string,
    reason?: string,
    runId?: string,
    config?: ITemporalConnectionConfig,
  ): Promise<void> {
    const client = await this.getClient(config);
    const handle = client.workflow.getHandle(workflowId, runId);
    await handle.terminate(reason);
  }

  /** Describe an execution and normalise the interesting metadata. */
  async describeWorkflow(
    workflowId: string,
    runId?: string,
    config?: ITemporalConnectionConfig,
  ): Promise<ITemporalDescribeResult> {
    const client = await this.getClient(config);
    const handle = client.workflow.getHandle(workflowId, runId);
    const description = await handle.describe();
    return {
      workflowId: description.workflowId,
      runId: description.runId,
      workflowType: description.type,
      taskQueue: description.taskQueue,
      status: mapExecutionStatus(description.status?.name),
      startTime: description.startTime?.toISOString(),
      closeTime: description.closeTime?.toISOString(),
      historyLength: description.historyLength,
      memo: description.memo as Record<string, any>,
    };
  }

  /**
   * Await an execution's terminal outcome and normalise it into an ITemporalOutcome.
   * Never throws for a workflow-level failure: a FAILED / TERMINATED / CANCELED run
   * is an *outcome*, and callers (the bridge, the steps) decide whether that is an
   * error in Reactory terms. Infrastructure errors still propagate.
   */
  async awaitOutcome(
    workflowId: string,
    runId?: string,
    config?: ITemporalConnectionConfig,
    maxResultBytes: number = DEFAULT_MAX_RESULT_BYTES,
  ): Promise<ITemporalOutcome> {
    const client = await this.getClient(config);
    const handle = client.workflow.getHandle(workflowId, runId);

    try {
      const raw = await handle.result();
      const normalised = normaliseResult(raw, maxResultBytes);
      return {
        workflowId,
        runId,
        status: 'COMPLETED',
        ...normalised,
      };
    } catch (error: any) {
      if (error instanceof WorkflowFailedError || error?.name === 'WorkflowFailedError') {
        // Re-describe so the precise terminal status (FAILED vs TERMINATED vs
        // TIMED_OUT vs CANCELED) is reported rather than a flat "failed".
        let status: TemporalExecutionStatus = 'FAILED';
        try {
          const description = await this.describeWorkflow(workflowId, runId, config);
          status = description.status;
        } catch {
          /* keep FAILED — describe is best-effort here */
        }
        return {
          workflowId,
          runId,
          status,
          failure: {
            message: error.message || 'Temporal workflow failed',
            type: error.cause?.name || error.name,
            cause: error.cause?.message,
          },
        };
      }
      throw error;
    }
  }

  /** Close every cached connection. Called on service dispose / server shutdown. */
  async close(): Promise<void> {
    const entries = Array.from(this.clients.values());
    this.clients.clear();
    await Promise.all(
      entries.map(async (entry) => {
        try {
          await entry.connection.close();
        } catch {
          /* best-effort */
        }
      }),
    );
  }

  /** Reactory service lifecycle hook — mirrors close(). */
  async onShutdown(): Promise<void> {
    await this.close();
  }

  // ── internals ────────────────────────────────────────────────────────────────

  private async connect(
    resolved: ITemporalConnectionConfig,
    cacheKey: string,
  ): Promise<CachedClient> {
    const tls = this.buildTlsOptions(resolved);
    const connection = await Connection.connect({
      address: resolved.address,
      apiKey: resolved.apiKey,
      tls,
    });

    const client = new Client({ connection, namespace: resolved.namespace });
    const entry: CachedClient = { client, connection, resolved };
    this.clients.set(cacheKey, entry);
    return entry;
  }

  /**
   * Build the SDK TLS options. Certificates may be given as filesystem paths or as
   * inline PEM; both are read into buffers here. Returns undefined when no TLS
   * material is configured (plaintext), or `true` when only an apiKey is present
   * (Temporal Cloud requires TLS for API-key auth).
   */
  private buildTlsOptions(resolved: ITemporalConnectionConfig): any {
    const tls = resolved.tls;
    const crt = readPem(tls?.clientCertPath, 'client certificate');
    const key = readPem(tls?.clientKeyPath, 'client key');
    const ca = readPem(tls?.serverRootCACertPath, 'server root CA certificate');

    if (!crt && !key && !ca && !tls?.serverNameOverride) {
      // API-key auth (Temporal Cloud) is only valid over TLS.
      return resolved.apiKey ? true : undefined;
    }

    return {
      clientCertPair: crt && key ? { crt, key } : undefined,
      serverRootCACertificate: ca,
      serverNameOverride: tls?.serverNameOverride,
    };
  }

  /** TLS material configured purely through the environment. */
  private tlsFromEnv(): ITemporalConnectionConfig['tls'] | undefined {
    const clientCertPath = process.env.TEMPORAL_TLS_CERT_PATH;
    const clientKeyPath = process.env.TEMPORAL_TLS_KEY_PATH;
    const serverRootCACertPath = process.env.TEMPORAL_TLS_CA_PATH;
    const serverNameOverride = process.env.TEMPORAL_TLS_SERVER_NAME;
    if (!clientCertPath && !clientKeyPath && !serverRootCACertPath && !serverNameOverride) {
      return undefined;
    }
    return { clientCertPath, clientKeyPath, serverRootCACertPath, serverNameOverride };
  }

  /**
   * Read a named connection from the active partner's settings, mirroring the
   * convention used by ReactorySQLService: a setting whose `data` is either a
   * connection object or a bare address string.
   */
  private readPartnerSetting(connectionKey: string): ITemporalConnectionConfig | undefined {
    const partner: any = this.context?.partner;
    if (!partner || typeof partner.getSetting !== 'function') return undefined;
    const setting = partner.getSetting(`temporal/${connectionKey}`) || partner.getSetting(connectionKey);
    const data = setting?.data;
    if (!data) return undefined;
    if (typeof data === 'string') return { address: data };
    return {
      address: data.address,
      namespace: data.namespace,
      apiKey: data.apiKey,
      tls: data.tls,
    };
  }

  /**
   * Cache key for a resolved config. Secrets are hashed rather than stored so the
   * key can be logged safely, and any change of credentials yields a new connection.
   */
  private cacheKey(resolved: ITemporalConnectionConfig): string {
    const secretMaterial = JSON.stringify({ apiKey: resolved.apiKey || '', tls: resolved.tls || null });
    const secretHash = createHash('sha256').update(secretMaterial).digest('hex').slice(0, 12);
    return `${resolved.address}|${resolved.namespace}|${secretHash}`;
  }
}

export const TemporalClientServiceDefinition: Reactory.Service.IReactoryServiceDefinition<TemporalClientService> = {
  id: 'reactory.TemporalClientService@1.0.0',
  name: 'TemporalClientService',
  nameSpace: 'reactory',
  version: '1.0.0',
  description: 'Temporal cluster connection management and workflow lifecycle operations',
  // MUST be a factory, not the class itself: ServiceManager activates services by
  // CALLING this field (`serviceRegister[id].service(props, context)`), so a bare
  // class constructor throws "cannot be invoked without 'new'".
  service: (
    props: Reactory.Service.IReactoryServiceProps,
    context: Reactory.Server.IReactoryContext,
  ) => new TemporalClientService(props, context),
  dependencies: [],
  serviceType: 'integration',
  lifeCycle: 'singleton',
};
