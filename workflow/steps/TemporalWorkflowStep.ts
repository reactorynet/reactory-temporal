/**
 * TemporalWorkflowStep (`temporal_workflow`) — start a Temporal execution from a
 * Reactory YAML workflow and, optionally, await its outcome.
 *
 * The step is idempotent by design. The Reactory engine re-executes a step on retry
 * and after a process restart, so `workflowId` MUST be deterministic (derived from
 * workflow inputs) and the start uses `workflowIdConflictPolicy: 'USE_EXISTING'` by
 * default: a second execution is never created, the running one is adopted and
 * `reattached: true` is reported.
 *
 * Await modes
 * ───────────
 *   none        start and continue. Pair with a core `wait_event` step if the YAML
 *               author wants to wait later in the flow.
 *   event       durable suspend. The step registers a bridge watch, checks whether
 *               the run already finished (closing the fast-completion race), then
 *               returns `control.waitForEvent`. The engine persists the instance and
 *               frees the worker; the bridge publishes the outcome when Temporal
 *               settles, and this same step body re-runs with the payload.
 *   sleep_poll  durable poll. Sleeps `pollInterval`, re-runs, describes the run, and
 *               repeats until terminal or `awaitTimeout`. No bridge required and no
 *               lost-wakeup window; costs one instance wake per interval.
 *   block_poll  in-step await. The only mode available on the non-durable executor;
 *               holds the step for the duration and restarts its wait after a
 *               process restart.
 *
 * `event` degrades to `sleep_poll` when the bridge service is unavailable, and both
 * degrade to `block_poll` when the executing engine cannot suspend — a suspend
 * directive that nobody can honour would hang the instance forever.
 *
 * Config (YAML `config` or `inputs`):
 *   workflowType:  "B2BMassPayoutWorkflow"      (required)
 *   workflowId:    "batch_${input.batchId}"     (required, deterministic)
 *   taskQueue:     "b2b-payouts-queue"          (defaults to $TEMPORAL_TASK_QUEUE)
 *   args:          [ { ... } ]                  (deep ${...} resolution)
 *   awaitMode:     none | event | sleep_poll | block_poll
 *   awaitTimeout:  86400000                     (ms; delivered as an event, never as
 *                                                a waitForEvent effectiveDate)
 *   pollInterval:  10000                        (ms; sleep_poll / block_poll)
 *   eventName:     "temporal.workflow.settled"  (bridge topic to await)
 *   eventKey:      "${workflowId}"              (correlation key; defaults to workflowId)
 *   resultPath:    "totalSettled"               (keeps large payloads out of the instance)
 *   failOnWorkflowFailure: true                 (false → surface FAILED as data, not error)
 *   signalOnStart: { name, args }               (signalWithStart)
 *   idConflictPolicy / idReusePolicy / memo / searchAttributes / typedSearchAttributes
 *   cronSchedule / workflowExecutionTimeout / workflowRunTimeout / workflowTaskTimeout
 *
 * Outputs: { workflowId, runId, status, started, reattached, awaited, result?,
 *            failure?, truncated?, bytes? }
 */

import {
  StepExecutionContext,
  StepExecutionResult,
  ValidationResult,
} from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/interfaces/IYamlStep';
import { BaseTemporalStep } from './BaseTemporalStep';
import {
  DEFAULT_MAX_RESULT_BYTES,
  isTerminalStatus,
  normaliseResult,
} from '../../services/TemporalClientService';
import {
  ITemporalOutcome,
  ITemporalStartWorkflowResult,
  TemporalExecutionStatus,
  TemporalWorkflowIdConflictPolicy,
  TemporalWorkflowIdReusePolicy,
} from '../../types';
import {
  DEFAULT_SETTLED_EVENT,
  TEMPORAL_EVENT_BRIDGE_SERVICE_ID,
  ITemporalEventBridge,
} from '../events';

export type TemporalAwaitMode = 'none' | 'event' | 'sleep_poll' | 'block_poll';

export const AWAIT_MODES: TemporalAwaitMode[] = ['none', 'event', 'sleep_poll', 'block_poll'];
const CONFLICT_POLICIES: TemporalWorkflowIdConflictPolicy[] = ['USE_EXISTING', 'FAIL', 'TERMINATE_EXISTING'];
const REUSE_POLICIES: TemporalWorkflowIdReusePolicy[] = [
  'ALLOW_DUPLICATE',
  'ALLOW_DUPLICATE_FAILED_ONLY',
  'REJECT_DUPLICATE',
  'TERMINATE_IF_RUNNING',
];

export const DEFAULT_AWAIT_TIMEOUT_MS = 24 * 60 * 60 * 1000; // 24 hours
export const DEFAULT_POLL_INTERVAL_MS = 10000;
/** Pseudo-status published by the bridge when a watch outlives its await deadline. */
export const AWAIT_TIMEOUT_STATUS = 'AWAIT_TIMEOUT';

/** State carried across a sleep_poll suspension. */
export interface PollState {
  temporalWorkflowId: string;
  temporalRunId?: string;
  /** Epoch ms when the await began (for waitedMs reporting). */
  startedAt: number;
  /** Epoch ms after which the await gives up. */
  deadline: number;
  started: boolean;
  reattached: boolean;
  attempts: number;
}

export class TemporalWorkflowStep extends BaseTemporalStep {
  // Typed as string, not a literal, so TemporalAwaitStep can override it while
  // inheriting this class's await machinery.
  public readonly stepType: string = 'temporal_workflow';

  protected async executeStep(context: StepExecutionContext): Promise<StepExecutionResult> {
    if (!context.reactoryContext) return this.noContext();
    const service = this.getTemporalService(context);
    if (!service) return this.serviceUnavailable({ stepType: this.stepType });

    const workflowType = this.required(this.config.workflowType, context);
    const workflowId = this.required(this.config.workflowId, context);
    const connection = this.resolveConnection(context);
    const maxResultBytes = typeof this.config.maxResultBytes === 'number'
      ? this.config.maxResultBytes
      : DEFAULT_MAX_RESULT_BYTES;
    const resultPath = this.optional(this.config.resultPath, context);

    // ── Phase 1: resumed by a bridge event ──────────────────────────────────────
    if (context.control?.eventPublished) {
      const outcome = this.outcomeFromEvent(context.control.eventData, workflowId);
      context.logger.info(
        `Temporal run ${this.describeTarget(workflowId, outcome.runId)} settled with status ${outcome.status}`,
      );
      return this.finish(outcome, { started: false, reattached: true, awaited: true }, resultPath, context, maxResultBytes);
    }

    // ── Phase 2: resumed from a sleep_poll suspension ───────────────────────────
    const pollState = context.control?.persistenceData as PollState | undefined;
    if (pollState?.temporalWorkflowId) {
      return this.continuePolling(pollState, { service, connection, resultPath, maxResultBytes }, context);
    }

    // ── Phase 3: first run ──────────────────────────────────────────────────────
    const requestedMode = (this.optional(this.config.awaitMode, context) as TemporalAwaitMode) || 'none';
    const awaitTimeout = this.numberConfig('awaitTimeout', DEFAULT_AWAIT_TIMEOUT_MS);
    const pollInterval = this.numberConfig('pollInterval', DEFAULT_POLL_INTERVAL_MS);

    let start: ITemporalStartWorkflowResult;
    try {
      start = await service.startWorkflow(
        {
          workflowType,
          workflowId,
          taskQueue: this.optional(this.config.taskQueue, context) || process.env.TEMPORAL_TASK_QUEUE || '',
          args: this.resolveDeep(this.config.args ?? [], context),
          memo: this.config.memo ? this.resolveDeep(this.config.memo, context) : undefined,
          searchAttributes: this.config.searchAttributes
            ? this.resolveDeep(this.config.searchAttributes, context)
            : undefined,
          typedSearchAttributes: this.config.typedSearchAttributes,
          cronSchedule: this.optional(this.config.cronSchedule, context),
          workflowIdConflictPolicy:
            (this.optional(this.config.idConflictPolicy, context) as TemporalWorkflowIdConflictPolicy) || 'USE_EXISTING',
          workflowIdReusePolicy: this.optional(this.config.idReusePolicy, context) as TemporalWorkflowIdReusePolicy,
          workflowExecutionTimeout: this.config.workflowExecutionTimeout,
          workflowRunTimeout: this.config.workflowRunTimeout,
          workflowTaskTimeout: this.config.workflowTaskTimeout,
          retry: this.config.retry,
          signalOnStart: this.config.signalOnStart
            ? {
                name: this.required(this.config.signalOnStart.name, context),
                args: this.resolveDeep(this.config.signalOnStart.args ?? [], context),
              }
            : undefined,
        },
        connection,
      );
    } catch (error: any) {
      context.logger.error(`Failed to start Temporal workflow "${workflowType}" (${workflowId}): ${error.message}`);
      return {
        success: false,
        error: `Failed to start Temporal workflow "${workflowType}" (${workflowId}): ${error.message}`,
        outputs: { workflowId },
        metadata: { workflowType, workflowId },
      };
    }

    context.logger.info(
      `Temporal workflow "${workflowType}" ${start.reattached ? 're-attached' : 'started'}: ` +
        `${this.describeTarget(start.workflowId, start.runId)}`,
    );

    const provenance = { started: start.started, reattached: start.reattached, awaited: false };

    if (requestedMode === 'none') {
      return {
        success: true,
        outputs: {
          workflowId: start.workflowId,
          runId: start.runId,
          status: 'RUNNING' as TemporalExecutionStatus,
          ...provenance,
          ...(start.signalled ? { signalled: true } : {}),
        },
        metadata: { workflowType, workflowId: start.workflowId, awaitMode: 'none' },
      };
    }

    // Resolve the mode we can actually honour here.
    const mode = await this.resolveAwaitMode(requestedMode, context);

    if (mode === 'block_poll') {
      return this.blockingAwait(
        start,
        { service, connection, resultPath, maxResultBytes, awaitTimeout },
        provenance,
        context,
      );
    }

    if (mode === 'event') {
      return this.suspendOnEvent(
        start,
        { service, connection, resultPath, maxResultBytes, awaitTimeout },
        provenance,
        context,
      );
    }

    // sleep_poll — check once before sleeping so a fast run never costs an interval.
    const described = await this.safeDescribe(service, start.workflowId, start.runId, connection, context);
    if (described && isTerminalStatus(described.status)) {
      const outcome = await service.awaitOutcome(start.workflowId, start.runId, connection, maxResultBytes);
      return this.finish(outcome, { ...provenance, awaited: true }, resultPath, context, maxResultBytes);
    }

    const state: PollState = {
      temporalWorkflowId: start.workflowId,
      temporalRunId: start.runId,
      startedAt: Date.now(),
      deadline: Date.now() + awaitTimeout,
      started: start.started,
      reattached: start.reattached,
      attempts: 0,
    };
    return {
      success: true,
      outputs: {
        workflowId: start.workflowId,
        runId: start.runId,
        status: 'RUNNING' as TemporalExecutionStatus,
        ...provenance,
      },
      metadata: { workflowType, workflowId: start.workflowId, awaitMode: 'sleep_poll' },
      control: { sleep: { durationMs: pollInterval }, persist: state as any },
    };
  }

  public validateConfig(config: Record<string, any>): ValidationResult {
    const errors: string[] = [];
    const warnings: string[] = [];

    if (!config.workflowType || typeof config.workflowType !== 'string') {
      errors.push('workflowType is required and must be a string');
    }
    if (!config.workflowId || typeof config.workflowId !== 'string') {
      errors.push('workflowId is required and must be a string');
    } else if (!config.workflowId.includes('${')) {
      // A static id is legitimate for a singleton workflow, so this is a warning:
      // hard-failing would block a valid design.
      warnings.push(
        'workflowId contains no ${...} reference — the id must be deterministic AND unique ' +
          'per logical run, otherwise a second run of this workflow re-attaches to the first',
      );
    }
    if (config.awaitMode !== undefined && !AWAIT_MODES.includes(config.awaitMode)) {
      errors.push(`awaitMode must be one of: ${AWAIT_MODES.join(', ')}`);
    }
    if (config.idConflictPolicy !== undefined && !CONFLICT_POLICIES.includes(config.idConflictPolicy)) {
      errors.push(`idConflictPolicy must be one of: ${CONFLICT_POLICIES.join(', ')}`);
    }
    if (config.idReusePolicy !== undefined && !REUSE_POLICIES.includes(config.idReusePolicy)) {
      errors.push(`idReusePolicy must be one of: ${REUSE_POLICIES.join(', ')}`);
    }
    for (const key of ['awaitTimeout', 'pollInterval', 'maxResultBytes']) {
      if (config[key] !== undefined && (typeof config[key] !== 'number' || config[key] <= 0)) {
        errors.push(`${key} must be a positive number`);
      }
    }
    if (config.args !== undefined && !Array.isArray(config.args)) {
      errors.push('args must be an array (Temporal workflow arguments are positional)');
    }
    if (config.signalOnStart !== undefined) {
      if (typeof config.signalOnStart !== 'object' || !config.signalOnStart.name) {
        errors.push('signalOnStart must be an object with a "name" property');
      }
    }

    return { valid: errors.length === 0, errors, warnings: warnings.length > 0 ? warnings : undefined };
  }

  // ── await strategies ─────────────────────────────────────────────────────────

  /**
   * Decide which await mode this execution can actually honour.
   *
   * `event` needs BOTH a suspend-capable engine and a live bridge to publish the
   * completion; without the bridge the instance would suspend on an event nobody
   * ever sends. `sleep_poll` needs only a suspend-capable engine.
   */
  protected async resolveAwaitMode(
    requested: TemporalAwaitMode,
    context: StepExecutionContext,
  ): Promise<TemporalAwaitMode> {
    if (requested === 'block_poll' || requested === 'none') return requested;

    if (!this.canSuspend(context)) {
      context.logger.warn(
        `Step "${this.id}": awaitMode "${requested}" needs a durable engine; ` +
          'falling back to block_poll (in-step waiting) on this executor',
      );
      return 'block_poll';
    }

    if (requested === 'event' && !this.getBridge(context)) {
      context.logger.warn(
        `Step "${this.id}": awaitMode "event" needs ${TEMPORAL_EVENT_BRIDGE_SERVICE_ID} ` +
          'to publish the completion; falling back to sleep_poll',
      );
      return 'sleep_poll';
    }

    return requested;
  }

  /** Register a bridge watch and suspend until the outcome event arrives. */
  protected async suspendOnEvent(
    start: ITemporalStartWorkflowResult,
    deps: {
      service: any;
      connection: any;
      resultPath?: string;
      maxResultBytes: number;
      awaitTimeout: number;
    },
    provenance: { started: boolean; reattached: boolean; awaited: boolean },
    context: StepExecutionContext,
  ): Promise<StepExecutionResult> {
    const eventName = this.optional(this.config.eventName, context) || DEFAULT_SETTLED_EVENT;
    const eventKey = this.optional(this.config.eventKey, context) || start.workflowId;
    const bridge = this.getBridge(context);

    // Register the watch BEFORE the terminal pre-check: if the run settles between
    // the check and the suspend, the watch is already in place to publish it, and
    // the bridge's re-publish sweep will keep trying until the pointer consumes it.
    try {
      await bridge?.watch({
        workflowId: start.workflowId,
        runId: start.runId,
        connectionKey: deps.connection?.connectionKey,
        namespace: deps.connection?.namespace,
        address: deps.connection?.address,
        eventName,
        eventKey,
        awaitTimeout: deps.awaitTimeout,
        instanceId: context.workflow?.instanceId,
        stepId: this.id,
        clientKey: (context.reactoryContext?.partner as any)?.key,
        userEmail: (context.reactoryContext?.user as any)?.email,
        maxResultBytes: deps.maxResultBytes,
      });
    } catch (error: any) {
      // A watch we cannot register means nothing will wake this instance — fail
      // rather than suspend indefinitely.
      context.logger.error(`Failed to register Temporal watch for ${start.workflowId}: ${error.message}`);
      return {
        success: false,
        error: `Failed to register Temporal completion watch for ${start.workflowId}: ${error.message}`,
        outputs: { workflowId: start.workflowId, runId: start.runId },
        metadata: { workflowId: start.workflowId },
      };
    }

    // Close the fast-completion window: if Temporal already finished, resolve now
    // instead of suspending on an event whose publish may already have been
    // discarded by the engine (an event with no live subscription is dropped).
    const described = await this.safeDescribe(deps.service, start.workflowId, start.runId, deps.connection, context);
    if (described && isTerminalStatus(described.status)) {
      const outcome = await deps.service.awaitOutcome(
        start.workflowId,
        start.runId,
        deps.connection,
        deps.maxResultBytes,
      );
      await bridge?.unwatch(start.workflowId, eventKey).catch(() => undefined);
      context.logger.info(
        `Temporal run ${this.describeTarget(start.workflowId, start.runId)} already terminal (${outcome.status}) — not suspending`,
      );
      return this.finish(outcome, { ...provenance, awaited: true }, deps.resultPath, context, deps.maxResultBytes);
    }

    context.logger.info(
      `Suspending step "${this.id}" until "${eventName}" (key: ${eventKey}) for ` +
        `${this.describeTarget(start.workflowId, start.runId)}`,
    );

    return {
      success: true,
      // These outputs are persisted BEFORE the suspend, so the resumed run (and any
      // later step) can still read the workflowId/runId this step started.
      outputs: {
        workflowId: start.workflowId,
        runId: start.runId,
        status: 'RUNNING' as TemporalExecutionStatus,
        ...provenance,
        awaitingEvent: eventName,
        awaitingEventKey: eventKey,
      },
      metadata: { workflowId: start.workflowId, awaitMode: 'event', eventName, eventKey },
      control: { waitForEvent: { eventName, eventKey } },
    };
  }

  /** One tick of a durable sleep_poll await. */
  protected async continuePolling(
    state: PollState,
    deps: { service: any; connection: any; resultPath?: string; maxResultBytes: number },
    context: StepExecutionContext,
  ): Promise<StepExecutionResult> {
    const attempts = (state.attempts || 0) + 1;
    const provenance = { started: state.started, reattached: state.reattached, awaited: true };

    const described = await this.safeDescribe(
      deps.service,
      state.temporalWorkflowId,
      state.temporalRunId,
      deps.connection,
      context,
    );

    if (described && isTerminalStatus(described.status)) {
      const outcome = await deps.service.awaitOutcome(
        state.temporalWorkflowId,
        state.temporalRunId,
        deps.connection,
        deps.maxResultBytes,
      );
      context.logger.info(
        `Temporal run ${this.describeTarget(state.temporalWorkflowId, state.temporalRunId)} ` +
          `settled after ${attempts} poll(s): ${outcome.status}`,
      );
      return this.finish(outcome, provenance, deps.resultPath, context, deps.maxResultBytes);
    }

    if (Date.now() >= state.deadline) {
      const waitedMs = Date.now() - state.startedAt;
      context.logger.error(
        `Timed out awaiting Temporal run ${state.temporalWorkflowId} after ${attempts} poll(s)`,
      );
      return {
        success: false,
        error:
          `Timed out awaiting Temporal workflow ${state.temporalWorkflowId} ` +
          `(status ${described?.status ?? 'UNKNOWN'} after ${attempts} polls)`,
        outputs: {
          workflowId: state.temporalWorkflowId,
          runId: state.temporalRunId,
          status: AWAIT_TIMEOUT_STATUS,
          ...provenance,
          waitedMs,
        },
        metadata: { workflowId: state.temporalWorkflowId, awaitMode: 'sleep_poll', attempts },
      };
    }

    const pollInterval = this.numberConfig('pollInterval', DEFAULT_POLL_INTERVAL_MS);
    return {
      success: true,
      outputs: {
        workflowId: state.temporalWorkflowId,
        runId: state.temporalRunId,
        status: described?.status ?? ('RUNNING' as TemporalExecutionStatus),
        ...provenance,
      },
      metadata: { workflowId: state.temporalWorkflowId, awaitMode: 'sleep_poll', attempts },
      control: {
        sleep: { durationMs: pollInterval },
        persist: { ...state, attempts } as any,
      },
    };
  }

  /** In-step await, racing the Temporal result against the await timeout. */
  protected async blockingAwait(
    start: ITemporalStartWorkflowResult,
    deps: {
      service: any;
      connection: any;
      resultPath?: string;
      maxResultBytes: number;
      awaitTimeout: number;
    },
    provenance: { started: boolean; reattached: boolean; awaited: boolean },
    context: StepExecutionContext,
  ): Promise<StepExecutionResult> {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<'__timeout__'>((resolve) => {
      timer = setTimeout(() => resolve('__timeout__'), deps.awaitTimeout);
    });

    try {
      const settled = await Promise.race([
        deps.service.awaitOutcome(start.workflowId, start.runId, deps.connection, deps.maxResultBytes),
        timeout,
      ]);

      if (settled === '__timeout__') {
        return {
          success: false,
          error:
            `Timed out after ${deps.awaitTimeout}ms awaiting Temporal workflow ${start.workflowId}. ` +
            'The Temporal execution is still running — it was not cancelled.',
          outputs: {
            workflowId: start.workflowId,
            runId: start.runId,
            status: AWAIT_TIMEOUT_STATUS,
            ...provenance,
            awaited: true,
          },
          metadata: { workflowId: start.workflowId, awaitMode: 'block_poll', timedOut: true },
        };
      }

      return this.finish(
        settled as ITemporalOutcome,
        { ...provenance, awaited: true },
        deps.resultPath,
        context,
        deps.maxResultBytes,
      );
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  // ── helpers ──────────────────────────────────────────────────────────────────

  /** The event bridge, when the module's service is registered. */
  protected getBridge(context: StepExecutionContext): ITemporalEventBridge | null {
    try {
      return (
        (context.reactoryContext?.getService(
          TEMPORAL_EVENT_BRIDGE_SERVICE_ID,
        ) as unknown as ITemporalEventBridge) ?? null
      );
    } catch {
      return null;
    }
  }

  /** describe() that degrades to null instead of failing the step. */
  protected async safeDescribe(
    service: any,
    workflowId: string,
    runId: string | undefined,
    connection: any,
    context: StepExecutionContext,
  ): Promise<{ status: TemporalExecutionStatus } | null> {
    try {
      return await service.describeWorkflow(workflowId, runId, connection);
    } catch (error: any) {
      context.logger.warn(`Could not describe Temporal run ${workflowId}: ${error.message}`);
      return null;
    }
  }

  /** Rebuild an outcome from a bridge event payload, tolerating partial payloads. */
  protected outcomeFromEvent(eventData: any, workflowId: string): ITemporalOutcome {
    const data = eventData && typeof eventData === 'object' ? eventData : {};
    return {
      workflowId: data.workflowId || workflowId,
      runId: data.runId,
      status: (data.status as TemporalExecutionStatus) || 'UNKNOWN',
      result: data.result,
      failure: data.failure,
      truncated: data.truncated,
      bytes: data.bytes,
    };
  }

  /**
   * Convert a Temporal outcome into the step result, applying `resultPath` and the
   * `failOnWorkflowFailure` policy.
   *
   * An AWAIT_TIMEOUT is always a step failure regardless of that policy: we do not
   * know the run's outcome, so reporting success would be a lie.
   */
  protected finish(
    outcome: ITemporalOutcome,
    provenance: { started: boolean; reattached: boolean; awaited: boolean },
    resultPath: string | undefined,
    context: StepExecutionContext,
    maxResultBytes: number,
  ): StepExecutionResult {
    const failOnWorkflowFailure = this.config.failOnWorkflowFailure !== false;
    const isTimeout = (outcome.status as string) === AWAIT_TIMEOUT_STATUS;
    const failed = isTimeout || (outcome.status !== 'COMPLETED' && outcome.status !== 'CONTINUED_AS_NEW');

    const extracted = resultPath ? this.extractPath(outcome.result, resultPath, context) : outcome.result;
    // Re-normalise: an extracted subtree still has to fit the durable instance.
    const normalised = normaliseResult(extracted, maxResultBytes);
    if (normalised.truncated) {
      context.logger.warn(
        `Temporal result for ${outcome.workflowId} exceeded ${maxResultBytes} bytes ` +
          `(${normalised.bytes}) and was not stored — query Temporal for the full payload`,
      );
    }

    const outputs: Record<string, any> = {
      workflowId: outcome.workflowId,
      runId: outcome.runId,
      status: outcome.status,
      ...provenance,
      ...(normalised.truncated
        ? { truncated: true, bytes: normalised.bytes }
        : { result: normalised.result }),
      ...(outcome.failure ? { failure: outcome.failure } : {}),
    };

    if (failed && (failOnWorkflowFailure || isTimeout)) {
      return {
        success: false,
        error:
          outcome.failure?.message ||
          `Temporal workflow ${outcome.workflowId} ended with status ${outcome.status}`,
        outputs,
        metadata: { workflowId: outcome.workflowId, status: outcome.status },
      };
    }

    return {
      success: true,
      outputs,
      metadata: { workflowId: outcome.workflowId, status: outcome.status },
    };
  }

  /** Numeric config with a default, tolerating strings from the designer. */
  protected numberConfig(key: string, fallback: number): number {
    const value = this.config[key];
    if (typeof value === 'number' && value > 0) return value;
    if (typeof value === 'string' && value.trim() !== '' && !Number.isNaN(Number(value))) {
      const parsed = Number(value);
      if (parsed > 0) return parsed;
    }
    return fallback;
  }
}
