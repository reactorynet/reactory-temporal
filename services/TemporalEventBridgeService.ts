/**
 * TemporalEventBridgeService — delivers Temporal outcomes into the Reactory workflow
 * engine, reliably.
 *
 * A naive bridge would just `await handle.result()` and publish. That loses work in
 * three ways, each of which this service is built around:
 *
 *  1. **The engine discards unmatched events.** When the event queue processes an
 *     event that matches no subscription, it marks it processed and drops it — there
 *     is no replay. A publish that lands before the awaiting step actually suspends
 *     is gone forever. Mitigation: the outcome is persisted to a watch record BEFORE
 *     the first publish, and re-published on every sweep until the waiting execution
 *     pointer is observed to have consumed it (`eventPublished`).
 *
 *  2. **Restarts kill in-memory listeners.** Mitigation: watches are durable; the
 *     sweeper re-attaches listeners for `pending` watches and reconciles immediately
 *     with `describe()`, so a completion that happened while Reactory was down is
 *     published on the first sweep after boot.
 *
 *  3. **Tenancy.** Instances are started under `partner.key` as their engine tenant
 *     and subscription matching is tenant-scoped. A sweeper running under the system
 *     context must publish under the *instance's* tenant, which is why the watch
 *     records `clientKey` and the ack check reads the instance's `tenantId`.
 *
 * The sweep is single-flighted across pods using the core SchedulerLock collection,
 * so N replicas do not each re-publish the same outcome.
 */

import Reactory from '@reactorynet/reactory-core';
import { SchedulerLockModel } from '@reactory/server-modules/reactory-core/workflow/Scheduler/SchedulerLock.model';
import TemporalWorkflowWatchModel, {
  TemporalWorkflowWatchDocument,
} from '../models/TemporalWorkflowWatch';
import { TemporalClientService, DEFAULT_MAX_RESULT_BYTES } from './TemporalClientService';
import {
  DEFAULT_SETTLED_EVENT,
  informationalEventFor,
  ITemporalEventBridge,
  ITemporalSweepReport,
} from '../workflow/events';
import {
  ITemporalConnectionConfig,
  ITemporalOutcome,
  ITemporalWatchInput,
} from '../types';

const WORKFLOW_SERVICE_ID = 'core.ReactoryWorkflowService@1.0.0';
const TEMPORAL_CLIENT_SERVICE_ID = 'reactory.TemporalClientService@1.0.0';

/** Give up re-publishing an outcome after this many attempts. */
const MAX_PUBLISH_ATTEMPTS = 12;
/** Base backoff between publish attempts (doubles, capped). */
const PUBLISH_BACKOFF_BASE_MS = 15000;
const PUBLISH_BACKOFF_CAP_MS = 10 * 60 * 1000;
/** Lock identity + lease for the single-flighted sweep. */
const SWEEP_LOCK_ID = 'temporal.bridge.sweep';
const SWEEP_LOCK_TTL_MS = 60 * 1000;
/** How many watches one sweep will process per category. */
const SWEEP_BATCH_SIZE = 200;
/** Cadence of the background reconciliation sweep. */
const SWEEP_INTERVAL_MS = Number(process.env.TEMPORAL_BRIDGE_SWEEP_INTERVAL_MS || 30000);

const POD_IDENTITY =
  process.env.REACTORY_POD_ID || `${require('os').hostname()}:${process.pid}`;

/** Cancellation handle for an in-process result listener. */
interface Listener {
  workflowId: string;
  key: string;
  cancelled: boolean;
}

export class TemporalEventBridgeService implements ITemporalEventBridge {
  name: string = 'TemporalEventBridgeService';
  nameSpace: string = 'reactory';
  version: string = '1.0.0';

  private context: Reactory.Server.IReactoryContext;
  /** Live result listeners, keyed by watch key — process-local by nature. */
  private listeners: Map<string, Listener> = new Map();
  /** Background sweep timer; null when the bridge is not started. */
  private sweepTimer: NodeJS.Timeout | null = null;
  /** Guards against overlapping sweeps in THIS process. */
  private sweeping: boolean = false;

  constructor(props: any, context: Reactory.Server.IReactoryContext) {
    this.context = context;
  }

  /**
   * Begin background reconciliation: re-attach listeners for watches left pending by
   * a previous process, then sweep on a timer.
   *
   * Idempotent, and safe to call from the module's boot workflow. The timer is
   * unref'd so it never keeps the process alive on its own.
   */
  async start(): Promise<void> {
    if (this.sweepTimer) return;

    // Immediate pass so a completion that happened while this process was down is
    // delivered at boot rather than one interval later.
    await this.safeSweep();

    this.sweepTimer = setInterval(() => {
      void this.safeSweep();
    }, SWEEP_INTERVAL_MS);
    if (typeof this.sweepTimer.unref === 'function') this.sweepTimer.unref();

    this.context.log(
      `Temporal event bridge started (sweep every ${SWEEP_INTERVAL_MS}ms)`,
      {},
      'info',
    );
  }

  /** Stop the background sweep and cancel in-process listeners. */
  async stop(): Promise<void> {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    for (const key of Array.from(this.listeners.keys())) this.detachListener(key);
  }

  /** Reactory service lifecycle hook — mirrors stop(). */
  async onShutdown(): Promise<void> {
    await this.stop();
  }

  /**
   * Run a sweep, swallowing errors: a background timer must never surface an
   * unhandled rejection, and a failed sweep is retried on the next tick anyway.
   * Overlapping ticks are skipped so a slow sweep cannot pile up.
   */
  private async safeSweep(): Promise<void> {
    if (this.sweeping) return;
    this.sweeping = true;
    try {
      const report = await this.sweep();
      const touched =
        report.reattached + report.republished + report.timedOut + report.acked + report.abandoned;
      if (touched > 0) {
        this.context.log('Temporal bridge sweep', report as any, 'debug');
      }
    } catch (error: any) {
      this.context.log(`Temporal bridge sweep failed: ${error.message}`, {}, 'error');
    } finally {
      this.sweeping = false;
    }
  }

  /**
   * Register interest in a Temporal run's terminal outcome.
   *
   * Idempotent per (workflowId, eventKey, eventName, clientKey): a step that
   * re-executes after an engine retry updates its existing watch instead of
   * creating a duplicate that would publish the same outcome twice.
   */
  async watch(input: ITemporalWatchInput): Promise<{ watchId: string }> {
    const eventName = input.eventName || DEFAULT_SETTLED_EVENT;
    const eventKey = input.eventKey || input.workflowId;
    const clientKey = input.clientKey || (this.context?.partner as any)?.key || null;

    const watch = await TemporalWorkflowWatchModel.findOneAndUpdate(
      { workflowId: input.workflowId, eventKey, eventName, clientKey },
      {
        $set: {
          runId: input.runId,
          namespace: input.namespace,
          address: input.address,
          connectionKey: input.connectionKey,
          instanceId: input.instanceId,
          stepId: input.stepId,
          userEmail: input.userEmail,
          maxResultBytes: input.maxResultBytes || DEFAULT_MAX_RESULT_BYTES,
          awaitDeadline: input.awaitTimeout ? new Date(Date.now() + input.awaitTimeout) : undefined,
        },
        // Only initialise the lifecycle fields on insert: an existing watch may
        // already have settled, and resetting it would replay a delivered outcome.
        $setOnInsert: { status: 'pending', attempts: 0 },
      },
      { upsert: true, new: true, setDefaultsOnInsert: true },
    );

    this.attachListener(watch);
    return { watchId: watch._id.toString() };
  }

  /**
   * Drop a watch. Used when the awaiting step discovers the run already settled and
   * resolves inline, so no event needs to be delivered.
   */
  async unwatch(workflowId: string, eventKey?: string): Promise<void> {
    const query: Record<string, any> = { workflowId };
    if (eventKey) query.eventKey = eventKey;
    const watches = await TemporalWorkflowWatchModel.find(query).exec();
    for (const watch of watches) {
      this.detachListener(this.watchKey(watch));
      // Keep the record as audit trail, but take it out of the sweeper's way.
      watch.status = 'acked';
      watch.ackedAt = new Date();
      await watch.save();
    }
  }

  /**
   * Publish an event into the workflow engine.
   *
   * `tenantId` matters: see the class docblock. Callers that know the target
   * instance should prefer publishForWatch, which resolves the tenant from the
   * instance itself.
   */
  async publish(eventName: string, eventKey: string, data: any, tenantId?: string): Promise<void> {
    const workflowService: any = this.context.getService(WORKFLOW_SERVICE_ID);
    if (!workflowService?.publishWorkflowEvent) {
      throw new Error(`${WORKFLOW_SERVICE_ID} does not expose publishWorkflowEvent`);
    }
    const result = await workflowService.publishWorkflowEvent(eventName, eventKey, data, tenantId);
    if (result && result.success === false) {
      throw new Error(result.message || `Failed to publish '${eventName}'`);
    }
  }

  /**
   * Record an outcome that arrived from outside this process (the REST callback used
   * by externally hosted Temporal workers) and deliver it.
   */
  async settleFromCallback(input: {
    workflowId: string;
    runId?: string;
    eventName: string;
    eventKey: string;
    data: any;
  }): Promise<{ published: boolean; watchAcked: boolean }> {
    const watch = await TemporalWorkflowWatchModel.findOne({
      workflowId: input.workflowId,
      eventKey: input.eventKey,
    })
      .sort({ createdAt: -1 })
      .exec();

    // No watch: this is a run Reactory never started (or an informational topic).
    // Publish it anyway — a YAML author may be waiting with an explicit wait_event
    // step — but there is nothing to persist or retry against.
    if (!watch) {
      await this.publish(input.eventName, input.eventKey, input.data);
      this.context.log(
        `Published unwatched Temporal event '${input.eventName}' (key: ${input.eventKey})`,
        {},
        'debug',
      );
      return { published: true, watchAcked: false };
    }

    const outcome: ITemporalOutcome = {
      workflowId: input.workflowId,
      runId: input.runId || watch.runId,
      status: input.data?.status || 'COMPLETED',
      result: input.data?.result,
      failure: input.data?.failure,
      truncated: input.data?.truncated,
      bytes: input.data?.bytes,
    };

    const acked = await this.settle(watch, outcome);
    return { published: true, watchAcked: acked };
  }

  /**
   * One reconciliation pass. Single-flighted across pods: a replica that loses the
   * lock race returns an empty report rather than duplicating the work.
   */
  async sweep(): Promise<ITemporalSweepReport> {
    const empty: ITemporalSweepReport = {
      reattached: 0,
      republished: 0,
      timedOut: 0,
      acked: 0,
      abandoned: 0,
    };

    if (!(await this.acquireSweepLock())) {
      return empty;
    }

    const report = { ...empty };

    // 1. Pending watches with no live listener — typically after a restart.
    const pending = await TemporalWorkflowWatchModel.find({ status: 'pending' })
      .sort({ updatedAt: 1 })
      .limit(SWEEP_BATCH_SIZE)
      .exec();

    for (const watch of pending) {
      // Await deadline first: a run that outlived its deadline must be reported to
      // the waiting step rather than watched forever.
      if (watch.awaitDeadline && watch.awaitDeadline.getTime() <= Date.now()) {
        await this.timeout(watch);
        report.timedOut += 1;
        continue;
      }

      if (!this.listeners.has(this.watchKey(watch))) {
        // Reconcile before re-attaching: the run may have settled while this
        // process (or the whole server) was not listening.
        const settled = await this.reconcile(watch);
        if (settled) {
          report.republished += 1;
        } else {
          this.attachListener(watch);
          report.reattached += 1;
        }
      }
    }

    // 2. Settled-but-unacked watches — the delivery guarantee.
    const unacked = await TemporalWorkflowWatchModel.find({ status: 'settled' })
      .sort({ updatedAt: 1 })
      .limit(SWEEP_BATCH_SIZE)
      .exec();

    for (const watch of unacked) {
      if (!this.publishDue(watch)) continue;
      const acked = await this.attemptPublish(watch);
      if (acked) report.acked += 1;
      else if (watch.status === 'abandoned') report.abandoned += 1;
      else report.republished += 1;
    }

    return report;
  }

  // ── internals ────────────────────────────────────────────────────────────────

  /** Stable identity for a watch (and for its in-process listener). */
  private watchKey(watch: TemporalWorkflowWatchDocument): string {
    return `${watch.workflowId}|${watch.eventKey}|${watch.eventName}|${watch.clientKey || ''}`;
  }

  /** The connection config a watch was registered with. */
  private connectionFor(watch: TemporalWorkflowWatchDocument): ITemporalConnectionConfig | undefined {
    if (!watch.namespace && !watch.address && !watch.connectionKey) return undefined;
    return {
      namespace: watch.namespace,
      address: watch.address,
      connectionKey: watch.connectionKey,
    };
  }

  private temporalService(): TemporalClientService {
    return this.context.getService(TEMPORAL_CLIENT_SERVICE_ID) as unknown as TemporalClientService;
  }

  /**
   * Attach an in-process `handle.result()` listener for a watch. Fire-and-forget by
   * design: the promise resolves when Temporal settles, which may be days away, and
   * the durable watch record is the real guarantee if this process dies first.
   */
  private attachListener(watch: TemporalWorkflowWatchDocument): void {
    if (watch.status !== 'pending') return;
    const key = this.watchKey(watch);
    if (this.listeners.has(key)) return;

    const listener: Listener = { workflowId: watch.workflowId, key, cancelled: false };
    this.listeners.set(key, listener);

    void (async () => {
      try {
        const outcome = await this.temporalService().awaitOutcome(
          watch.workflowId,
          watch.runId,
          this.connectionFor(watch),
          watch.maxResultBytes || DEFAULT_MAX_RESULT_BYTES,
        );
        if (listener.cancelled) return;
        // Re-read: the watch may have been settled or acked elsewhere while we waited.
        const current = await TemporalWorkflowWatchModel.findById(watch._id).exec();
        if (!current || current.status !== 'pending') return;
        await this.settle(current, outcome);
      } catch (error: any) {
        // Infrastructure failure (cluster unreachable, connection dropped). Leave the
        // watch pending — the next sweep re-attaches and reconciles.
        this.context.log(
          `Temporal watch listener for ${watch.workflowId} failed: ${error.message}`,
          { workflowId: watch.workflowId },
          'warn',
        );
      } finally {
        this.listeners.delete(key);
      }
    })();
  }

  private detachListener(key: string): void {
    const listener = this.listeners.get(key);
    if (listener) {
      listener.cancelled = true;
      this.listeners.delete(key);
    }
  }

  /**
   * Check a pending watch against Temporal and settle it if the run has finished.
   * Returns true when the watch was settled by this call.
   */
  private async reconcile(watch: TemporalWorkflowWatchDocument): Promise<boolean> {
    try {
      const service = this.temporalService();
      const connection = this.connectionFor(watch);
      const described = await service.describeWorkflow(watch.workflowId, watch.runId, connection);
      if (described.status === 'RUNNING' || described.status === 'UNKNOWN') return false;

      const outcome = await service.awaitOutcome(
        watch.workflowId,
        watch.runId,
        connection,
        watch.maxResultBytes || DEFAULT_MAX_RESULT_BYTES,
      );
      await this.settle(watch, outcome);
      return true;
    } catch (error: any) {
      watch.lastError = error.message;
      await watch.save();
      this.context.log(
        `Could not reconcile Temporal watch ${watch.workflowId}: ${error.message}`,
        { workflowId: watch.workflowId },
        'warn',
      );
      return false;
    }
  }

  /**
   * Persist a terminal outcome, then attempt delivery.
   *
   * The persist-before-publish order is the whole point: if the publish is dropped
   * (no subscription yet) or this process dies mid-publish, the outcome is already
   * durable and the sweeper will keep trying.
   */
  private async settle(
    watch: TemporalWorkflowWatchDocument,
    outcome: ITemporalOutcome,
  ): Promise<boolean> {
    watch.outcome = {
      status: outcome.status,
      result: outcome.result,
      failure: outcome.failure,
      truncated: outcome.truncated,
      bytes: outcome.bytes,
    };
    watch.runId = outcome.runId || watch.runId;
    watch.status = 'settled';
    watch.settledAt = new Date();
    await watch.save();
    this.detachListener(this.watchKey(watch));
    return this.attemptPublish(watch);
  }

  /** Publish an AWAIT_TIMEOUT outcome for a watch that outlived its deadline. */
  private async timeout(watch: TemporalWorkflowWatchDocument): Promise<void> {
    const waitedMs = Date.now() - watch.createdAt.getTime();
    watch.outcome = { status: 'AWAIT_TIMEOUT' };
    watch.status = 'settled';
    watch.settledAt = new Date();
    await watch.save();
    this.detachListener(this.watchKey(watch));

    const acked = await this.attemptPublish(watch, { waitedMs });
    // Distinguish a timed-out watch from a normally settled one for operators, but
    // only once delivery is resolved so the sweeper's retry logic still applies.
    if (acked) {
      watch.status = 'timed_out';
      await watch.save();
    }
    this.context.log(
      `Temporal watch ${watch.workflowId} timed out after ${waitedMs}ms — published AWAIT_TIMEOUT`,
      { workflowId: watch.workflowId },
      'warn',
    );
  }

  /**
   * Publish a settled watch's outcome and verify it was consumed.
   * Returns true when the waiting step is confirmed to have received it.
   */
  private async attemptPublish(
    watch: TemporalWorkflowWatchDocument,
    extra: Record<string, any> = {},
  ): Promise<boolean> {
    watch.attempts = (watch.attempts || 0) + 1;
    watch.lastAttemptAt = new Date();

    const tenantId = await this.resolveTenant(watch);
    const payload = {
      workflowId: watch.workflowId,
      runId: watch.runId,
      status: watch.outcome?.status,
      ...(watch.outcome?.result !== undefined ? { result: watch.outcome.result } : {}),
      ...(watch.outcome?.failure ? { failure: watch.outcome.failure } : {}),
      ...(watch.outcome?.truncated ? { truncated: true, bytes: watch.outcome.bytes } : {}),
      ...extra,
    };

    try {
      await this.publish(watch.eventName, watch.eventKey, payload, tenantId);

      // Informational per-outcome topic, for observability and for authors who wrote
      // an explicit wait_event against a specific outcome. Best-effort: a failure
      // here must not affect the primary delivery's accounting.
      const informational = informationalEventFor(watch.outcome?.status || '');
      if (informational && informational !== watch.eventName) {
        await this.publish(informational, watch.eventKey, payload, tenantId).catch(() => undefined);
      }
    } catch (error: any) {
      watch.lastError = error.message;
      if (watch.attempts >= MAX_PUBLISH_ATTEMPTS) {
        watch.status = 'abandoned';
        this.context.log(
          `Abandoning Temporal watch ${watch.workflowId} after ${watch.attempts} failed publish attempts: ${error.message}`,
          { workflowId: watch.workflowId },
          'error',
        );
      }
      await watch.save();
      return false;
    }

    const acked = await this.verifyAck(watch);
    if (acked) {
      watch.status = 'acked';
      watch.ackedAt = new Date();
      watch.lastError = undefined;
    } else if (watch.attempts >= MAX_PUBLISH_ATTEMPTS) {
      watch.status = 'abandoned';
      this.context.log(
        `Abandoning Temporal watch ${watch.workflowId}: published ${watch.attempts} times but the ` +
          'waiting step never consumed the event (is the instance still suspended on it?)',
        { workflowId: watch.workflowId, eventName: watch.eventName, eventKey: watch.eventKey },
        'error',
      );
    }
    await watch.save();
    return acked;
  }

  /**
   * Was the published event actually consumed by the waiting step?
   *
   * Read the target instance and look for an execution pointer subscribed to this
   * watch's event: if such a pointer exists and is NOT `eventPublished`, the event
   * has not been delivered and we must publish again. If the pointer is published,
   * or there is no waiting pointer at all, delivery is done.
   *
   * Without an instanceId there is nothing to check, so a successful publish is
   * taken as delivery — the awaiting step's own pre-suspend `describe()` check is
   * the safety net in that case.
   */
  private async verifyAck(watch: TemporalWorkflowWatchDocument): Promise<boolean> {
    if (!watch.instanceId) return true;
    try {
      const workflowService: any = this.context.getService(WORKFLOW_SERVICE_ID);
      const instance = await workflowService?.getWorkflowHistoryById(watch.instanceId);
      if (!instance) return true; // instance gone — nothing left to wake

      const stillWaiting = (instance.executionPointers || []).some(
        (pointer: any) => pointer.eventName === watch.eventName && !pointer.eventPublished,
      );
      return !stillWaiting;
    } catch (error: any) {
      // Cannot verify — do not claim delivery; the next sweep will try again.
      this.context.log(
        `Could not verify delivery of '${watch.eventName}' to instance ${watch.instanceId}: ${error.message}`,
        {},
        'warn',
      );
      return false;
    }
  }

  /**
   * The tenant to publish under: the awaiting instance's own tenant when known,
   * falling back to the watch's recorded client key.
   */
  private async resolveTenant(watch: TemporalWorkflowWatchDocument): Promise<string | undefined> {
    if (watch.instanceId) {
      try {
        const workflowService: any = this.context.getService(WORKFLOW_SERVICE_ID);
        const instance = await workflowService?.getWorkflowHistoryById(watch.instanceId);
        if (instance?.tenantId) return instance.tenantId;
      } catch {
        /* fall through to the recorded client key */
      }
    }
    return watch.clientKey || undefined;
  }

  /** Exponential backoff gate for republication. */
  private publishDue(watch: TemporalWorkflowWatchDocument): boolean {
    if (!watch.lastAttemptAt) return true;
    const backoff = Math.min(
      PUBLISH_BACKOFF_BASE_MS * 2 ** Math.max(0, (watch.attempts || 1) - 1),
      PUBLISH_BACKOFF_CAP_MS,
    );
    return Date.now() - watch.lastAttemptAt.getTime() >= backoff;
  }

  /**
   * Win the right to run this sweep tick. Reuses the core scheduler's lock
   * collection: the unique (scheduleId, slotTime) index makes the insert the
   * serialisation primitive, and its TTL releases the lease if this pod dies.
   */
  private async acquireSweepLock(): Promise<boolean> {
    const slot = new Date();
    slot.setSeconds(0, 0);
    try {
      await SchedulerLockModel.create({
        scheduleId: SWEEP_LOCK_ID,
        slotTime: slot,
        instanceId: POD_IDENTITY,
        acquiredAt: new Date(),
        expiresAt: new Date(Date.now() + SWEEP_LOCK_TTL_MS),
      });
      return true;
    } catch (error: any) {
      if (error?.code === 11000) return false; // another pod holds this slot
      this.context.log(`Temporal bridge sweep lock error: ${error.message}`, {}, 'warn');
      return false;
    }
  }
}

export const TemporalEventBridgeServiceDefinition: Reactory.Service.IReactoryServiceDefinition<TemporalEventBridgeService> =
  {
    id: 'reactory.TemporalEventBridgeService@1.0.0',
    name: 'TemporalEventBridgeService',
    nameSpace: 'reactory',
    version: '1.0.0',
    description:
      'Delivers Temporal workflow outcomes into the Reactory workflow engine with at-least-once, restart-safe publication',
    // Factory, not the class — ServiceManager CALLS this field.
    service: (
      props: Reactory.Service.IReactoryServiceProps,
      context: Reactory.Server.IReactoryContext,
    ) => new TemporalEventBridgeService(props, context),
    dependencies: [],
    serviceType: 'integration',
    lifeCycle: 'singleton',
  };
