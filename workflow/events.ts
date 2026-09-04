/**
 * Temporal ⇄ Reactory workflow-engine event contract.
 *
 * Why a "settled" topic exists
 * ────────────────────────────
 * workflow-es matches ONE `(eventName, eventKey)` pair per suspended execution
 * pointer, so a waiting step cannot subscribe to `completed`, `failed`, `cancelled`
 * and `timeout` simultaneously. The bridge therefore publishes a single collapsed
 * topic — `temporal.workflow.settled` — for EVERY terminal outcome, with the real
 * status in the payload. The per-outcome topics below are published in addition, for
 * observability and for authors who write an explicit `wait_event` step against one
 * specific outcome.
 *
 * The correlation key is the Temporal **workflowId**, not the runId: the id is
 * deterministic and known to the waiting step before the run starts, and it survives
 * continue-as-new (which changes the runId).
 */

import { ITemporalOutcome, ITemporalWatchInput, TemporalWatchStatus } from '../types';

export const TEMPORAL_EVENT_BRIDGE_SERVICE_ID = 'reactory.TemporalEventBridgeService@1.0.0';

/** Collapsed topic every awaiting step subscribes to. */
export const DEFAULT_SETTLED_EVENT = 'temporal.workflow.settled';

/** Informational per-outcome topics. */
export const TEMPORAL_EVENTS = {
  settled: DEFAULT_SETTLED_EVENT,
  completed: 'temporal.workflow.completed',
  failed: 'temporal.workflow.failed',
  cancelled: 'temporal.workflow.cancelled',
  timeout: 'temporal.workflow.timeout',
  progress: 'temporal.workflow.progress',
} as const;

/** Every event name the callback route will accept and publish. */
export const ALLOWED_EVENT_PREFIX = 'temporal.';

/**
 * Map a normalised Temporal status to its informational topic. Returns undefined for
 * a non-terminal status (nothing informational to announce).
 */
export function informationalEventFor(status: string): string | undefined {
  switch (status) {
    case 'COMPLETED':
    case 'CONTINUED_AS_NEW':
      return TEMPORAL_EVENTS.completed;
    case 'FAILED':
    case 'TERMINATED':
    case 'TIMED_OUT':
      return TEMPORAL_EVENTS.failed;
    case 'CANCELED':
      return TEMPORAL_EVENTS.cancelled;
    case 'AWAIT_TIMEOUT':
      return TEMPORAL_EVENTS.timeout;
    default:
      return undefined;
  }
}

/** The payload delivered to a waiting step (and to `wait_event` authors). */
export interface ITemporalSettledEvent extends ITemporalOutcome {
  /** Milliseconds the bridge watched this run before it settled. */
  waitedMs?: number;
}

/**
 * The bridge's public surface, as consumed by the workflow steps. Declared as an
 * interface so a step never imports the implementation (which pulls in mongoose).
 */
export interface ITemporalEventBridge {
  /** Register interest in a run's terminal outcome. Idempotent per (workflowId, eventKey). */
  watch(input: ITemporalWatchInput): Promise<{ watchId: string }>;
  /** Drop a watch — used when a step discovers the run already settled. */
  unwatch(workflowId: string, eventKey?: string): Promise<void>;
  /** Publish an event into the workflow engine. */
  publish(eventName: string, eventKey: string, data: any): Promise<void>;
  /** Record an outcome that arrived from outside (callback route) and publish it. */
  settleFromCallback(input: {
    workflowId: string;
    runId?: string;
    eventName: string;
    eventKey: string;
    data: any;
  }): Promise<{ published: boolean; watchAcked: boolean }>;
  /** One reconciliation pass: re-attach, re-publish, time out, ack. */
  sweep(): Promise<ITemporalSweepReport>;
  /** Begin background reconciliation (idempotent). */
  start(): Promise<void>;
  /** Stop background reconciliation and cancel in-process listeners. */
  stop(): Promise<void>;
}

export interface ITemporalSweepReport {
  reattached: number;
  republished: number;
  timedOut: number;
  acked: number;
  abandoned: number;
}

/** Watch statuses that the sweeper still has work to do for. */
export const ACTIVE_WATCH_STATUSES: TemporalWatchStatus[] = ['pending', 'settled'];
