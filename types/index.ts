export interface ITemporalConnectionConfig {
  address?: string; // e.g. "localhost:7233" or "temporal.internal:7233"
  namespace?: string; // e.g. "default" or "b2b-payments"
  tls?: {
    /** Path to a client certificate PEM file, or the PEM contents themselves. */
    clientCertPath?: string;
    /** Path to a client private key PEM file, or the PEM contents themselves. */
    clientKeyPath?: string;
    /** Path to the server root CA PEM file, or the PEM contents themselves. */
    serverRootCACertPath?: string;
    serverNameOverride?: string;
  };
  apiKey?: string; // For Temporal Cloud
  /**
   * Named connection to resolve from the active partner's settings / environment
   * instead of supplying address + namespace inline. Resolution order is
   * documented on TemporalClientService.resolveConnectionConfig.
   */
  connectionKey?: string;
}

export interface ITemporalStartWorkflowInput {
  workflowType: string;
  workflowId: string;
  taskQueue: string;
  args?: any[];
  /** @deprecated Temporal >= 1.12 prefers typedSearchAttributes. */
  searchAttributes?: Record<string, any>;
  typedSearchAttributes?: any;
  memo?: Record<string, any>;
  cronSchedule?: string;
  /** Conflict handling when an execution with the same workflowId is already running. */
  workflowIdConflictPolicy?: TemporalWorkflowIdConflictPolicy;
  /** Reuse handling when a *closed* execution with the same workflowId exists. */
  workflowIdReusePolicy?: TemporalWorkflowIdReusePolicy;
  workflowExecutionTimeout?: string | number;
  workflowRunTimeout?: string | number;
  workflowTaskTimeout?: string | number;
  retry?: Record<string, any>;
  /** When set, the workflow is started with signalWithStart. */
  signalOnStart?: { name: string; args?: any[] };
}

export type TemporalWorkflowIdConflictPolicy = 'USE_EXISTING' | 'FAIL' | 'TERMINATE_EXISTING';

export type TemporalWorkflowIdReusePolicy =
  | 'ALLOW_DUPLICATE'
  | 'ALLOW_DUPLICATE_FAILED_ONLY'
  | 'REJECT_DUPLICATE'
  | 'TERMINATE_IF_RUNNING';

export interface ITemporalSignalWorkflowInput {
  workflowId: string;
  runId?: string;
  signalName: string;
  args?: any[];
}

export interface ITemporalQueryWorkflowInput {
  workflowId: string;
  runId?: string;
  queryType: string;
  args?: any[];
}

export interface ITemporalUpdateWorkflowInput {
  workflowId: string;
  runId?: string;
  updateName: string;
  args?: any[];
  /** 'ACCEPTED' returns as soon as the update is accepted; 'COMPLETED' awaits the result. */
  waitForStage?: 'ACCEPTED' | 'COMPLETED';
}

/** Normalised terminal/most-recent status of a Temporal execution. */
export type TemporalExecutionStatus =
  | 'RUNNING'
  | 'COMPLETED'
  | 'FAILED'
  | 'CANCELED'
  | 'TERMINATED'
  | 'CONTINUED_AS_NEW'
  | 'TIMED_OUT'
  | 'UNKNOWN';

export interface ITemporalStartWorkflowResult {
  workflowId: string;
  runId: string;
  /** True when a new execution was started, false when an existing one was re-attached. */
  started: boolean;
  /** True when an execution with this workflowId was already running. */
  reattached: boolean;
  /** Present when the workflow was started with signalWithStart. */
  signalled?: boolean;
}

export interface ITemporalDescribeResult {
  workflowId: string;
  runId: string;
  workflowType?: string;
  taskQueue?: string;
  status: TemporalExecutionStatus;
  startTime?: string;
  closeTime?: string;
  historyLength?: number;
  memo?: Record<string, any>;
}

/** Terminal outcome of a Temporal execution, as delivered to the workflow engine. */
export interface ITemporalOutcome {
  workflowId: string;
  runId?: string;
  status: TemporalExecutionStatus;
  result?: any;
  failure?: { message: string; type?: string; cause?: string };
  /** Set when `result` exceeded the configured size cap and was dropped. */
  truncated?: boolean;
  /** Byte size of the original result when truncated. */
  bytes?: number;
}

/** Status lifecycle of a bridge watch record. */
export type TemporalWatchStatus = 'pending' | 'settled' | 'acked' | 'timed_out' | 'abandoned';

export interface ITemporalWatchInput {
  workflowId: string;
  runId?: string;
  connectionKey?: string;
  namespace?: string;
  address?: string;
  /** Event name published on settle — defaults to temporal.workflow.settled. */
  eventName?: string;
  /** Correlation key — defaults to the workflowId. */
  eventKey?: string;
  /** Milliseconds before the bridge publishes an AWAIT_TIMEOUT outcome. */
  awaitTimeout?: number;
  /** Engine instance + step this watch will wake (used for ack verification). */
  instanceId?: string;
  stepId?: string;
  clientKey?: string;
  userEmail?: string;
  /** Max bytes of Temporal result data to carry in the published payload. */
  maxResultBytes?: number;
}
