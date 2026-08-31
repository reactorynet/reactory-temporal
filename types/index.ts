export interface ITemporalConnectionConfig {
  address?: string; // e.g. "localhost:7233" or "temporal.internal:7233"
  namespace?: string; // e.g. "default" or "b2b-payments"
  tls?: {
    clientCertPath?: string;
    clientKeyPath?: string;
    serverRootCACertPath?: string;
    serverNameOverride?: string;
  };
  apiKey?: string; // For Temporal Cloud
}

export interface ITemporalStartWorkflowInput {
  workflowType: string;
  workflowId: string;
  taskQueue: string;
  args?: any[];
  searchAttributes?: Record<string, any>;
  memo?: Record<string, any>;
  cronSchedule?: string;
}

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
