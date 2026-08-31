import { TemporalClientService } from '../services/TemporalClientService';

/**
 * Reactory Workflow Macro: temporalStartWorkflow
 * Starts a durable execution on Temporal from within a Reactory workflow.
 */
export const temporalStartWorkflowMacro = async (
  params: {
    workflowType: string;
    workflowId: string;
    taskQueue: string;
    args?: any[];
    searchAttributes?: Record<string, any>;
    memo?: Record<string, any>;
    cronSchedule?: string;
    connectionConfig?: any;
  },
  context: Reactory.Server.IReactoryContext
): Promise<{ workflowId: string; runId: string; success: boolean }> => {
  const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
  const result = await temporalService.startWorkflow(params, params.connectionConfig);

  return {
    workflowId: result.workflowId,
    runId: result.runId,
    success: true,
  };
};

/**
 * Reactory Workflow Macro: temporalSignalWorkflow
 * Sends a signal to a running Temporal workflow.
 */
export const temporalSignalWorkflowMacro = async (
  params: {
    workflowId: string;
    runId?: string;
    signalName: string;
    args?: any[];
    connectionConfig?: any;
  },
  context: Reactory.Server.IReactoryContext
): Promise<{ success: boolean }> => {
  const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
  await temporalService.signalWorkflow(params, params.connectionConfig);

  return { success: true };
};

/**
 * Reactory Workflow Macro: temporalQueryWorkflow
 * Queries the in-memory or persisted state of a Temporal workflow.
 */
export const temporalQueryWorkflowMacro = async (
  params: {
    workflowId: string;
    runId?: string;
    queryType: string;
    args?: any[];
    connectionConfig?: any;
  },
  context: Reactory.Server.IReactoryContext
): Promise<{ result: any; success: boolean }> => {
  const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
  const result = await temporalService.queryWorkflow(params, params.connectionConfig);

  return { result, success: true };
};

/**
 * Reactory Workflow Macro: temporalCancelWorkflow
 * Requests cancellation of a running Temporal workflow.
 */
export const temporalCancelWorkflowMacro = async (
  params: {
    workflowId: string;
    runId?: string;
    connectionConfig?: any;
  },
  context: Reactory.Server.IReactoryContext
): Promise<{ success: boolean }> => {
  const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
  await temporalService.cancelWorkflow(params.workflowId, params.runId, params.connectionConfig);

  return { success: true };
};
