import { TemporalClientService } from '../../services/TemporalClientService';

const TemporalResolvers: Reactory.Graph.IReactoryResolvers = {
  Query: {
    temporalWorkflowStatus: async (_parent: any, { workflowId, runId, connectionConfig }: any, context: Reactory.Server.IReactoryContext) => {
      try {
        const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
        const client = await temporalService.getClient(connectionConfig);
        const handle = client.workflow.getHandle(workflowId, runId);
        const desc = await handle.describe();

        return {
          workflowId: desc.workflowId,
          runId: desc.runId,
          workflowType: desc.type,
          taskQueue: desc.taskQueue,
          status: desc.status.name,
          startTime: desc.startTime?.toISOString(),
          closeTime: desc.closeTime?.toISOString(),
          historyLength: desc.historyLength,
        };
      } catch (error: any) {
        context.log(`Error querying Temporal workflow status: ${error.message}`, { error }, 'error');
        throw error;
      }
    },

    temporalQueryWorkflow: async (_parent: any, { input }: any, context: Reactory.Server.IReactoryContext) => {
      try {
        const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
        return await temporalService.queryWorkflow(input, input.connectionConfig);
      } catch (error: any) {
        context.log(`Error executing Temporal workflow query: ${error.message}`, { error }, 'error');
        throw error;
      }
    },

    temporalPing: async (_parent: any, { connectionConfig }: any, context: Reactory.Server.IReactoryContext) => {
      try {
        const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
        const client = await temporalService.getClient(connectionConfig);
        await client.workflowService.getSystemInfo({});
        return true;
      } catch (error: any) {
        context.log(`Temporal ping failed: ${error.message}`, { error }, 'warn');
        return false;
      }
    },
  },

  Mutation: {
    temporalStartWorkflow: async (_parent: any, { input }: any, context: Reactory.Server.IReactoryContext) => {
      try {
        const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
        const result = await temporalService.startWorkflow(input, input.connectionConfig);
        return {
          workflowId: result.workflowId,
          runId: result.runId,
          status: 'RUNNING',
        };
      } catch (error: any) {
        context.log(`Error starting Temporal workflow: ${error.message}`, { error }, 'error');
        throw error;
      }
    },

    temporalSignalWorkflow: async (_parent: any, { input }: any, context: Reactory.Server.IReactoryContext) => {
      try {
        const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
        await temporalService.signalWorkflow(input, input.connectionConfig);
        return true;
      } catch (error: any) {
        context.log(`Error signaling Temporal workflow: ${error.message}`, { error }, 'error');
        throw error;
      }
    },

    temporalCancelWorkflow: async (_parent: any, { workflowId, runId, connectionConfig }: any, context: Reactory.Server.IReactoryContext) => {
      try {
        const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
        await temporalService.cancelWorkflow(workflowId, runId, connectionConfig);
        return true;
      } catch (error: any) {
        context.log(`Error cancelling Temporal workflow: ${error.message}`, { error }, 'error');
        throw error;
      }
    },

    temporalTerminateWorkflow: async (_parent: any, { workflowId, reason, runId, connectionConfig }: any, context: Reactory.Server.IReactoryContext) => {
      try {
        const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
        await temporalService.terminateWorkflow(workflowId, reason, runId, connectionConfig);
        return true;
      } catch (error: any) {
        context.log(`Error terminating Temporal workflow: ${error.message}`, { error }, 'error');
        throw error;
      }
    },
  },
};

export default TemporalResolvers;
