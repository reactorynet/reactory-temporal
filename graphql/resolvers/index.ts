import { TemporalClientService } from '../../services/TemporalClientService';
import TemporalWorkflowWatchModel from '../../models/TemporalWorkflowWatch';
import {
  ALLOWED_EVENT_PREFIX,
  DEFAULT_SETTLED_EVENT,
  TEMPORAL_EVENT_BRIDGE_SERVICE_ID,
  ITemporalEventBridge,
} from '../../workflow/events';

/** Shape a watch document for the GraphQL layer. */
const mapWatch = (watch: any) => ({
  id: watch._id.toString(),
  workflowId: watch.workflowId,
  runId: watch.runId ?? null,
  eventName: watch.eventName,
  eventKey: watch.eventKey,
  status: watch.status,
  outcomeStatus: watch.outcome?.status ?? null,
  instanceId: watch.instanceId ?? null,
  stepId: watch.stepId ?? null,
  attempts: watch.attempts ?? 0,
  lastError: watch.lastError ?? null,
  awaitDeadline: watch.awaitDeadline?.toISOString() ?? null,
  createdAt: watch.createdAt?.toISOString() ?? null,
  settledAt: watch.settledAt?.toISOString() ?? null,
  ackedAt: watch.ackedAt?.toISOString() ?? null,
});

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

    temporalWorkflowWatches: async (_parent: any, { filter }: any, context: Reactory.Server.IReactoryContext) => {
      try {
        // Scoped to the calling partner: a watch names the workflow instance it can
        // wake, so it must never be readable across tenants.
        const query: Record<string, any> = { clientKey: (context.partner as any)?.key };
        if (filter?.status) query.status = filter.status;
        if (filter?.workflowId) query.workflowId = filter.workflowId;
        if (filter?.instanceId) query.instanceId = filter.instanceId;

        const watches = await TemporalWorkflowWatchModel.find(query)
          .sort({ updatedAt: -1 })
          .limit(Math.min(filter?.limit || 50, 200))
          .exec();

        return watches.map(mapWatch);
      } catch (error: any) {
        context.log(`Error listing Temporal watches: ${error.message}`, { error }, 'error');
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

    temporalPublishWorkflowEvent: async (
      _parent: any,
      { eventName, eventKey, workflowId, runId, data }: any,
      context: Reactory.Server.IReactoryContext,
    ) => {
      const topic = eventName || DEFAULT_SETTLED_EVENT;
      // Same constraint as the REST callback: this must not become a way to publish
      // arbitrary events into any workflow in the system.
      if (typeof topic !== 'string' || !topic.startsWith(ALLOWED_EVENT_PREFIX)) {
        throw new Error(`eventName must begin with "${ALLOWED_EVENT_PREFIX}"`);
      }
      const correlationKey = eventKey || workflowId;
      if (!correlationKey) throw new Error('eventKey (or workflowId) is required');

      try {
        const bridge = context.getService(
          TEMPORAL_EVENT_BRIDGE_SERVICE_ID,
        ) as unknown as ITemporalEventBridge;
        const result = await bridge.settleFromCallback({
          workflowId: workflowId || correlationKey,
          runId,
          eventName: topic,
          eventKey: correlationKey,
          data: data || {},
        });
        return result.published;
      } catch (error: any) {
        context.log(`Error publishing Temporal workflow event: ${error.message}`, { error }, 'error');
        throw error;
      }
    },

    temporalSweepWatches: async (_parent: any, _args: any, context: Reactory.Server.IReactoryContext) => {
      try {
        const bridge = context.getService(
          TEMPORAL_EVENT_BRIDGE_SERVICE_ID,
        ) as unknown as ITemporalEventBridge;
        return await bridge.sweep();
      } catch (error: any) {
        context.log(`Error sweeping Temporal watches: ${error.message}`, { error }, 'error');
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
