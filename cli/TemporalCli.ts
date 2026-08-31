import { TemporalClientService } from '../services/TemporalClientService';

export const pingTemporalCommand: Reactory.SDK.IReactoryCommandLine = {
  name: 'ping',
  description: 'Tests connectivity to the Temporal cluster',
  exec: async (props: any, context: Reactory.Server.IReactoryContext) => {
    try {
      const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
      const client = await temporalService.getClient();
      const sysInfo = await client.workflowService.getSystemInfo({});
      context.log(`✅ Successfully connected to Temporal cluster at ${process.env.TEMPORAL_ADDRESS || 'localhost:7233'} (Server Version: ${sysInfo.serverVersion})`);
      return true;
    } catch (error: any) {
      context.log(`❌ Failed to connect to Temporal: ${error.message}`, { error }, 'error');
      return false;
    }
  },
};

export const describeWorkflowCommand: Reactory.SDK.IReactoryCommandLine = {
  name: 'describe',
  description: 'Describes a Temporal workflow execution: reactory temporal describe <workflowId> [runId]',
  exec: async (props: any, context: Reactory.Server.IReactoryContext) => {
    try {
      const workflowId = props._?.[0] || props.workflowId;
      const runId = props._?.[1] || props.runId;

      if (!workflowId) {
        context.log('❌ workflowId is required.', null, 'error');
        return false;
      }

      const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
      const client = await temporalService.getClient();
      const handle = client.workflow.getHandle(workflowId, runId);
      const desc = await handle.describe();

      context.log(`📋 Workflow: ${desc.workflowId} (${desc.type})`);
      context.log(`   Run ID: ${desc.runId}`);
      context.log(`   Status: ${desc.status.name}`);
      context.log(`   Task Queue: ${desc.taskQueue}`);
      context.log(`   Start Time: ${desc.startTime?.toISOString()}`);
      if (desc.closeTime) context.log(`   Close Time: ${desc.closeTime.toISOString()}`);
      context.log(`   History Events: ${desc.historyLength}`);

      return desc;
    } catch (error: any) {
      context.log(`❌ Error describing workflow: ${error.message}`, { error }, 'error');
      return false;
    }
  },
};
