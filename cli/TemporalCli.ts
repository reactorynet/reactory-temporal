import { TemporalClientService } from '../services/TemporalClientService';
import { runTemporalWorker } from '../workflows/worker';
import { B2BMassPayoutWorkflow, approveBatchSignal, getBatchProgressQuery } from '../workflows/workflows';

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

export const startWorkerCommand: Reactory.SDK.IReactoryCommandLine = {
  name: 'worker',
  description: 'Starts the Temporal worker for B2B payout workflows',
  exec: async (props: any, context: Reactory.Server.IReactoryContext) => {
    try {
      context.log('🚀 Starting Temporal Worker for B2BMassPayoutWorkflow...');
      await runTemporalWorker({
        taskQueue: props.taskQueue || 'b2b-payouts-queue',
        address: props.address,
        namespace: props.namespace,
      });
      return true;
    } catch (error: any) {
      context.log(`❌ Worker encountered error: ${error.message}`, { error }, 'error');
      return false;
    }
  },
};

export const testBatchCommand: Reactory.SDK.IReactoryCommandLine = {
  name: 'test-batch',
  description: 'Dispatches a sample B2B Mass Payout workflow to Temporal and executes Maker/Checker approval',
  exec: async (props: any, context: Reactory.Server.IReactoryContext) => {
    try {
      const temporalService: TemporalClientService = context.getService('reactory.TemporalClientService@1.0.0');
      const client = await temporalService.getClient();
      const batchId = `payroll_${Date.now()}`;
      const taskQueue = props.taskQueue || 'b2b-payouts-queue';

      context.log(`🚀 [Maker] Submitting sample batch ${batchId} to Temporal (${taskQueue})...`);

      const handle = await client.workflow.start(B2BMassPayoutWorkflow, {
        taskQueue,
        workflowId: batchId,
        args: [
          {
            batchId,
            accountId: 'acc_acme_payroll_gbp',
            currency: 'GBP',
            rows: [
              { rowId: 'row_1', beneficiaryName: 'Sarah Jenkins', accountNumber: 'GB82WEST12345678901234', amount: 3200, currency: 'GBP' },
              { rowId: 'row_2', beneficiaryName: 'Alex Thorne', accountNumber: 'GB29NWBK60161331926819', amount: 2850, currency: 'GBP' },
              { rowId: 'row_3', beneficiaryName: 'Priya Sharma', accountNumber: 'GB33BUKB20201555555555', amount: 4100, currency: 'GBP' },
            ],
          },
        ],
      });

      context.log(`✅ Workflow started! Workflow ID: ${handle.workflowId}`);

      // Query state
      await new Promise((r) => setTimeout(r, 1000));
      let progress = await handle.query(getBatchProgressQuery);
      context.log(`📊 State: ${progress.status} (Reserved: ${progress.totalAmount} ${progress.currency})`);

      context.log(`👉 Sending Checker Approval Signal (user_checker_werner)...`);
      await handle.signal(approveBatchSignal, 'user_checker_werner');

      context.log(`⏳ Waiting for workflow completion...`);
      const result = await handle.result();
      context.log(`🎉 Batch settled successfully! Result:`, result);

      return result;
    } catch (error: any) {
      context.log(`❌ Error running test batch: ${error.message}`, { error }, 'error');
      return false;
    }
  },
};
