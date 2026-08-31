import { Connection, Client } from '@temporalio/client';
import { B2BMassPayoutWorkflow, approveBatchSignal, getBatchProgressQuery } from './workflows';

async function main() {
  const address = process.env.TEMPORAL_ADDRESS || 'localhost:7233';
  const namespace = process.env.TEMPORAL_NAMESPACE || 'default';
  const batchId = `payroll_${Date.now()}`;
  const taskQueue = 'b2b-payouts-queue';

  console.log(`🔌 Connecting to Temporal Server at ${address}...`);
  const connection = await Connection.connect({ address });
  const client = new Client({ connection, namespace });

  console.log(`\n🚀 [Step 1: Maker] Submitting Batch Payout Workflow: ${batchId}...`);

  const handle = await client.workflow.start(B2BMassPayoutWorkflow, {
    taskQueue,
    workflowId: batchId,
    args: [
      {
        batchId,
        accountId: 'acc_acme_payroll_gbp',
        currency: 'GBP',
        rows: [
          { rowId: 'row_101', beneficiaryName: 'Sarah Jenkins', accountNumber: 'GB82WEST12345678901234', amount: 3200, currency: 'GBP' },
          { rowId: 'row_102', beneficiaryName: 'Alex Thorne', accountNumber: 'GB29NWBK60161331926819', amount: 2850, currency: 'GBP' },
          { rowId: 'row_103', beneficiaryName: 'Priya Sharma', accountNumber: 'GB33BUKB20201555555555', amount: 4100, currency: 'GBP' },
          { rowId: 'row_104', beneficiaryName: 'Marco Rossi', accountNumber: 'GB44BARC20000012345678', amount: 3750, currency: 'GBP' },
          { rowId: 'row_105', beneficiaryName: 'Elena Rostova', accountNumber: 'GB11HSBC40404011223344', amount: 2900, currency: 'GBP' },
        ],
      },
    ],
  });

  console.log(`✅ Workflow started! Workflow ID: ${handle.workflowId}, Run ID: ${handle.firstExecutionRunId}`);

  // Give the workflow a moment to screen & reserve funds
  await new Promise((resolve) => setTimeout(resolve, 2000));

  // Query workflow state
  let progress = await handle.query(getBatchProgressQuery);
  console.log(`\n📊 [Step 2: Progress Query] State: ${progress.status} (Approved: ${progress.isApproved}, Total: ${progress.totalAmount} ${progress.currency})`);

  console.log(`\n⏳ [Step 3: Maker/Checker Gate] Batch is waiting for Checker approval signal...`);
  console.log(`👉 Sending Signal: approveBatch (Approver: user_checker_werner)...`);

  await handle.signal(approveBatchSignal, 'user_checker_werner');
  console.log(`✅ Signal sent! Temporal workflow unblocked and processing payouts.`);

  // Wait for workflow completion and return result
  const result = await handle.result();
  console.log(`\n🎉 [Step 4: Completion] Workflow finished! Result:`, result);

  // Final progress query
  progress = await handle.query(getBatchProgressQuery);
  console.log(`📊 [Final State]: ${progress.status} (Processed: ${progress.processedCount}/${progress.totalRows})`);
}

main().catch((err) => {
  console.error('❌ Test execution error:', err);
  process.exit(1);
});
