import {
  proxyActivities,
  defineSignal,
  defineQuery,
  setHandler,
  condition,
} from '@temporalio/workflow';
import type * as activities from './activities';
import type { PayoutRow } from './activities';

// Proxy activities with automatic retry policies and timeouts
const {
  reserveFundsActivity,
  screenBeneficiariesActivity,
  createPayoutActivity,
  settleRowActivity,
  notifyBatchCompletionActivity,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: {
    initialInterval: '1s',
    backoffCoefficient: 2,
    maximumAttempts: 5,
  },
});

// Signals
export const approveBatchSignal = defineSignal<[string]>('approveBatch');
export const cancelBatchSignal = defineSignal<[string]>('cancelBatch');

// Queries
export interface BatchWorkflowProgress {
  batchId: string;
  status: 'SCREENING' | 'RESERVED_AWAITING_APPROVAL' | 'PROCESSING' | 'SETTLED' | 'CANCELLED';
  isApproved: boolean;
  approverUserId?: string;
  processedCount: number;
  totalRows: number;
  totalAmount: number;
  currency: string;
}

export const getBatchProgressQuery = defineQuery<BatchWorkflowProgress>('getBatchProgress');

export interface BatchPayoutWorkflowInput {
  batchId: string;
  accountId: string;
  currency: string;
  rows: PayoutRow[];
}

/**
 * B2B Mass Payout Orchestration Workflow
 * Implements the deterministic state machine specified in architecture.md:
 * Screening -> Reserve -> Checker Approval -> Fan-out Payout -> Settle -> Notify
 */
export async function B2BMassPayoutWorkflow(input: BatchPayoutWorkflowInput): Promise<{ batchId: string; status: string; totalSettled: number }> {
  let status: BatchWorkflowProgress['status'] = 'SCREENING';
  let isApproved = false;
  let isCancelled = false;
  let approverUserId = '';
  let cancelReason = '';
  let processedCount = 0;

  const totalAmount = input.rows.reduce((sum, r) => sum + r.amount, 0);

  // Setup Signal & Query handlers
  setHandler(approveBatchSignal, (userId: string) => {
    isApproved = true;
    approverUserId = userId;
  });

  setHandler(cancelBatchSignal, (reason: string) => {
    isCancelled = true;
    cancelReason = reason;
  });

  setHandler(getBatchProgressQuery, () => ({
    batchId: input.batchId,
    status,
    isApproved,
    approverUserId: approverUserId || undefined,
    processedCount,
    totalRows: input.rows.length,
    totalAmount,
    currency: input.currency,
  }));

  // Step 1: Beneficiary Screening
  const screeningResults = await screenBeneficiariesActivity(input.rows);
  const anyHold = screeningResults.some((s) => s.status === 'ALERT_HOLD');
  if (anyHold) {
    throw new Error(`Screening alert detected on batch ${input.batchId}. Escalated to LOD1.`);
  }

  // Step 2: Atomic Funds Reservation
  status = 'RESERVED_AWAITING_APPROVAL';
  await reserveFundsActivity(input.accountId, totalAmount, input.currency);

  // Step 3: Human-in-the-Loop Maker/Checker Gate
  // The workflow pauses durably until a Checker approves via Reactory UI or timeout (e.g. 7 days)
  const approved = await condition(() => isApproved || isCancelled, '7 days');

  if (!approved || isCancelled) {
    status = 'CANCELLED';
    throw new Error(`Batch ${input.batchId} cancelled or expired. Reason: ${cancelReason || 'Approval timeout'}`);
  }

  // Step 4: Dispatch Payouts & Settle Rows
  status = 'PROCESSING';
  for (const row of input.rows) {
    const payoutResult = await createPayoutActivity(row, input.batchId);
    if (payoutResult.status === 'DELIVERED') {
      await settleRowActivity(input.accountId, row.rowId, row.amount, row.currency, payoutResult.payoutId);
      processedCount++;
    }
  }

  // Step 5: Finalize & Notify
  status = 'SETTLED';
  await notifyBatchCompletionActivity(input.batchId, processedCount, totalAmount, input.currency);

  return {
    batchId: input.batchId,
    status: 'SETTLED',
    totalSettled: processedCount,
  };
}
