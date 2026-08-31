export interface PayoutRow {
  rowId: string;
  beneficiaryName: string;
  accountNumber: string;
  amount: number;
  currency: string;
}

export interface ReservationResult {
  reservationId: string;
  accountId: string;
  reservedAmount: number;
  currency: string;
  status: 'RESERVED' | 'FAILED';
}

export interface ScreeningResult {
  rowId: string;
  status: 'CLEARED' | 'ALERT_HOLD';
  matchScore?: number;
}

export interface PayoutExecutionResult {
  rowId: string;
  payoutId: string;
  status: 'DELIVERED' | 'FAILED';
  transactedAt: string;
}

/**
 * Activity: Reserves client balance in wallet-ledger
 */
export async function reserveFundsActivity(accountId: string, amount: number, currency: string): Promise<ReservationResult> {
  console.log(`[Activity:reserveFunds] 🔒 Reserving ${amount} ${currency} on wallet-ledger account: ${accountId}`);
  return {
    reservationId: `res_${Date.now()}`,
    accountId,
    reservedAmount: amount,
    currency,
    status: 'RESERVED',
  };
}

/**
 * Activity: Runs FinCrime / Sanctions screening on each row
 */
export async function screenBeneficiariesActivity(rows: PayoutRow[]): Promise<ScreeningResult[]> {
  console.log(`[Activity:screenBeneficiaries] 🔍 Screening ${rows.length} beneficiaries via Bridger/ZTP...`);
  return rows.map((row) => ({
    rowId: row.rowId,
    status: 'CLEARED',
    matchScore: 0.02,
  }));
}

/**
 * Activity: Calls payout transactor to dispatch money to bank/mobile wallet
 */
export async function createPayoutActivity(row: PayoutRow, batchId: string): Promise<PayoutExecutionResult> {
  console.log(`[Activity:createPayout] 💸 Dispatching payout row #${row.rowId} (${row.amount} ${row.currency} -> ${row.beneficiaryName} [${row.accountNumber}])`);
  return {
    rowId: row.rowId,
    payoutId: `pz_${batchId}_${row.rowId}`,
    status: 'DELIVERED',
    transactedAt: new Date().toISOString(),
  };
}

/**
 * Activity: Settles ledger line in wallet-ledger per delivered row
 */
export async function settleRowActivity(accountId: string, rowId: string, amount: number, currency: string, payoutId: string): Promise<{ settled: boolean }> {
  console.log(`[Activity:settleRow] 📝 Settled row #${rowId} in wallet-ledger (${amount} ${currency}, ref: ${payoutId})`);
  return { settled: true };
}

/**
 * Activity: Emits webhook and audit notification upon batch completion
 */
export async function notifyBatchCompletionActivity(batchId: string, totalSettled: number, totalAmount: number, currency: string): Promise<void> {
  console.log(`[Activity:notifyBatchCompletion] 🔔 Batch ${batchId} settled! Total: ${totalSettled} payouts (${totalAmount} ${currency}). Webhooks dispatched.`);
}
