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

// ─────────────────────────────────────────────────────────────────────────────────
// Reactory callback activity
// ─────────────────────────────────────────────────────────────────────────────────

/**
 * Activity: notify a Reactory workflow instance from inside a Temporal workflow.
 *
 * Use this to push PROGRESS or intermediate events (approval needed, milestone
 * reached) into a waiting Reactory YAML workflow. Terminal outcomes do NOT need it:
 * the TemporalEventBridgeService already watches every run Reactory starts and
 * publishes the settled event itself.
 *
 * It must be an ACTIVITY, never inline workflow code: Temporal replays workflow
 * functions deterministically, so network I/O inside a workflow is illegal.
 *
 * Environment:
 *   REACTORY_API_URL      e.g. http://localhost:4000  (required)
 *   REACTORY_CLIENT_KEY   the Reactory client key     (required — global auth)
 *   REACTORY_CLIENT_PWD   the matching client secret  (required)
 */
export async function notifyReactoryActivity(
  eventName: string,
  eventKey: string,
  data?: Record<string, unknown>,
): Promise<{ accepted: boolean; watchAcked?: boolean }> {
  const apiUrl = process.env.REACTORY_API_URL;
  const clientKey = process.env.REACTORY_CLIENT_KEY;
  const clientPwd = process.env.REACTORY_CLIENT_PWD;

  if (!apiUrl || !clientKey || !clientPwd) {
    throw new Error(
      'notifyReactoryActivity requires REACTORY_API_URL, REACTORY_CLIENT_KEY and REACTORY_CLIENT_PWD',
    );
  }

  const response = await fetch(`${apiUrl.replace(/\/$/, '')}/api/temporal/v1/events`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-client-key': clientKey,
      'x-client-pwd': clientPwd,
    },
    body: JSON.stringify({ eventName, eventKey, data: data || {} }),
  });

  if (!response.ok) {
    // Throwing lets Temporal's activity retry policy handle a transient Reactory
    // outage — which is exactly the behaviour we want for a notification.
    const body = await response.text().catch(() => '');
    throw new Error(`Reactory callback failed (${response.status}): ${body.slice(0, 300)}`);
  }

  const payload = (await response.json().catch(() => ({}))) as {
    accepted?: boolean;
    watchAcked?: boolean;
  };
  return { accepted: payload.accepted !== false, watchAcked: payload.watchAcked };
}
