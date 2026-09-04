/**
 * Temporal REST surface.
 *
 * The callback endpoint exists for Temporal workers that Reactory did not start and
 * cannot watch in-process (externally hosted workers, other languages, other
 * clusters). Such a worker calls back with an outcome or a progress event, and the
 * bridge turns it into a workflow-engine event that resumes the waiting YAML step.
 *
 * AUTH: `ReactoryClientAuthenticationMiddleware` is installed globally (see
 * express/middleware/index.ts) ahead of route handling, so every request here has
 * already been authenticated with `x-client-key` / `x-client-pwd` and carries
 * `req.partner`. The handlers assert that rather than re-implementing auth.
 */

import express, { Response } from 'express';
import logger from '@reactory/server-core/logging';
import {
  ALLOWED_EVENT_PREFIX,
  DEFAULT_SETTLED_EVENT,
  TEMPORAL_EVENT_BRIDGE_SERVICE_ID,
  ITemporalEventBridge,
} from '../workflow/events';
import TemporalWorkflowWatchModel from '../models/TemporalWorkflowWatch';

/** Statuses a caller may report on a settle callback. */
const REPORTABLE_STATUSES = [
  'COMPLETED',
  'FAILED',
  'CANCELED',
  'TERMINATED',
  'TIMED_OUT',
  'CONTINUED_AS_NEW',
  'AWAIT_TIMEOUT',
];

const router = express.Router();

/** Resolve the bridge from the request context, or 503 the caller. */
function resolveBridge(req: any, res: Response): ITemporalEventBridge | null {
  try {
    const bridge = req.context?.getService(TEMPORAL_EVENT_BRIDGE_SERVICE_ID) as ITemporalEventBridge;
    if (bridge) return bridge;
  } catch (error: any) {
    logger.error(`Temporal callback: bridge unavailable — ${error.message}`);
  }
  res.status(503).json({
    success: false,
    error: { message: `${TEMPORAL_EVENT_BRIDGE_SERVICE_ID} is not available` },
  });
  return null;
}

/**
 * @route POST /api/temporal/v1/events
 * @description Publish a Temporal event into the Reactory workflow engine, waking any
 *              step suspended on (eventName, eventKey).
 * @access Partner (global client-key authentication)
 *
 * Body: { eventName?, eventKey, workflowId?, runId?, data? }
 *   eventName defaults to temporal.workflow.settled — the topic awaiting steps use.
 *   eventKey is the correlation key, which for the standard flow is the Temporal
 *   workflowId.
 */
router.post('/events', async (req: any, res: Response) => {
  const partner = req.partner;
  if (!partner) {
    return res.status(401).json({ success: false, error: { message: 'Unauthenticated' } });
  }

  const { eventName = DEFAULT_SETTLED_EVENT, eventKey, workflowId, runId, data } = req.body || {};

  if (typeof eventName !== 'string' || !eventName.startsWith(ALLOWED_EVENT_PREFIX)) {
    // Constrained on purpose: this endpoint must not become a way to publish
    // arbitrary events into any workflow in the system.
    return res.status(400).json({
      success: false,
      error: { message: `eventName must be a string beginning with "${ALLOWED_EVENT_PREFIX}"` },
    });
  }

  const correlationKey = eventKey || workflowId;
  if (!correlationKey || typeof correlationKey !== 'string') {
    return res.status(400).json({
      success: false,
      error: { message: 'eventKey (or workflowId) is required' },
    });
  }

  if (data?.status !== undefined && !REPORTABLE_STATUSES.includes(data.status)) {
    return res.status(400).json({
      success: false,
      error: { message: `data.status must be one of: ${REPORTABLE_STATUSES.join(', ')}` },
    });
  }

  const bridge = resolveBridge(req, res);
  if (!bridge) return undefined;

  try {
    const result = await bridge.settleFromCallback({
      workflowId: workflowId || correlationKey,
      runId,
      eventName,
      eventKey: correlationKey,
      data: data || {},
    });
    logger.info(
      `Temporal callback published '${eventName}' (key: ${correlationKey}, acked: ${result.watchAcked})`,
    );
    return res.status(200).json({ success: true, accepted: true, watchAcked: result.watchAcked });
  } catch (error: any) {
    logger.error(`Temporal callback failed for '${eventName}' (${correlationKey}): ${error.message}`);
    return res.status(500).json({ success: false, error: { message: error.message } });
  }
});

/**
 * @route GET /api/temporal/v1/watches
 * @description Operational visibility into outstanding completion watches for the
 *              calling partner — which runs the engine is still waiting on, and
 *              whether any delivery has been abandoned.
 * @access Partner
 */
router.get('/watches', async (req: any, res: Response) => {
  const partner = req.partner;
  if (!partner) {
    return res.status(401).json({ success: false, error: { message: 'Unauthenticated' } });
  }

  const { status, workflowId, limit } = req.query;
  const query: Record<string, any> = { clientKey: partner.key };
  if (status) query.status = status;
  if (workflowId) query.workflowId = workflowId;

  try {
    const watches = await TemporalWorkflowWatchModel.find(query)
      .sort({ updatedAt: -1 })
      .limit(Math.min(Number(limit) || 50, 200))
      .exec();

    return res.status(200).json({
      success: true,
      data: watches.map((watch) => ({
        id: watch._id.toString(),
        workflowId: watch.workflowId,
        runId: watch.runId,
        eventName: watch.eventName,
        eventKey: watch.eventKey,
        status: watch.status,
        outcomeStatus: watch.outcome?.status ?? null,
        instanceId: watch.instanceId,
        stepId: watch.stepId,
        attempts: watch.attempts,
        lastError: watch.lastError ?? null,
        awaitDeadline: watch.awaitDeadline ?? null,
        createdAt: watch.createdAt,
        settledAt: watch.settledAt ?? null,
        ackedAt: watch.ackedAt ?? null,
      })),
    });
  } catch (error: any) {
    logger.error(`Failed to list Temporal watches: ${error.message}`);
    return res.status(500).json({ success: false, error: { message: error.message } });
  }
});

/**
 * Module route map: mount path → router. ConfigureRoutes (express/routes/index.ts)
 * consumes this shape directly — the keys ARE the mount paths and the values must be
 * routers, not factories.
 */
export default {
  '/api/temporal/v1': router,
};
