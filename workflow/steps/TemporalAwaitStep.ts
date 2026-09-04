/**
 * TemporalAwaitStep (`temporal_await`) — await the outcome of a Temporal execution
 * that was started somewhere else: by an earlier `temporal_workflow` step with
 * `awaitMode: none`, by the `temporalStartWorkflow` GraphQL mutation, by another
 * service, or by an operator.
 *
 * It is `temporal_workflow` minus the start, so the await machinery (durable event
 * suspension, durable sleep-poll, in-step blocking, timeout accounting, result
 * normalisation) is inherited rather than duplicated.
 *
 * Config:
 *   workflowId:   "batch_${input.batchId}"   (required)
 *   runId:        "${input.runId}"           (optional — omit to await the latest run)
 *   awaitMode:    event | sleep_poll | block_poll   (default event; "none" is invalid
 *                                                    — awaiting IS the point)
 *   awaitTimeout / pollInterval / eventName / eventKey / resultPath /
 *   maxResultBytes / failOnWorkflowFailure   (as per temporal_workflow)
 *
 * Outputs: { workflowId, runId, status, started: false, reattached: true, awaited,
 *            result?, failure? }
 */

import {
  StepExecutionContext,
  StepExecutionResult,
  ValidationResult,
} from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/interfaces/IYamlStep';
import {
  AWAIT_MODES,
  DEFAULT_AWAIT_TIMEOUT_MS,
  DEFAULT_POLL_INTERVAL_MS,
  PollState,
  TemporalAwaitMode,
  TemporalWorkflowStep,
} from './TemporalWorkflowStep';
import { DEFAULT_MAX_RESULT_BYTES, isTerminalStatus } from '../../services/TemporalClientService';
import { ITemporalStartWorkflowResult, TemporalExecutionStatus } from '../../types';

/** Await modes that make sense for a step whose only job is to wait. */
const AWAITING_MODES = AWAIT_MODES.filter((mode) => mode !== 'none');

export class TemporalAwaitStep extends TemporalWorkflowStep {
  public readonly stepType: string = 'temporal_await';

  protected async executeStep(context: StepExecutionContext): Promise<StepExecutionResult> {
    if (!context.reactoryContext) return this.noContext();
    const service = this.getTemporalService(context);
    if (!service) return this.serviceUnavailable({ stepType: this.stepType });

    const workflowId = this.required(this.config.workflowId, context);
    const connection = this.resolveConnection(context);
    const resultPath = this.optional(this.config.resultPath, context);
    const maxResultBytes =
      typeof this.config.maxResultBytes === 'number' ? this.config.maxResultBytes : DEFAULT_MAX_RESULT_BYTES;

    // Resumed by a bridge event.
    if (context.control?.eventPublished) {
      const outcome = this.outcomeFromEvent(context.control.eventData, workflowId);
      context.logger.info(`Awaited Temporal run ${workflowId} settled with status ${outcome.status}`);
      return this.finish(
        outcome,
        { started: false, reattached: true, awaited: true },
        resultPath,
        context,
        maxResultBytes,
      );
    }

    // Resumed from a durable sleep-poll tick.
    const pollState = context.control?.persistenceData as PollState | undefined;
    if (pollState?.temporalWorkflowId) {
      return this.continuePolling(pollState, { service, connection, resultPath, maxResultBytes }, context);
    }

    // First run — resolve the target run and short-circuit if it is already terminal.
    const runId = this.optional(this.config.runId, context);
    const awaitTimeout = this.numberConfig('awaitTimeout', DEFAULT_AWAIT_TIMEOUT_MS);
    const pollInterval = this.numberConfig('pollInterval', DEFAULT_POLL_INTERVAL_MS);
    const provenance = { started: false, reattached: true, awaited: false };

    const described = await this.safeDescribe(service, workflowId, runId, connection, context);
    if (!described) {
      return {
        success: false,
        error:
          `Cannot await Temporal workflow ${workflowId} — the execution could not be described ` +
          '(unknown workflowId, wrong namespace, or the cluster is unreachable)',
        outputs: { workflowId, runId, found: false },
        metadata: { workflowId },
      };
    }

    if (isTerminalStatus(described.status)) {
      const outcome = await service.awaitOutcome(workflowId, runId, connection, maxResultBytes);
      context.logger.info(`Temporal run ${workflowId} already terminal (${outcome.status}) — not waiting`);
      return this.finish(outcome, { ...provenance, awaited: true }, resultPath, context, maxResultBytes);
    }

    // The inherited await strategies are written against a start result; this step
    // adopts an existing run, so synthesise the equivalent target descriptor.
    const target: ITemporalStartWorkflowResult = {
      workflowId,
      runId: runId || (described as any).runId || '',
      started: false,
      reattached: true,
    };

    const requested = (this.optional(this.config.awaitMode, context) as TemporalAwaitMode) || 'event';
    const mode = await this.resolveAwaitMode(requested, context);

    if (mode === 'block_poll') {
      return this.blockingAwait(
        target,
        { service, connection, resultPath, maxResultBytes, awaitTimeout },
        provenance,
        context,
      );
    }

    if (mode === 'event') {
      return this.suspendOnEvent(
        target,
        { service, connection, resultPath, maxResultBytes, awaitTimeout },
        provenance,
        context,
      );
    }

    const state: PollState = {
      temporalWorkflowId: target.workflowId,
      temporalRunId: target.runId,
      startedAt: Date.now(),
      deadline: Date.now() + awaitTimeout,
      started: false,
      reattached: true,
      attempts: 0,
    };
    return {
      success: true,
      outputs: {
        workflowId: target.workflowId,
        runId: target.runId,
        status: described.status as TemporalExecutionStatus,
        ...provenance,
      },
      metadata: { workflowId: target.workflowId, awaitMode: 'sleep_poll' },
      control: { sleep: { durationMs: pollInterval }, persist: state as any },
    };
  }

  public validateConfig(config: Record<string, any>): ValidationResult {
    const errors: string[] = [];
    if (!config.workflowId || typeof config.workflowId !== 'string') {
      errors.push('workflowId is required and must be a string');
    }
    if (config.awaitMode !== undefined && !AWAITING_MODES.includes(config.awaitMode)) {
      errors.push(`awaitMode must be one of: ${AWAITING_MODES.join(', ')} (temporal_await always waits)`);
    }
    for (const key of ['awaitTimeout', 'pollInterval', 'maxResultBytes']) {
      if (config[key] !== undefined && (typeof config[key] !== 'number' || config[key] <= 0)) {
        errors.push(`${key} must be a positive number`);
      }
    }
    return { valid: errors.length === 0, errors };
  }
}
