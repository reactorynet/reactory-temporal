/**
 * TemporalTerminateStep (`temporal_terminate`) — terminate a Temporal execution
 * immediately.
 *
 * Termination is NOT cooperative: the workflow gets no chance to run cleanup, and
 * any in-flight activity is abandoned. Prefer `temporal_cancel` unless the run must
 * be stopped unconditionally.
 *
 * Config:
 *   workflowId: "batch_${input.batchId}"       (required)
 *   runId:      "${input.runId}"               (optional)
 *   reason:     "superseded by ${input.newId}" (optional, recorded in Temporal history)
 *   ignoreNotFound: true                       (optional — idempotent termination)
 *
 * Outputs: { workflowId, runId, terminated, notFound? }
 */

import {
  StepExecutionContext,
  StepExecutionResult,
  ValidationResult,
} from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/interfaces/IYamlStep';
import { BaseTemporalStep } from './BaseTemporalStep';

function isNotFound(error: any): boolean {
  const name = error?.name || '';
  const message = error?.message || '';
  return (
    name === 'WorkflowNotFoundError' ||
    /not found|NOT_FOUND|workflow execution already completed/i.test(message)
  );
}

export class TemporalTerminateStep extends BaseTemporalStep {
  public readonly stepType = 'temporal_terminate';

  protected async executeStep(context: StepExecutionContext): Promise<StepExecutionResult> {
    if (!context.reactoryContext) return this.noContext();
    const service = this.getTemporalService(context);
    if (!service) return this.serviceUnavailable({ stepType: this.stepType });

    const workflowId = this.required(this.config.workflowId, context);
    const runId = this.optional(this.config.runId, context);
    const reason = this.optional(this.config.reason, context) || `Terminated by Reactory step ${this.id}`;
    const ignoreNotFound = this.config.ignoreNotFound === true;

    try {
      await service.terminateWorkflow(workflowId, reason, runId, this.resolveConnection(context));
      context.logger.warn(`Terminated ${this.describeTarget(workflowId, runId)}: ${reason}`);
      return {
        success: true,
        outputs: { workflowId, runId, terminated: true },
        metadata: { workflowId, reason },
      };
    } catch (error: any) {
      if (ignoreNotFound && isNotFound(error)) {
        context.logger.warn(
          `Temporal run ${workflowId} not found or already closed — treating termination as satisfied`,
        );
        return {
          success: true,
          outputs: { workflowId, runId, terminated: false, notFound: true },
          metadata: { workflowId, ignoredNotFound: true },
        };
      }
      context.logger.error(`Failed to terminate ${workflowId}: ${error.message}`);
      return {
        success: false,
        error: `Failed to terminate Temporal workflow ${workflowId}: ${error.message}`,
        outputs: { workflowId, runId, terminated: false },
        metadata: { workflowId },
      };
    }
  }

  public validateConfig(config: Record<string, any>): ValidationResult {
    const errors: string[] = [];
    if (!config.workflowId || typeof config.workflowId !== 'string') {
      errors.push('workflowId is required and must be a string');
    }
    return { valid: errors.length === 0, errors };
  }
}
