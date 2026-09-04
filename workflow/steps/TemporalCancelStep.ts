/**
 * TemporalCancelStep (`temporal_cancel`) — request graceful cancellation of a
 * Temporal execution.
 *
 * Cancellation is cooperative: Temporal delivers it to the workflow, which can run
 * its cleanup before finishing. The step returns as soon as the request is accepted
 * and does NOT wait for the run to close — pair it with `temporal_await` (or
 * `temporal_describe`) when the flow needs the final state.
 *
 * Config:
 *   workflowId: "batch_${input.batchId}"  (required)
 *   runId:      "${input.runId}"          (optional)
 *   ignoreNotFound: true                  (optional — treat an unknown/closed run as
 *                                          success; makes cancellation idempotent in
 *                                          compensation/saga paths)
 *
 * Outputs: { workflowId, runId, cancelled, notFound? }
 */

import {
  StepExecutionContext,
  StepExecutionResult,
  ValidationResult,
} from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/interfaces/IYamlStep';
import { BaseTemporalStep } from './BaseTemporalStep';

/** Temporal error names that mean "there is nothing there to cancel". */
function isNotFound(error: any): boolean {
  const name = error?.name || '';
  const message = error?.message || '';
  return (
    name === 'WorkflowNotFoundError' ||
    /not found|NOT_FOUND|workflow execution already completed/i.test(message)
  );
}

export class TemporalCancelStep extends BaseTemporalStep {
  public readonly stepType = 'temporal_cancel';

  protected async executeStep(context: StepExecutionContext): Promise<StepExecutionResult> {
    if (!context.reactoryContext) return this.noContext();
    const service = this.getTemporalService(context);
    if (!service) return this.serviceUnavailable({ stepType: this.stepType });

    const workflowId = this.required(this.config.workflowId, context);
    const runId = this.optional(this.config.runId, context);
    const ignoreNotFound = this.config.ignoreNotFound === true;

    try {
      await service.cancelWorkflow(workflowId, runId, this.resolveConnection(context));
      context.logger.info(`Requested cancellation of ${this.describeTarget(workflowId, runId)}`);
      return {
        success: true,
        outputs: { workflowId, runId, cancelled: true },
        metadata: { workflowId },
      };
    } catch (error: any) {
      if (ignoreNotFound && isNotFound(error)) {
        context.logger.warn(
          `Temporal run ${workflowId} not found or already closed — treating cancellation as satisfied`,
        );
        return {
          success: true,
          outputs: { workflowId, runId, cancelled: false, notFound: true },
          metadata: { workflowId, ignoredNotFound: true },
        };
      }
      context.logger.error(`Failed to cancel ${workflowId}: ${error.message}`);
      return {
        success: false,
        error: `Failed to cancel Temporal workflow ${workflowId}: ${error.message}`,
        outputs: { workflowId, runId, cancelled: false },
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
