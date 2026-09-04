/**
 * TemporalDescribeStep (`temporal_describe`) — read an execution's status and
 * metadata without waiting for it.
 *
 * Useful for branching (`condition` on `${steps.check.outputs.status}`) and for
 * observability steps that record progress into Reactory.
 *
 * Config:
 *   workflowId:     "batch_${input.batchId}"  (required)
 *   runId:          "${input.runId}"          (optional)
 *   failIfNotFound: false                     (default false — an unknown run yields
 *                                              status UNKNOWN + found:false rather
 *                                              than failing the step)
 *
 * Outputs: { workflowId, runId, status, workflowType, taskQueue, startTime,
 *            closeTime, historyLength, found, terminal }
 */

import {
  StepExecutionContext,
  StepExecutionResult,
  ValidationResult,
} from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/interfaces/IYamlStep';
import { BaseTemporalStep } from './BaseTemporalStep';
import { isTerminalStatus } from '../../services/TemporalClientService';

export class TemporalDescribeStep extends BaseTemporalStep {
  public readonly stepType = 'temporal_describe';

  protected async executeStep(context: StepExecutionContext): Promise<StepExecutionResult> {
    if (!context.reactoryContext) return this.noContext();
    const service = this.getTemporalService(context);
    if (!service) return this.serviceUnavailable({ stepType: this.stepType });

    const workflowId = this.required(this.config.workflowId, context);
    const runId = this.optional(this.config.runId, context);
    const failIfNotFound = this.config.failIfNotFound === true;

    try {
      const described = await service.describeWorkflow(workflowId, runId, this.resolveConnection(context));
      return {
        success: true,
        outputs: {
          ...described,
          found: true,
          terminal: isTerminalStatus(described.status),
        },
        metadata: { workflowId, status: described.status },
      };
    } catch (error: any) {
      if (failIfNotFound) {
        context.logger.error(`Failed to describe ${workflowId}: ${error.message}`);
        return {
          success: false,
          error: `Failed to describe Temporal workflow ${workflowId}: ${error.message}`,
          outputs: { workflowId, runId, found: false },
          metadata: { workflowId },
        };
      }
      context.logger.warn(`Could not describe ${workflowId} (${error.message}) — reporting UNKNOWN`);
      return {
        success: true,
        outputs: { workflowId, runId, status: 'UNKNOWN', found: false, terminal: false },
        metadata: { workflowId, describeError: error.message },
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
