/**
 * TemporalUpdateStep (`temporal_update`) — execute a Workflow Update.
 *
 * An Update is Temporal's synchronous request/response primitive: unlike a Signal it
 * returns a value and can be validated/rejected by the workflow. `waitForStage:
 * ACCEPTED` returns as soon as the update is durably accepted; the default
 * (`COMPLETED`) awaits the handler's result.
 *
 * Config:
 *   workflowId:   "batch_${input.batchId}"  (required)
 *   runId:        "${input.runId}"          (optional)
 *   updateName:   "adjustLimit"             (required)
 *   args:         [1500]                    (positional update arguments)
 *   waitForStage: ACCEPTED | COMPLETED      (default COMPLETED)
 *   resultPath / maxResultBytes             (as per temporal_query)
 *
 * Outputs: { workflowId, runId, result?, updateId?, accepted }
 */

import {
  StepExecutionContext,
  StepExecutionResult,
  ValidationResult,
} from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/interfaces/IYamlStep';
import { BaseTemporalStep } from './BaseTemporalStep';
import { DEFAULT_MAX_RESULT_BYTES, normaliseResult } from '../../services/TemporalClientService';

const VALID_STAGES = ['ACCEPTED', 'COMPLETED'];

export class TemporalUpdateStep extends BaseTemporalStep {
  public readonly stepType = 'temporal_update';

  protected async executeStep(context: StepExecutionContext): Promise<StepExecutionResult> {
    if (!context.reactoryContext) return this.noContext();
    const service = this.getTemporalService(context);
    if (!service) return this.serviceUnavailable({ stepType: this.stepType });

    const workflowId = this.required(this.config.workflowId, context);
    const runId = this.optional(this.config.runId, context);
    const updateName = this.required(this.config.updateName, context);
    const args = this.resolveDeep(this.config.args ?? [], context);
    const waitForStage = (this.optional(this.config.waitForStage, context) as 'ACCEPTED' | 'COMPLETED') || 'COMPLETED';
    const resultPath = this.optional(this.config.resultPath, context);
    const maxResultBytes =
      typeof this.config.maxResultBytes === 'number' ? this.config.maxResultBytes : DEFAULT_MAX_RESULT_BYTES;

    try {
      const outcome = await service.updateWorkflow(
        { workflowId, runId, updateName, args, waitForStage },
        this.resolveConnection(context),
      );
      const extracted = resultPath ? this.extractPath(outcome.result, resultPath, context) : outcome.result;
      const normalised = normaliseResult(extracted, maxResultBytes);
      context.logger.info(
        `Temporal update "${updateName}" on ${this.describeTarget(workflowId, runId)} ${waitForStage === 'ACCEPTED' ? 'accepted' : 'completed'}`,
      );
      return {
        success: true,
        outputs: {
          workflowId,
          runId,
          accepted: outcome.accepted,
          ...(outcome.updateId ? { updateId: outcome.updateId } : {}),
          ...(normalised.truncated ? { truncated: true, bytes: normalised.bytes } : { result: normalised.result }),
        },
        metadata: { workflowId, updateName, waitForStage },
      };
    } catch (error: any) {
      context.logger.error(`Temporal update "${updateName}" on ${workflowId} failed: ${error.message}`);
      return {
        success: false,
        error: `Temporal update "${updateName}" on ${workflowId} failed: ${error.message}`,
        outputs: { workflowId, runId, accepted: false },
        metadata: { workflowId, updateName },
      };
    }
  }

  public validateConfig(config: Record<string, any>): ValidationResult {
    const errors: string[] = [];
    if (!config.workflowId || typeof config.workflowId !== 'string') {
      errors.push('workflowId is required and must be a string');
    }
    if (!config.updateName || typeof config.updateName !== 'string') {
      errors.push('updateName is required and must be a string');
    }
    if (config.waitForStage !== undefined && !VALID_STAGES.includes(config.waitForStage)) {
      errors.push(`waitForStage must be one of: ${VALID_STAGES.join(', ')}`);
    }
    if (config.args !== undefined && !Array.isArray(config.args)) {
      errors.push('args must be an array (update arguments are positional)');
    }
    return { valid: errors.length === 0, errors };
  }
}
