/**
 * TemporalQueryStep (`temporal_query`) — run a read-only Query against a Temporal
 * execution and surface the result to the workflow.
 *
 * Queries are synchronous and side-effect free, so this step never suspends.
 *
 * Config:
 *   workflowId:  "batch_${input.batchId}"  (required)
 *   runId:       "${input.runId}"          (optional)
 *   queryType:   "getBatchProgress"        (required)
 *   args:        []                        (positional query arguments)
 *   resultPath:  "processedCount"          (optional — extract a subtree)
 *   maxResultBytes: 65536                  (optional — cap on stored payload)
 *
 * Outputs: { workflowId, runId, result } (or { truncated, bytes } when oversized)
 */

import {
  StepExecutionContext,
  StepExecutionResult,
  ValidationResult,
} from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/interfaces/IYamlStep';
import { BaseTemporalStep } from './BaseTemporalStep';
import { DEFAULT_MAX_RESULT_BYTES, normaliseResult } from '../../services/TemporalClientService';

export class TemporalQueryStep extends BaseTemporalStep {
  public readonly stepType = 'temporal_query';

  protected async executeStep(context: StepExecutionContext): Promise<StepExecutionResult> {
    if (!context.reactoryContext) return this.noContext();
    const service = this.getTemporalService(context);
    if (!service) return this.serviceUnavailable({ stepType: this.stepType });

    const workflowId = this.required(this.config.workflowId, context);
    const runId = this.optional(this.config.runId, context);
    const queryType = this.required(this.config.queryType, context);
    const args = this.resolveDeep(this.config.args ?? [], context);
    const resultPath = this.optional(this.config.resultPath, context);
    const maxResultBytes =
      typeof this.config.maxResultBytes === 'number' ? this.config.maxResultBytes : DEFAULT_MAX_RESULT_BYTES;

    try {
      const raw = await service.queryWorkflow({ workflowId, runId, queryType, args }, this.resolveConnection(context));
      const extracted = resultPath ? this.extractPath(raw, resultPath, context) : raw;
      const normalised = normaliseResult(extracted, maxResultBytes);
      if (normalised.truncated) {
        context.logger.warn(
          `Query "${queryType}" result for ${workflowId} exceeded ${maxResultBytes} bytes (${normalised.bytes}) and was not stored`,
        );
      }
      return {
        success: true,
        outputs: {
          workflowId,
          runId,
          ...(normalised.truncated ? { truncated: true, bytes: normalised.bytes } : { result: normalised.result }),
        },
        metadata: { workflowId, queryType },
      };
    } catch (error: any) {
      context.logger.error(`Temporal query "${queryType}" on ${workflowId} failed: ${error.message}`);
      return {
        success: false,
        error: `Temporal query "${queryType}" on ${workflowId} failed: ${error.message}`,
        outputs: { workflowId, runId },
        metadata: { workflowId, queryType },
      };
    }
  }

  public validateConfig(config: Record<string, any>): ValidationResult {
    const errors: string[] = [];
    if (!config.workflowId || typeof config.workflowId !== 'string') {
      errors.push('workflowId is required and must be a string');
    }
    if (!config.queryType || typeof config.queryType !== 'string') {
      errors.push('queryType is required and must be a string');
    }
    if (config.args !== undefined && !Array.isArray(config.args)) {
      errors.push('args must be an array (query arguments are positional)');
    }
    return { valid: errors.length === 0, errors };
  }
}
