/**
 * TemporalSignalStep (`temporal_signal`) — send a Signal to a Temporal execution.
 *
 * This is the Reactory → Temporal half of a human-in-the-loop gate: a Reactory form
 * approval or `user_activity` completes, and this step releases the Temporal
 * workflow's `await condition(...)`.
 *
 * Config:
 *   workflowId:   "batch_${input.batchId}"   (required)
 *   runId:        "${input.runId}"           (optional — omit to target the latest run)
 *   signalName:   "approveBatch"             (required)
 *   args:         ["${variables.approverId}"] (positional signal arguments)
 *   startIfNotRunning: { workflowType, taskQueue, args }
 *                                            (optional — signalWithStart semantics:
 *                                             start the workflow if it is not running,
 *                                             then deliver the signal)
 *
 * Outputs: { workflowId, runId, signalled, started }
 */

import {
  StepExecutionContext,
  StepExecutionResult,
  ValidationResult,
} from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/interfaces/IYamlStep';
import { BaseTemporalStep } from './BaseTemporalStep';

export class TemporalSignalStep extends BaseTemporalStep {
  public readonly stepType = 'temporal_signal';

  protected async executeStep(context: StepExecutionContext): Promise<StepExecutionResult> {
    if (!context.reactoryContext) return this.noContext();
    const service = this.getTemporalService(context);
    if (!service) return this.serviceUnavailable({ stepType: this.stepType });

    const workflowId = this.required(this.config.workflowId, context);
    const runId = this.optional(this.config.runId, context);
    const signalName = this.required(this.config.signalName, context);
    const args = this.resolveDeep(this.config.args ?? [], context);
    const connection = this.resolveConnection(context);
    const startIfNotRunning = this.config.startIfNotRunning;

    try {
      // signalWithStart is the correct primitive when the caller is willing to create
      // the workflow: it is atomic, so there is no race between "is it running?" and
      // the signal delivery.
      if (startIfNotRunning?.workflowType) {
        const result = await service.startWorkflow(
          {
            workflowType: this.required(startIfNotRunning.workflowType, context),
            workflowId,
            taskQueue:
              this.optional(startIfNotRunning.taskQueue, context) || process.env.TEMPORAL_TASK_QUEUE || '',
            args: this.resolveDeep(startIfNotRunning.args ?? [], context),
            signalOnStart: { name: signalName, args },
          },
          connection,
        );
        context.logger.info(
          `Signalled "${signalName}" via signalWithStart on ${this.describeTarget(workflowId, result.runId)}`,
        );
        return {
          success: true,
          outputs: { workflowId, runId: result.runId, signalled: true, started: true },
          metadata: { workflowId, signalName, signalWithStart: true },
        };
      }

      await service.signalWorkflow({ workflowId, runId, signalName, args }, connection);
      context.logger.info(`Signalled "${signalName}" on ${this.describeTarget(workflowId, runId)}`);
      return {
        success: true,
        outputs: { workflowId, runId, signalled: true, started: false },
        metadata: { workflowId, signalName },
      };
    } catch (error: any) {
      context.logger.error(`Failed to signal "${signalName}" on ${workflowId}: ${error.message}`);
      return {
        success: false,
        error: `Failed to signal "${signalName}" on Temporal workflow ${workflowId}: ${error.message}`,
        outputs: { workflowId, runId, signalled: false },
        metadata: { workflowId, signalName },
      };
    }
  }

  public validateConfig(config: Record<string, any>): ValidationResult {
    const errors: string[] = [];
    if (!config.workflowId || typeof config.workflowId !== 'string') {
      errors.push('workflowId is required and must be a string');
    }
    if (!config.signalName || typeof config.signalName !== 'string') {
      errors.push('signalName is required and must be a string');
    }
    if (config.args !== undefined && !Array.isArray(config.args)) {
      errors.push('args must be an array (signal arguments are positional)');
    }
    if (config.startIfNotRunning !== undefined) {
      if (typeof config.startIfNotRunning !== 'object' || !config.startIfNotRunning.workflowType) {
        errors.push('startIfNotRunning must be an object with a "workflowType" property');
      }
    }
    return { valid: errors.length === 0, errors };
  }
}
