/**
 * BaseTemporalStep — shared behaviour for every Temporal workflow step.
 *
 * Provides:
 *   • connection resolution (inline `connection`, `connectionKey`, `namespace`)
 *   • the TemporalClientService handle, with a uniform "service unavailable" failure
 *   • deep `${...}` template resolution for config values (strings, arrays, objects)
 *   • optional-token cleaning: an unsupplied optional YAML input leaves its
 *     "${input.foo}" token intact after resolution, which must be read as "absent"
 *     rather than passed to Temporal as a literal
 *   • lodash-style result extraction via `resultPath`
 *
 * Every concrete step keeps its own `executeStep`; this class deliberately does not
 * impose a template-method flow beyond the helpers.
 */

import Reactory from '@reactorynet/reactory-core';
import { BaseYamlStep } from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/base/BaseYamlStep';
import {
  StepExecutionContext,
  StepExecutionResult,
} from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/interfaces/IYamlStep';
import { TemporalClientService } from '../../services/TemporalClientService';
import { ITemporalConnectionConfig } from '../../types';

export const TEMPORAL_CLIENT_SERVICE_ID = 'reactory.TemporalClientService@1.0.0';

/** Config keys understood by every temporal step. */
export interface TemporalStepCommonConfig {
  /** Inline connection override. */
  connection?: ITemporalConnectionConfig;
  /** Named connection resolved from partner settings / environment. */
  connectionKey?: string;
  /** Shorthand for connection.namespace. */
  namespace?: string;
  enabled?: boolean;
}

export abstract class BaseTemporalStep extends BaseYamlStep {
  /**
   * Resolve the Temporal client service, or null when it is not registered.
   * Callers surface `this.serviceUnavailable()` in that case.
   */
  protected getTemporalService(context: StepExecutionContext): TemporalClientService | null {
    try {
      const service = context.reactoryContext?.getService(
        TEMPORAL_CLIENT_SERVICE_ID,
      ) as unknown as TemporalClientService;
      return service ?? null;
    } catch {
      return null;
    }
  }

  /** Uniform failure when the module's client service cannot be activated. */
  protected serviceUnavailable(extra: Record<string, any> = {}): StepExecutionResult {
    return {
      success: false,
      error: `Temporal client service (${TEMPORAL_CLIENT_SERVICE_ID}) is not available`,
      outputs: {},
      metadata: extra,
    };
  }

  /** Uniform failure when no Reactory context is available (no services, no tenant). */
  protected noContext(): StepExecutionResult {
    return {
      success: false,
      error: 'No Reactory context available — cannot reach Temporal',
      outputs: {},
      metadata: {},
    };
  }

  /**
   * Build the connection config for this step from its own config, resolving
   * templates. Returns undefined when nothing is configured, so the service falls
   * back to environment defaults.
   */
  protected resolveConnection(context: StepExecutionContext): ITemporalConnectionConfig | undefined {
    const config = this.config as TemporalStepCommonConfig;
    const inline = config.connection ? (this.resolveDeep(config.connection, context) as ITemporalConnectionConfig) : undefined;
    const connectionKey = this.optional(config.connectionKey, context);
    const namespace = this.optional(config.namespace, context) || inline?.namespace;

    if (!inline && !connectionKey && !namespace) return undefined;
    return { ...(inline || {}), connectionKey: connectionKey || inline?.connectionKey, namespace };
  }

  /**
   * Resolve an OPTIONAL string config value.
   *
   * `resolveTemplate` intentionally passes an unresolved token through unchanged, so
   * a YAML author who wires `runId: "${input.runId}"` and starts the workflow without
   * that input gets the literal string "${input.runId}". For optional values that
   * must be read as "not supplied" — otherwise we would ask Temporal for run id
   * "${input.runId}" and get a confusing not-found.
   */
  protected optional(value: string | undefined, context: StepExecutionContext): string | undefined {
    if (typeof value !== 'string') return undefined;
    const resolved = this.resolveTemplate(value, context);
    if (typeof resolved !== 'string') return resolved as any;
    const trimmed = resolved.trim();
    if (trimmed === '' || trimmed.includes('${')) return undefined;
    return trimmed;
  }

  /** Resolve a REQUIRED string config value (unresolved tokens are left visible). */
  protected required(value: string | undefined, context: StepExecutionContext): string {
    if (typeof value !== 'string') return '';
    return this.resolveTemplate(value, context);
  }

  /**
   * Deep-resolve `${...}` templates inside strings, arrays and plain objects,
   * PRESERVING TYPE for whole-value references.
   *
   * This matters for Temporal arguments specifically: they are data handed to a
   * workflow function, not text. `rows: "${input.rows}"` must arrive as an array —
   * plain template resolution would JSON-stringify it and the workflow's
   * `input.rows.reduce(...)` would throw inside Temporal, where the failure is far
   * from its cause. Mixed content ("batch_${input.id}") still interpolates.
   */
  protected resolveDeep(value: any, context: StepExecutionContext): any {
    if (typeof value === 'string') return this.resolveTemplateValue(value, context);
    if (Array.isArray(value)) return value.map((entry) => this.resolveDeep(entry, context));
    if (value && typeof value === 'object') {
      const out: Record<string, any> = {};
      for (const [key, entry] of Object.entries(value)) out[key] = this.resolveDeep(entry, context);
      return out;
    }
    return value;
  }

  /**
   * Extract a nested value from a Temporal result by dot path, used to keep large
   * payloads out of the durable workflow instance. Uses lodash from the Reactory
   * context when available, with a small built-in walk as the fallback (bare
   * test/CLI contexts have no `utils`).
   */
  protected extractPath(value: any, path: string | undefined, context: StepExecutionContext): any {
    if (!path) return value;
    const lodash = context.reactoryContext?.utils?.lodash ?? (context as any).utils?.lodash;
    if (lodash?.get) return lodash.get(value, path);
    let current: any = value;
    for (const key of path.split('.')) {
      if (current && typeof current === 'object' && key in current) current = current[key];
      else return undefined;
    }
    return current;
  }

  /** True when the durable engine can honour a suspend directive. */
  protected canSuspend(context: StepExecutionContext): boolean {
    return context.control?.supportsSuspend === true;
  }

  /**
   * Human-readable identity of the run this step is acting on, for log lines.
   */
  protected describeTarget(workflowId: string, runId?: string): string {
    return runId ? `${workflowId} (run ${runId})` : workflowId;
  }
}

/** Re-exported for the concrete steps' convenience. */
export type { Reactory };
