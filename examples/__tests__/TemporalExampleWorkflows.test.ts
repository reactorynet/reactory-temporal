/**
 * Parse + registration tests for the module's YAML examples.
 *
 * These are the integration guard for two things that are easy to get wrong and
 * silently break every Temporal workflow:
 *
 *   1. step types must be listed in reactory-core's WorkflowSchema.json `type` enum —
 *      the parser Ajv-validates against it BEFORE the registry is consulted, so an
 *      unlisted type fails to parse with no reference to the registry at all;
 *   2. the module's providers must construct and validate through the shared
 *      registry, exactly as WorkflowRunner.discoverModuleSteps() registers them.
 */

import { join } from 'path';
import { YamlFlowParser } from '@reactory/server-modules/reactory-core/workflow/YamlFlow/YamlFlowParser';
import { YamlStepRegistry } from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/registry/YamlStepRegistry';
import { workflowSteps } from '../../workflow/steps';

const EXAMPLES_DIR = join(__dirname, '..');
const WORKFLOWS_DIR = join(__dirname, '..', '..', 'workflows');

/** Register the module's steps the way the WorkflowRunner does at startup. */
function registryWithTemporalSteps(): YamlStepRegistry {
  const registry = new YamlStepRegistry();
  for (const provider of workflowSteps) {
    registry.registerStep(
      provider.stepType,
      provider.constructor as any,
      provider.options || {},
      (provider as any).definition,
      'module',
    );
  }
  return registry;
}

describe('temporal step registration', () => {
  it('registers every Temporal step type into the shared registry', () => {
    const registry = registryWithTemporalSteps();
    for (const stepType of [
      'temporal_workflow',
      'temporal_await',
      'temporal_signal',
      'temporal_query',
      'temporal_update',
      'temporal_cancel',
      'temporal_terminate',
      'temporal_describe',
    ]) {
      expect(registry.hasStep(stepType)).toBe(true);
    }
  });

  it('exposes each step in the designer catalog with a definition', () => {
    const catalog = registryWithTemporalSteps().getStepCatalog();
    const temporal = catalog.filter((entry) => entry.stepType.startsWith('temporal_'));
    expect(temporal).toHaveLength(8);
    for (const entry of temporal) {
      expect(entry.source).toBe('module');
      expect(entry.definition).toBeDefined();
      // The designer needs a property schema to render the config form.
      expect(entry.definition?.propertySchema).toBeDefined();
    }
  });

  it('creates a step instance through the registry with YAML-shaped inputs', () => {
    const registry = registryWithTemporalSteps();
    const step = registry.createStep({
      id: 'payout',
      type: 'temporal_workflow',
      // The designer stores config as a JSON string in `inputs`.
      inputs: JSON.stringify({
        workflowType: 'B2BMassPayoutWorkflow',
        workflowId: 'batch_${input.batchId}',
        awaitMode: 'event',
      }) as any,
    });
    expect(step.stepType).toBe('temporal_workflow');
    expect(step.config.workflowType).toBe('B2BMassPayoutWorkflow');
  });

  it('rejects an invalid step configuration at creation time', () => {
    const registry = registryWithTemporalSteps();
    expect(() =>
      registry.createStep({
        id: 'bad',
        type: 'temporal_signal',
        config: { workflowId: 'batch_1' }, // missing signalName
      }),
    ).toThrow(/signalName is required/);
  });
});

describe('example YAML workflows', () => {
  const parser = new YamlFlowParser();

  it.each([
    ['DispatchBatchPayout.yaml', 'DispatchBatchPayout'],
    ['ApproveBatchPayout.yaml', 'ApproveBatchPayout'],
  ])('parses %s against the workflow schema', (filename, name) => {
    const result = parser.parseFromFile(join(EXAMPLES_DIR, filename));

    // A step type missing from WorkflowSchema.json's enum surfaces here.
    expect(result.errors ?? []).toEqual([]);
    expect(result.success).toBe(true);
    expect(result.workflow?.name).toBe(name);
    expect(result.workflow?.nameSpace).toBe('reactory-temporal-examples');
  });

  it('validates DispatchBatchPayout against the live step registry', async () => {
    const result = parser.parseFromFile(join(EXAMPLES_DIR, 'DispatchBatchPayout.yaml'));
    expect(result.success).toBe(true);

    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { YamlWorkflowExecutor } = require('@reactory/server-modules/reactory-core/workflow/YamlFlow/execution/YamlWorkflowExecutor');
    const executor = new YamlWorkflowExecutor(registryWithTemporalSteps());

    const validation = await executor.validateWorkflow(result.workflow!);
    expect(validation.errors).toEqual([]);
    expect(validation.valid).toBe(true);
  });

  it('validates ApproveBatchPayout, including its wait_event + signal handshake', async () => {
    const result = parser.parseFromFile(join(EXAMPLES_DIR, 'ApproveBatchPayout.yaml'));
    expect(result.success).toBe(true);

    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { YamlWorkflowExecutor } = require('@reactory/server-modules/reactory-core/workflow/YamlFlow/execution/YamlWorkflowExecutor');
    const executor = new YamlWorkflowExecutor(registryWithTemporalSteps());

    const validation = await executor.validateWorkflow(result.workflow!);
    expect(validation.errors).toEqual([]);
    expect(validation.valid).toBe(true);

    const types = result.workflow!.steps.map((step: any) => step.type);
    expect(types).toEqual([
      'temporal_workflow',
      'wait_event',
      'temporal_signal',
      'temporal_await',
      'log',
    ]);
  });

  it('parses the bridge bootstrap workflow', () => {
    const result = parser.parseFromFile(join(WORKFLOWS_DIR, 'TemporalBridgeBootstrap.yaml'));
    expect(result.errors ?? []).toEqual([]);
    expect(result.success).toBe(true);
    // Registered as a YAML workflow specifically so the auto-start path — which
    // raises the start event with the BARE workflow name — can resolve it.
    expect(result.workflow?.name).toBe('TemporalBridgeBootstrap');
    expect(result.workflow?.steps[0]).toMatchObject({
      type: 'service_invoke',
      continueOnError: true,
    });
  });

  it('builds a durable engine class from the example definition', () => {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { buildYamlWorkflowClass, engineWorkflowId } = require('@reactory/server-modules/reactory-core/workflow/YamlFlow/YamlFlowBuilder');
    const result = parser.parseFromFile(join(EXAMPLES_DIR, 'DispatchBatchPayout.yaml'));
    const WorkflowClass: any = buildYamlWorkflowClass(result.workflow!);
    const instance = new WorkflowClass();

    expect(instance.id).toBe(engineWorkflowId(result.workflow!));
    expect(instance.version).toBe('1.0.0');
    // The content digest protects in-flight instances from an edited definition.
    expect(instance.fingerprintSeed).toHaveLength(32);
  });
});
