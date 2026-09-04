import { join } from 'path';
import { loadYamlWorkflow } from '@reactory/server-modules/reactory-core/workflow/YamlFlow/YamlToWorkflow';

/**
 * Workflows this module registers with the Reactory engine.
 *
 * Only entries with `category: 'workflow'` are loaded (see
 * WorkflowRunner.getDefaultWorkflows) — `loadYamlWorkflow` produces that shape.
 *
 * The B2B payout sample under this directory (workflows.ts / worker.ts) is
 * Temporal-side code: it runs on a Temporal worker, not the Reactory engine, and is
 * deliberately NOT registered here.
 */
/** Engine workflows shipped in this directory (alongside the Temporal-side sample). */
const WORKFLOWS_DIR = __dirname;
/** Example workflows demonstrating the integration. */
const EXAMPLES_DIR = join(__dirname, '..', 'examples');

const DEFINITIONS: Array<{ filename: string; name: string; autoStart?: boolean }> = [
  // Activates the event bridge at startup (see the file header for why it is YAML).
  { filename: 'TemporalBridgeBootstrap.yaml', name: 'TemporalBridgeBootstrap', autoStart: true },
];

/** Example workflows demonstrating the integration; registered, never auto-started. */
const EXAMPLES: Array<{ filename: string; name: string }> = [
  { filename: 'DispatchBatchPayout.yaml', name: 'DispatchBatchPayout' },
  { filename: 'ApproveBatchPayout.yaml', name: 'ApproveBatchPayout' },
];

/**
 * Guarded on REACTORY_DATA because loadYamlWorkflow resolves the catalog path from it
 * eagerly — an unset variable (tests, tooling) would throw at import time rather than
 * degrade.
 */
function load(
  nameSpace: string,
  sourceDir: string,
  entries: Array<{ filename: string; name: string; autoStart?: boolean }>,
): Reactory.Workflow.IWorkflow[] {
  if (!process.env.REACTORY_DATA) return [];
  return entries
    .map(({ filename, name, autoStart }) => {
      const workflow = loadYamlWorkflow(nameSpace, name, filename, '1.0.0', sourceDir);
      if (workflow && autoStart) {
        return { ...workflow, autoStart: true } as Reactory.Workflow.IWorkflow;
      }
      return workflow;
    })
    .filter((workflow): workflow is Reactory.Workflow.IWorkflow => workflow != null);
}

const workflows: Reactory.Workflow.IWorkflow[] = [
  ...load('reactory-temporal', WORKFLOWS_DIR, DEFINITIONS),
  ...load('reactory-temporal-examples', EXAMPLES_DIR, EXAMPLES),
];

export default workflows;
