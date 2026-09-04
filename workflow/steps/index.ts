/**
 * Reactory Temporal workflow steps.
 *
 * Exports every step implementation the module contributes, plus a `workflowSteps`
 * array matching IWorkflowStepProvider[]. These providers are auto-discovered at
 * server startup by WorkflowRunner.discoverModuleSteps() and registered into the
 * shared YAML step registry, which makes them usable by `type:` in any YAML
 * workflow — on BOTH execution paths (the durable workflow-es engine and the
 * standalone YamlWorkflowExecutor).
 *
 * Each provider also attaches a `definition` (designer rendering + config schema)
 * surfaced to the Visual Workflow Designer via the `workflowStepCatalog` query.
 *
 * NOTE: a new step type must ALSO be added to the `type` enum in
 * reactory-core/workflow/YamlFlow/schema/WorkflowSchema.json — the parser Ajv-
 * validates step types against that enum, so an unlisted type fails to parse
 * before the registry is ever consulted.
 */

import Reactory from '@reactorynet/reactory-core';
import { IWorkflowStepDesignerDefinition } from '@reactory/server-modules/reactory-core/workflow/YamlFlow/types/StepDesignerDefinition';
import { TemporalWorkflowStep } from './TemporalWorkflowStep';
import { TemporalAwaitStep } from './TemporalAwaitStep';
import { TemporalSignalStep } from './TemporalSignalStep';
import { TemporalQueryStep } from './TemporalQueryStep';
import { TemporalUpdateStep } from './TemporalUpdateStep';
import { TemporalCancelStep } from './TemporalCancelStep';
import { TemporalTerminateStep } from './TemporalTerminateStep';
import { TemporalDescribeStep } from './TemporalDescribeStep';
import {
  temporalWorkflowDesigner,
  temporalAwaitDesigner,
  temporalSignalDesigner,
  temporalQueryDesigner,
  temporalUpdateDesigner,
  temporalCancelDesigner,
  temporalTerminateDesigner,
  temporalDescribeDesigner,
} from './designer/definitions';

/**
 * Local provider shape extending the published IWorkflowStepProvider with the
 * optional designer `definition`, which the discovery loop reads off each provider.
 */
interface TemporalStepProvider extends Reactory.Workflow.IWorkflowStepProvider {
  definition?: IWorkflowStepDesignerDefinition;
}

const VERSION = '1.0.0';

const providers: TemporalStepProvider[] = [
  {
    stepType: 'temporal_workflow',
    constructor: TemporalWorkflowStep as unknown as Reactory.Workflow.IStepConstructor,
    options: {
      description: 'Start a durable Temporal execution and optionally await its outcome',
      version: VERSION,
    },
    definition: temporalWorkflowDesigner,
  },
  {
    stepType: 'temporal_await',
    constructor: TemporalAwaitStep as unknown as Reactory.Workflow.IStepConstructor,
    options: {
      description: 'Await the outcome of a Temporal execution started elsewhere',
      version: VERSION,
    },
    definition: temporalAwaitDesigner,
  },
  {
    stepType: 'temporal_signal',
    constructor: TemporalSignalStep as unknown as Reactory.Workflow.IStepConstructor,
    options: { description: 'Send a Signal to a running Temporal execution', version: VERSION },
    definition: temporalSignalDesigner,
  },
  {
    stepType: 'temporal_query',
    constructor: TemporalQueryStep as unknown as Reactory.Workflow.IStepConstructor,
    options: { description: 'Run a read-only Query against a Temporal execution', version: VERSION },
    definition: temporalQueryDesigner,
  },
  {
    stepType: 'temporal_update',
    constructor: TemporalUpdateStep as unknown as Reactory.Workflow.IStepConstructor,
    options: { description: 'Execute a Temporal Workflow Update', version: VERSION },
    definition: temporalUpdateDesigner,
  },
  {
    stepType: 'temporal_cancel',
    constructor: TemporalCancelStep as unknown as Reactory.Workflow.IStepConstructor,
    options: { description: 'Request graceful cancellation of a Temporal execution', version: VERSION },
    definition: temporalCancelDesigner,
  },
  {
    stepType: 'temporal_terminate',
    constructor: TemporalTerminateStep as unknown as Reactory.Workflow.IStepConstructor,
    options: { description: 'Terminate a Temporal execution immediately', version: VERSION },
    definition: temporalTerminateDesigner,
  },
  {
    stepType: 'temporal_describe',
    constructor: TemporalDescribeStep as unknown as Reactory.Workflow.IStepConstructor,
    options: { description: 'Read a Temporal execution status and metadata', version: VERSION },
    definition: temporalDescribeDesigner,
  },
];

export const workflowSteps: Reactory.Workflow.IWorkflowStepProvider[] = providers;

export {
  TemporalWorkflowStep,
  TemporalAwaitStep,
  TemporalSignalStep,
  TemporalQueryStep,
  TemporalUpdateStep,
  TemporalCancelStep,
  TemporalTerminateStep,
  TemporalDescribeStep,
};

export default workflowSteps;
