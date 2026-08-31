import {
  temporalStartWorkflowMacro,
  temporalSignalWorkflowMacro,
  temporalQueryWorkflowMacro,
  temporalCancelWorkflowMacro,
} from './TemporalStepMacros';

export default [
  {
    name: 'temporalStartWorkflow',
    description: 'Starts a durable workflow execution on Temporal.io',
    fn: temporalStartWorkflowMacro,
  },
  {
    name: 'temporalSignalWorkflow',
    description: 'Sends a signal to an active Temporal.io workflow execution',
    fn: temporalSignalWorkflowMacro,
  },
  {
    name: 'temporalQueryWorkflow',
    description: 'Queries an active or completed Temporal.io workflow state',
    fn: temporalQueryWorkflowMacro,
  },
  {
    name: 'temporalCancelWorkflow',
    description: 'Requests cancellation of an active Temporal.io workflow',
    fn: temporalCancelWorkflowMacro,
  },
];
