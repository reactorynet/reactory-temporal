/**
 * Workflow-engine integration surface for the Temporal module.
 *
 * `steps`  → IWorkflowStepProvider[] registered into the shared YAML step registry
 *            (module.workflowSteps), making Temporal invokable by `type:` in YAML.
 * `events` → the event/topic contract used to wake suspended steps.
 *
 * The previous `TemporalStepMacros` export was dead code: it was assigned to
 * `module.workflows`, which the WorkflowRunner filters to entries with
 * `category: 'workflow'` (macros were logged as "bad shape" and dropped), and the
 * YAML engine has no `macro:` step form for them to be reachable from. Steps
 * replace them.
 */

export { workflowSteps, workflowSteps as default } from './steps';
export * from './events';
