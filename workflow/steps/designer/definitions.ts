/**
 * Visual Workflow Designer definitions for the Temporal step types.
 *
 * These are surfaced to the PWA designer through the `workflowStepCatalog` GraphQL
 * query (see YamlStepRegistry.getStepCatalog) and merged with the designer's
 * built-in step library. `propertySchema` / `uiSchema` / `rendering` are opaque to
 * the server and passed through verbatim, authored against the PWA's StepDefinition
 * shape — the same contract the reactory-reactor steps use.
 */

import { IWorkflowStepDesignerDefinition } from '@reactory/server-modules/reactory-core/workflow/YamlFlow/types/StepDesignerDefinition';

/** Shared Temporal palette + circuit rendering so the family reads as one group. */
const TEMPORAL_COLOR = '#5c6bc0';
const TEMPORAL_TAGS = ['temporal', 'durable', 'integration'];

/** Connection fields every Temporal step accepts. */
const CONNECTION_PROPERTIES = {
  connectionKey: {
    type: 'string',
    title: 'Connection',
    description: 'Named Temporal connection from partner settings / environment (blank = default)',
  },
  namespace: {
    type: 'string',
    title: 'Namespace',
    description: 'Temporal namespace override (blank = $TEMPORAL_NAMESPACE)',
  },
};

/** Build the circuit rendering block for a step. */
function circuit(labelPrefix: string, pinCount: number, height = 80) {
  return {
    webgl: {
      type: 'webgl',
      theme: 'circuit',
      circuit: {
        elementType: 'icChip',
        labelPrefix,
        colors: {
          body: 0x1a1a1a,
          bodyHover: 0x2a2a2a,
          bodySelected: 0x5c6bc0,
          pins: 0x808080,
          pinsConnected: 0xb87333,
        },
        features: { hasNotch: true, pinCount },
        dimensions: { width: 130, height },
      },
    },
  };
}

const AWAIT_MODE_OPTIONS = [
  { key: 'none', value: 'none', label: 'None — start and continue' },
  { key: 'event', value: 'event', label: 'Event — durable suspend (recommended)' },
  { key: 'sleep_poll', value: 'sleep_poll', label: 'Sleep poll — durable polling' },
  { key: 'block_poll', value: 'block_poll', label: 'Block poll — wait in-step' },
];

export const temporalWorkflowDesigner: IWorkflowStepDesignerDefinition = {
  id: 'temporal_workflow',
  name: 'Temporal Workflow',
  category: 'integration',
  description:
    'Start a durable Temporal execution and optionally await its outcome without holding a worker',
  icon: 'schedule',
  color: TEMPORAL_COLOR,
  inputPorts: [{ name: 'previous', type: 'control_input', dataType: 'any', description: 'Previous step in workflow' }],
  outputPorts: [
    { name: 'next', type: 'control_output', dataType: 'any', description: 'Next step in workflow' },
    {
      name: 'outcome',
      type: 'output',
      dataType: 'object',
      description: 'Execution outcome { workflowId, runId, status, result }',
    },
  ],
  propertySchema: {
    type: 'object',
    properties: {
      name: { type: 'string', title: 'Step Name', default: 'Temporal Workflow' },
      workflowType: {
        type: 'string',
        title: 'Workflow Type',
        description: 'The Temporal workflow function name, e.g. B2BMassPayoutWorkflow',
      },
      workflowId: {
        type: 'string',
        title: 'Workflow Id',
        description:
          'MUST be deterministic — derive it from workflow inputs (e.g. batch_${input.batchId}) so a retry re-attaches instead of starting a second run',
      },
      taskQueue: {
        type: 'string',
        title: 'Task Queue',
        description: 'Worker task queue (blank = $TEMPORAL_TASK_QUEUE)',
      },
      args: {
        type: 'array',
        title: 'Arguments',
        description: 'Positional workflow arguments (supports ${...} in any string value)',
        items: { type: 'object' },
      },
      awaitMode: { type: 'string', title: 'Await Mode', enum: AWAIT_MODE_OPTIONS.map((o) => o.value), default: 'none' },
      awaitTimeout: {
        type: 'number',
        title: 'Await Timeout (ms)',
        default: 86400000,
        description: 'Give up waiting after this long; delivered as an AWAIT_TIMEOUT outcome',
      },
      pollInterval: { type: 'number', title: 'Poll Interval (ms)', default: 10000 },
      eventName: {
        type: 'string',
        title: 'Settle Event',
        default: 'temporal.workflow.settled',
        description: 'Bridge topic the step suspends on (event mode)',
      },
      eventKey: {
        type: 'string',
        title: 'Correlation Key',
        description: 'Defaults to the Workflow Id',
      },
      resultPath: {
        type: 'string',
        title: 'Result Path',
        description: 'Extract a subtree of the result (keeps large payloads out of the instance)',
      },
      failOnWorkflowFailure: {
        type: 'boolean',
        title: 'Fail Step On Workflow Failure',
        default: true,
        description: 'Uncheck to branch on status instead of failing the Reactory workflow',
      },
      idConflictPolicy: {
        type: 'string',
        title: 'Id Conflict Policy',
        enum: ['USE_EXISTING', 'FAIL', 'TERMINATE_EXISTING'],
        default: 'USE_EXISTING',
      },
      cronSchedule: { type: 'string', title: 'Cron Schedule' },
      ...CONNECTION_PROPERTIES,
    },
    required: ['name', 'workflowType', 'workflowId'],
  },
  uiSchema: {
    'ui:order': [
      'name',
      'workflowType',
      'workflowId',
      'taskQueue',
      'args',
      'awaitMode',
      'awaitTimeout',
      'pollInterval',
      'eventName',
      'eventKey',
      'resultPath',
      'failOnWorkflowFailure',
      'idConflictPolicy',
      'cronSchedule',
      'connectionKey',
      'namespace',
    ],
    workflowId: { 'ui:help': 'Deterministic and unique per logical run — e.g. batch_${input.batchId}' },
    awaitMode: { 'ui:widget': 'SelectWidget', 'ui:options': { selectOptions: AWAIT_MODE_OPTIONS } },
    idConflictPolicy: {
      'ui:widget': 'SelectWidget',
      'ui:options': {
        selectOptions: [
          { key: 'USE_EXISTING', value: 'USE_EXISTING', label: 'Use existing run (idempotent)' },
          { key: 'FAIL', value: 'FAIL', label: 'Fail if already running' },
          { key: 'TERMINATE_EXISTING', value: 'TERMINATE_EXISTING', label: 'Terminate the existing run' },
        ],
      },
    },
  },
  defaultProperties: {
    name: 'Temporal Workflow',
    awaitMode: 'none',
    awaitTimeout: 86400000,
    pollInterval: 10000,
    idConflictPolicy: 'USE_EXISTING',
    failOnWorkflowFailure: true,
  },
  tags: [...TEMPORAL_TAGS, 'start'],
  rendering: circuit('TMP', 6, 90),
};

export const temporalAwaitDesigner: IWorkflowStepDesignerDefinition = {
  id: 'temporal_await',
  name: 'Temporal Await',
  category: 'integration',
  description: 'Await the outcome of a Temporal execution started elsewhere',
  icon: 'hourglass_top',
  color: TEMPORAL_COLOR,
  inputPorts: [{ name: 'previous', type: 'control_input', dataType: 'any', description: 'Previous step in workflow' }],
  outputPorts: [
    { name: 'next', type: 'control_output', dataType: 'any', description: 'Next step in workflow' },
    { name: 'outcome', type: 'output', dataType: 'object', description: 'Execution outcome' },
  ],
  propertySchema: {
    type: 'object',
    properties: {
      name: { type: 'string', title: 'Step Name', default: 'Temporal Await' },
      workflowId: { type: 'string', title: 'Workflow Id', description: 'Run to await (supports ${...})' },
      runId: { type: 'string', title: 'Run Id', description: 'Optional — blank awaits the latest run' },
      awaitMode: {
        type: 'string',
        title: 'Await Mode',
        enum: ['event', 'sleep_poll', 'block_poll'],
        default: 'event',
      },
      awaitTimeout: { type: 'number', title: 'Await Timeout (ms)', default: 86400000 },
      pollInterval: { type: 'number', title: 'Poll Interval (ms)', default: 10000 },
      resultPath: { type: 'string', title: 'Result Path' },
      failOnWorkflowFailure: { type: 'boolean', title: 'Fail Step On Workflow Failure', default: true },
      ...CONNECTION_PROPERTIES,
    },
    required: ['name', 'workflowId'],
  },
  uiSchema: {
    'ui:order': [
      'name',
      'workflowId',
      'runId',
      'awaitMode',
      'awaitTimeout',
      'pollInterval',
      'resultPath',
      'failOnWorkflowFailure',
      'connectionKey',
      'namespace',
    ],
    awaitMode: {
      'ui:widget': 'SelectWidget',
      'ui:options': { selectOptions: AWAIT_MODE_OPTIONS.filter((o) => o.value !== 'none') },
    },
  },
  defaultProperties: { name: 'Temporal Await', awaitMode: 'event', awaitTimeout: 86400000, pollInterval: 10000 },
  tags: [...TEMPORAL_TAGS, 'await'],
  rendering: circuit('AWT', 4),
};

export const temporalSignalDesigner: IWorkflowStepDesignerDefinition = {
  id: 'temporal_signal',
  name: 'Temporal Signal',
  category: 'integration',
  description: 'Send a Signal to a running Temporal execution (releases an await condition)',
  icon: 'bolt',
  color: TEMPORAL_COLOR,
  inputPorts: [{ name: 'previous', type: 'control_input', dataType: 'any', description: 'Previous step in workflow' }],
  outputPorts: [
    { name: 'next', type: 'control_output', dataType: 'any', description: 'Next step in workflow' },
    { name: 'result', type: 'output', dataType: 'object', description: '{ workflowId, runId, signalled }' },
  ],
  propertySchema: {
    type: 'object',
    properties: {
      name: { type: 'string', title: 'Step Name', default: 'Temporal Signal' },
      workflowId: { type: 'string', title: 'Workflow Id' },
      runId: { type: 'string', title: 'Run Id', description: 'Optional' },
      signalName: { type: 'string', title: 'Signal Name', description: 'e.g. approveBatch' },
      args: { type: 'array', title: 'Signal Arguments', items: { type: 'object' } },
      ...CONNECTION_PROPERTIES,
    },
    required: ['name', 'workflowId', 'signalName'],
  },
  uiSchema: {
    'ui:order': ['name', 'workflowId', 'signalName', 'args', 'runId', 'connectionKey', 'namespace'],
  },
  defaultProperties: { name: 'Temporal Signal' },
  tags: [...TEMPORAL_TAGS, 'signal'],
  rendering: circuit('SIG', 4),
};

export const temporalQueryDesigner: IWorkflowStepDesignerDefinition = {
  id: 'temporal_query',
  name: 'Temporal Query',
  category: 'integration',
  description: 'Run a read-only Query against a Temporal execution',
  icon: 'query_stats',
  color: TEMPORAL_COLOR,
  inputPorts: [{ name: 'previous', type: 'control_input', dataType: 'any', description: 'Previous step in workflow' }],
  outputPorts: [
    { name: 'next', type: 'control_output', dataType: 'any', description: 'Next step in workflow' },
    { name: 'result', type: 'output', dataType: 'object', description: 'Query result' },
  ],
  propertySchema: {
    type: 'object',
    properties: {
      name: { type: 'string', title: 'Step Name', default: 'Temporal Query' },
      workflowId: { type: 'string', title: 'Workflow Id' },
      runId: { type: 'string', title: 'Run Id', description: 'Optional' },
      queryType: { type: 'string', title: 'Query Name', description: 'e.g. getBatchProgress' },
      args: { type: 'array', title: 'Query Arguments', items: { type: 'object' } },
      resultPath: { type: 'string', title: 'Result Path' },
      ...CONNECTION_PROPERTIES,
    },
    required: ['name', 'workflowId', 'queryType'],
  },
  uiSchema: {
    'ui:order': ['name', 'workflowId', 'queryType', 'args', 'resultPath', 'runId', 'connectionKey', 'namespace'],
  },
  defaultProperties: { name: 'Temporal Query' },
  tags: [...TEMPORAL_TAGS, 'query'],
  rendering: circuit('QRY', 4),
};

export const temporalUpdateDesigner: IWorkflowStepDesignerDefinition = {
  id: 'temporal_update',
  name: 'Temporal Update',
  category: 'integration',
  description: 'Execute a Workflow Update — a validated, synchronous request/response',
  icon: 'sync_alt',
  color: TEMPORAL_COLOR,
  inputPorts: [{ name: 'previous', type: 'control_input', dataType: 'any', description: 'Previous step in workflow' }],
  outputPorts: [
    { name: 'next', type: 'control_output', dataType: 'any', description: 'Next step in workflow' },
    { name: 'result', type: 'output', dataType: 'object', description: 'Update result' },
  ],
  propertySchema: {
    type: 'object',
    properties: {
      name: { type: 'string', title: 'Step Name', default: 'Temporal Update' },
      workflowId: { type: 'string', title: 'Workflow Id' },
      runId: { type: 'string', title: 'Run Id', description: 'Optional' },
      updateName: { type: 'string', title: 'Update Name' },
      args: { type: 'array', title: 'Update Arguments', items: { type: 'object' } },
      waitForStage: {
        type: 'string',
        title: 'Wait For',
        enum: ['COMPLETED', 'ACCEPTED'],
        default: 'COMPLETED',
      },
      resultPath: { type: 'string', title: 'Result Path' },
      ...CONNECTION_PROPERTIES,
    },
    required: ['name', 'workflowId', 'updateName'],
  },
  uiSchema: {
    'ui:order': [
      'name',
      'workflowId',
      'updateName',
      'args',
      'waitForStage',
      'resultPath',
      'runId',
      'connectionKey',
      'namespace',
    ],
    waitForStage: {
      'ui:widget': 'SelectWidget',
      'ui:options': {
        selectOptions: [
          { key: 'COMPLETED', value: 'COMPLETED', label: 'Completed — await the result' },
          { key: 'ACCEPTED', value: 'ACCEPTED', label: 'Accepted — return once durable' },
        ],
      },
    },
  },
  defaultProperties: { name: 'Temporal Update', waitForStage: 'COMPLETED' },
  tags: [...TEMPORAL_TAGS, 'update'],
  rendering: circuit('UPD', 4),
};

export const temporalCancelDesigner: IWorkflowStepDesignerDefinition = {
  id: 'temporal_cancel',
  name: 'Temporal Cancel',
  category: 'integration',
  description: 'Request graceful cancellation of a Temporal execution',
  icon: 'cancel',
  color: '#ef6c00',
  inputPorts: [{ name: 'previous', type: 'control_input', dataType: 'any', description: 'Previous step in workflow' }],
  outputPorts: [
    { name: 'next', type: 'control_output', dataType: 'any', description: 'Next step in workflow' },
    { name: 'result', type: 'output', dataType: 'object', description: '{ workflowId, cancelled }' },
  ],
  propertySchema: {
    type: 'object',
    properties: {
      name: { type: 'string', title: 'Step Name', default: 'Temporal Cancel' },
      workflowId: { type: 'string', title: 'Workflow Id' },
      runId: { type: 'string', title: 'Run Id', description: 'Optional' },
      ignoreNotFound: {
        type: 'boolean',
        title: 'Ignore Missing Run',
        default: false,
        description: 'Treat an unknown or already-closed run as success (idempotent compensation)',
      },
      ...CONNECTION_PROPERTIES,
    },
    required: ['name', 'workflowId'],
  },
  uiSchema: { 'ui:order': ['name', 'workflowId', 'runId', 'ignoreNotFound', 'connectionKey', 'namespace'] },
  defaultProperties: { name: 'Temporal Cancel', ignoreNotFound: false },
  tags: [...TEMPORAL_TAGS, 'cancel'],
  rendering: circuit('CAN', 4),
};

export const temporalTerminateDesigner: IWorkflowStepDesignerDefinition = {
  id: 'temporal_terminate',
  name: 'Temporal Terminate',
  category: 'integration',
  description: 'Terminate a Temporal execution immediately (no cleanup — prefer Cancel)',
  icon: 'dangerous',
  color: '#c62828',
  inputPorts: [{ name: 'previous', type: 'control_input', dataType: 'any', description: 'Previous step in workflow' }],
  outputPorts: [
    { name: 'next', type: 'control_output', dataType: 'any', description: 'Next step in workflow' },
    { name: 'result', type: 'output', dataType: 'object', description: '{ workflowId, terminated }' },
  ],
  propertySchema: {
    type: 'object',
    properties: {
      name: { type: 'string', title: 'Step Name', default: 'Temporal Terminate' },
      workflowId: { type: 'string', title: 'Workflow Id' },
      runId: { type: 'string', title: 'Run Id', description: 'Optional' },
      reason: { type: 'string', title: 'Reason', description: 'Recorded in the Temporal history' },
      ignoreNotFound: { type: 'boolean', title: 'Ignore Missing Run', default: false },
      ...CONNECTION_PROPERTIES,
    },
    required: ['name', 'workflowId'],
  },
  uiSchema: {
    'ui:order': ['name', 'workflowId', 'reason', 'runId', 'ignoreNotFound', 'connectionKey', 'namespace'],
  },
  defaultProperties: { name: 'Temporal Terminate', ignoreNotFound: false },
  tags: [...TEMPORAL_TAGS, 'terminate'],
  rendering: circuit('TRM', 4),
};

export const temporalDescribeDesigner: IWorkflowStepDesignerDefinition = {
  id: 'temporal_describe',
  name: 'Temporal Describe',
  category: 'integration',
  description: 'Read a Temporal execution status and metadata without waiting',
  icon: 'info',
  color: TEMPORAL_COLOR,
  inputPorts: [{ name: 'previous', type: 'control_input', dataType: 'any', description: 'Previous step in workflow' }],
  outputPorts: [
    { name: 'next', type: 'control_output', dataType: 'any', description: 'Next step in workflow' },
    {
      name: 'description',
      type: 'output',
      dataType: 'object',
      description: '{ status, workflowType, taskQueue, historyLength, terminal }',
    },
  ],
  propertySchema: {
    type: 'object',
    properties: {
      name: { type: 'string', title: 'Step Name', default: 'Temporal Describe' },
      workflowId: { type: 'string', title: 'Workflow Id' },
      runId: { type: 'string', title: 'Run Id', description: 'Optional' },
      failIfNotFound: {
        type: 'boolean',
        title: 'Fail If Not Found',
        default: false,
        description: 'When off, an unknown run yields status UNKNOWN and found: false',
      },
      ...CONNECTION_PROPERTIES,
    },
    required: ['name', 'workflowId'],
  },
  uiSchema: { 'ui:order': ['name', 'workflowId', 'runId', 'failIfNotFound', 'connectionKey', 'namespace'] },
  defaultProperties: { name: 'Temporal Describe', failIfNotFound: false },
  tags: [...TEMPORAL_TAGS, 'status'],
  rendering: circuit('DSC', 4),
};
