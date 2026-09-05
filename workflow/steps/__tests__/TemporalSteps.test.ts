/**
 * Unit tests for the Temporal workflow steps.
 *
 * These drive the steps directly with a fake TemporalClientService and a fake
 * Reactory context, so they assert the step CONTRACT — award modes, idempotency
 * reporting, template resolution, degradation when the engine cannot suspend, and
 * the failure policy — without needing a Temporal cluster.
 */

import { TemporalWorkflowStep } from '../TemporalWorkflowStep';
import { TemporalAwaitStep } from '../TemporalAwaitStep';
import { TemporalSignalStep } from '../TemporalSignalStep';
import { TemporalQueryStep } from '../TemporalQueryStep';
import { TemporalCancelStep } from '../TemporalCancelStep';
import { TemporalDescribeStep } from '../TemporalDescribeStep';
import { DEFAULT_SETTLED_EVENT, TEMPORAL_EVENT_BRIDGE_SERVICE_ID } from '../../events';
import { TEMPORAL_CLIENT_SERVICE_ID } from '../BaseTemporalStep';

/** Records every call so tests can assert what reached Temporal. */
interface FakeTemporal {
  startWorkflow: jest.Mock;
  describeWorkflow: jest.Mock;
  awaitOutcome: jest.Mock;
  signalWorkflow: jest.Mock;
  queryWorkflow: jest.Mock;
  updateWorkflow: jest.Mock;
  cancelWorkflow: jest.Mock;
  terminateWorkflow: jest.Mock;
}

function fakeTemporal(overrides: Partial<FakeTemporal> = {}): FakeTemporal {
  return {
    // Echo the requested id, as Temporal does — the start result's workflowId is
    // what the step treats as authoritative from then on.
    startWorkflow: jest.fn().mockImplementation(async (input: any) => ({
      workflowId: input.workflowId,
      runId: 'run-1',
      started: true,
      reattached: false,
    })),
    describeWorkflow: jest.fn().mockImplementation(async (workflowId: string) => ({
      workflowId,
      runId: 'run-1',
      status: 'RUNNING',
    })),
    awaitOutcome: jest.fn().mockResolvedValue({
      workflowId: 'batch_1',
      runId: 'run-1',
      status: 'COMPLETED',
      result: { totalSettled: 42 },
    }),
    signalWorkflow: jest.fn().mockResolvedValue(undefined),
    queryWorkflow: jest.fn().mockResolvedValue({ processedCount: 7 }),
    updateWorkflow: jest.fn().mockResolvedValue({ result: 'ok', updateId: 'u1', accepted: true }),
    cancelWorkflow: jest.fn().mockResolvedValue(undefined),
    terminateWorkflow: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  } as FakeTemporal;
}

function fakeBridge() {
  return {
    watch: jest.fn().mockResolvedValue({ watchId: 'w1' }),
    unwatch: jest.fn().mockResolvedValue(undefined),
    publish: jest.fn().mockResolvedValue(undefined),
    settleFromCallback: jest.fn(),
    sweep: jest.fn(),
    start: jest.fn(),
    stop: jest.fn(),
  };
}

/**
 * Build a step execution context.
 * @param supportsSuspend whether the "engine" running this step can suspend
 */
function makeContext(opts: {
  temporal?: FakeTemporal | null;
  bridge?: any;
  supportsSuspend?: boolean;
  eventPublished?: boolean;
  eventData?: any;
  persistenceData?: any;
  inputs?: Record<string, any>;
  variables?: Record<string, any>;
  stepResults?: Record<string, any>;
} = {}): any {
  const services: Record<string, any> = {};
  if (opts.temporal !== null) services[TEMPORAL_CLIENT_SERVICE_ID] = opts.temporal || fakeTemporal();
  if (opts.bridge) services[TEMPORAL_EVENT_BRIDGE_SERVICE_ID] = opts.bridge;

  const logger = { log: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() };

  return {
    inputs: opts.inputs || {},
    workflowInputs: opts.inputs || {},
    variables: opts.variables || {},
    env: {},
    stepResults: opts.stepResults || {},
    logger,
    workflow: {
      id: 'ns.wf@1.0.0',
      instanceId: 'instance-1',
      nameSpace: 'ns',
      name: 'wf',
      version: '1.0.0',
    },
    reactoryContext: {
      getService: (id: string) => {
        if (!services[id]) throw new Error(`service ${id} not registered`);
        return services[id];
      },
      partner: { key: 'reactory' },
      user: { email: 'tester@reactory.net' },
      utils: {},
    },
    control: {
      supportsSuspend: opts.supportsSuspend !== false,
      eventPublished: opts.eventPublished === true,
      eventData: opts.eventData,
      persistenceData: opts.persistenceData,
    },
  };
}

describe('temporal_workflow', () => {
  describe('config validation', () => {
    it('requires workflowType and workflowId', () => {
      const step = new TemporalWorkflowStep('s', {});
      const result = step.validateConfig({});
      expect(result.valid).toBe(false);
      expect(result.errors).toEqual(
        expect.arrayContaining([
          expect.stringContaining('workflowType is required'),
          expect.stringContaining('workflowId is required'),
        ]),
      );
    });

    it('warns — but does not fail — on a static workflowId', () => {
      const step = new TemporalWorkflowStep('s', {});
      const result = step.validateConfig({ workflowType: 'W', workflowId: 'fixed-id' });
      // A static id is legitimate for a singleton workflow, so this must not block.
      expect(result.valid).toBe(true);
      expect(result.warnings?.[0]).toMatch(/deterministic AND unique/i);
    });

    it('rejects an unknown awaitMode and a non-array args', () => {
      const step = new TemporalWorkflowStep('s', {});
      const result = step.validateConfig({
        workflowType: 'W',
        workflowId: '${input.id}',
        awaitMode: 'eventually',
        args: { not: 'an array' },
      });
      expect(result.valid).toBe(false);
      expect(result.errors.join(' ')).toMatch(/awaitMode must be one of/);
      expect(result.errors.join(' ')).toMatch(/args must be an array/);
    });
  });

  describe('awaitMode: none', () => {
    it('starts the workflow with a resolved id and returns without waiting', async () => {
      const temporal = fakeTemporal();
      const step = new TemporalWorkflowStep('payout', {
        workflowType: 'B2BMassPayoutWorkflow',
        workflowId: 'batch_${input.batchId}',
        taskQueue: 'payouts',
        awaitMode: 'none',
        args: [{ batchId: '${input.batchId}', nested: { account: '${variables.accountId}' } }],
      });

      const context = makeContext({
        temporal,
        inputs: { batchId: '2026_001' },
        variables: { accountId: 'acc_1' },
      });
      const result = await step.execute(context);

      expect(result.success).toBe(true);
      expect(temporal.startWorkflow).toHaveBeenCalledTimes(1);
      const [input] = temporal.startWorkflow.mock.calls[0];
      expect(input.workflowId).toBe('batch_2026_001');
      expect(input.taskQueue).toBe('payouts');
      // Deep template resolution reaches nested argument values.
      expect(input.args[0]).toEqual({ batchId: '2026_001', nested: { account: 'acc_1' } });
      // Idempotent by default.
      expect(input.workflowIdConflictPolicy).toBe('USE_EXISTING');
      expect(result.outputs.status).toBe('RUNNING');
      expect(result.outputs.awaited).toBe(false);
      expect(temporal.awaitOutcome).not.toHaveBeenCalled();
    });

    it('passes non-scalar arguments to Temporal with their type intact', async () => {
      const temporal = fakeTemporal();
      const rows = [
        { rowId: '1', amount: 3200, beneficiaryName: 'Sarah Jenkins' },
        { rowId: '2', amount: 2850, beneficiaryName: 'Alex Thorne' },
      ];
      const step = new TemporalWorkflowStep('payout', {
        workflowType: 'W',
        workflowId: 'batch_${input.batchId}',
        awaitMode: 'none',
        args: [
          {
            batchId: '${input.batchId}',
            rows: '${input.rows}',
            limits: '${variables.limits}',
            label: 'batch ${input.batchId}',
          },
        ],
      });

      await step.execute(
        makeContext({
          temporal,
          inputs: { batchId: '2026_001', rows },
          variables: { limits: { max: 10 } },
        }),
      );

      const [arg] = temporal.startWorkflow.mock.calls[0][0].args;
      // A whole-value reference keeps its type — the Temporal workflow does
      // input.rows.reduce(...), which would throw on a JSON string.
      expect(arg.rows).toEqual(rows);
      expect(Array.isArray(arg.rows)).toBe(true);
      expect(arg.limits).toEqual({ max: 10 });
      // Mixed content still interpolates to a string.
      expect(arg.label).toBe('batch 2026_001');
      expect(arg.batchId).toBe('2026_001');
    });

    it('leaves an unresolved argument reference as its literal token', async () => {
      const temporal = fakeTemporal();
      const step = new TemporalWorkflowStep('payout', {
        workflowType: 'W',
        workflowId: 'batch_1',
        awaitMode: 'none',
        args: [{ rows: '${input.rows}' }],
      });
      await step.execute(makeContext({ temporal, inputs: {} }));
      // Unresolved tokens pass through so the optional-value handling elsewhere can
      // still tell "not supplied" from "supplied as empty".
      expect(temporal.startWorkflow.mock.calls[0][0].args[0].rows).toBe('${input.rows}');
    });

    it('reports reattached when Temporal adopted an existing run', async () => {
      const temporal = fakeTemporal({
        startWorkflow: jest.fn().mockImplementation(async (input: any) => ({
          workflowId: input.workflowId,
          runId: 'run-0',
          started: false,
          reattached: true,
        })),
      });
      const step = new TemporalWorkflowStep('payout', {
        workflowType: 'W',
        workflowId: 'batch_${input.batchId}',
        awaitMode: 'none',
      });
      const result = await step.execute(makeContext({ temporal, inputs: { batchId: '1' } }));
      expect(result.outputs.reattached).toBe(true);
      expect(result.outputs.started).toBe(false);
    });
  });

  describe('awaitMode: event', () => {
    const config = {
      workflowType: 'W',
      workflowId: 'batch_${input.batchId}',
      awaitMode: 'event',
      resultPath: 'totalSettled',
    };

    it('registers a watch and returns a waitForEvent directive', async () => {
      const temporal = fakeTemporal();
      const bridge = fakeBridge();
      const step = new TemporalWorkflowStep('payout', config);

      const result = await step.execute(
        makeContext({ temporal, bridge, inputs: { batchId: '2026_001' } }),
      );

      expect(bridge.watch).toHaveBeenCalledTimes(1);
      const watchInput = bridge.watch.mock.calls[0][0];
      expect(watchInput).toMatchObject({
        workflowId: 'batch_2026_001',
        eventName: DEFAULT_SETTLED_EVENT,
        eventKey: 'batch_2026_001',
        instanceId: 'instance-1',
        stepId: 'payout',
        clientKey: 'reactory',
      });

      expect(result.control?.waitForEvent).toEqual({
        eventName: DEFAULT_SETTLED_EVENT,
        eventKey: 'batch_2026_001',
      });
      // Outputs carry the started run so the resumed step (and later steps) can see it.
      expect(result.outputs).toMatchObject({ workflowId: 'batch_2026_001', runId: 'run-1' });
    });

    it('does not suspend when the run already finished (closes the fast-completion race)', async () => {
      const temporal = fakeTemporal({
        describeWorkflow: jest.fn().mockImplementation(async (workflowId: string) => ({
          workflowId,
          runId: 'run-1',
          status: 'COMPLETED',
        })),
      });
      const bridge = fakeBridge();
      const step = new TemporalWorkflowStep('payout', config);

      const result = await step.execute(
        makeContext({ temporal, bridge, inputs: { batchId: '2026_001' } }),
      );

      expect(result.control).toBeUndefined();
      expect(result.success).toBe(true);
      expect(result.outputs.status).toBe('COMPLETED');
      // resultPath extracted the subtree.
      expect(result.outputs.result).toBe(42);
      // The now-redundant watch is released.
      expect(bridge.unwatch).toHaveBeenCalledWith('batch_2026_001', 'batch_2026_001');
    });

    it('resumes from the bridge event payload without touching Temporal', async () => {
      const temporal = fakeTemporal();
      const step = new TemporalWorkflowStep('payout', config);

      const result = await step.execute(
        makeContext({
          temporal,
          inputs: { batchId: '2026_001' },
          eventPublished: true,
          eventData: {
            workflowId: 'batch_2026_001',
            runId: 'run-1',
            status: 'COMPLETED',
            result: { totalSettled: 42 },
          },
        }),
      );

      expect(result.success).toBe(true);
      expect(result.outputs.status).toBe('COMPLETED');
      expect(result.outputs.result).toBe(42);
      expect(result.outputs.awaited).toBe(true);
      // A resumed step must never re-start or re-poll the execution.
      expect(temporal.startWorkflow).not.toHaveBeenCalled();
      expect(temporal.awaitOutcome).not.toHaveBeenCalled();
    });

    it('fails the step when a Temporal failure arrives and failOnWorkflowFailure holds', async () => {
      const step = new TemporalWorkflowStep('payout', config);
      const result = await step.execute(
        makeContext({
          inputs: { batchId: '1' },
          eventPublished: true,
          eventData: {
            workflowId: 'batch_1',
            status: 'FAILED',
            failure: { message: 'screening alert' },
          },
        }),
      );
      expect(result.success).toBe(false);
      expect(result.error).toMatch(/screening alert/);
      expect(result.outputs.status).toBe('FAILED');
    });

    it('surfaces a Temporal failure as data when failOnWorkflowFailure is false', async () => {
      const step = new TemporalWorkflowStep('payout', { ...config, failOnWorkflowFailure: false });
      const result = await step.execute(
        makeContext({
          inputs: { batchId: '1' },
          eventPublished: true,
          eventData: { workflowId: 'batch_1', status: 'FAILED', failure: { message: 'nope' } },
        }),
      );
      // Lets the YAML author branch on status instead of failing the run.
      expect(result.success).toBe(true);
      expect(result.outputs.status).toBe('FAILED');
      expect(result.outputs.failure).toEqual({ message: 'nope' });
    });

    it('always fails on an AWAIT_TIMEOUT, even when failOnWorkflowFailure is false', async () => {
      const step = new TemporalWorkflowStep('payout', { ...config, failOnWorkflowFailure: false });
      const result = await step.execute(
        makeContext({
          inputs: { batchId: '1' },
          eventPublished: true,
          eventData: { workflowId: 'batch_1', status: 'AWAIT_TIMEOUT', waitedMs: 1000 },
        }),
      );
      // The outcome is unknown — reporting success would be a lie.
      expect(result.success).toBe(false);
      expect(result.outputs.status).toBe('AWAIT_TIMEOUT');
    });

    it('degrades to sleep_poll when no bridge is registered', async () => {
      const temporal = fakeTemporal();
      const step = new TemporalWorkflowStep('payout', config);
      const context = makeContext({ temporal, inputs: { batchId: '1' } }); // no bridge

      const result = await step.execute(context);

      // Nothing would ever publish the event, so suspending on it would hang.
      expect(result.control?.waitForEvent).toBeUndefined();
      expect(result.control?.sleep).toBeDefined();
      expect(context.logger.warn).toHaveBeenCalledWith(expect.stringMatching(/falling back to sleep_poll/));
    });
  });

  describe('awaitMode: sleep_poll', () => {
    it('sleeps with poll state and reports RUNNING while the run is in flight', async () => {
      const temporal = fakeTemporal();
      const step = new TemporalWorkflowStep('payout', {
        workflowType: 'W',
        workflowId: 'batch_${input.batchId}',
        awaitMode: 'sleep_poll',
        pollInterval: 5000,
      });

      const result = await step.execute(makeContext({ temporal, inputs: { batchId: '1' } }));

      expect(result.control?.sleep).toEqual({ durationMs: 5000 });
      expect(result.control?.persist).toMatchObject({
        temporalWorkflowId: 'batch_1',
        temporalRunId: 'run-1',
        attempts: 0,
      });
      expect(result.outputs.status).toBe('RUNNING');
    });

    it('settles on a later tick once the run is terminal', async () => {
      const temporal = fakeTemporal({
        describeWorkflow: jest.fn().mockResolvedValue({ status: 'COMPLETED' }),
      });
      const step = new TemporalWorkflowStep('payout', {
        workflowType: 'W',
        workflowId: 'batch_1',
        awaitMode: 'sleep_poll',
        resultPath: 'totalSettled',
      });

      const result = await step.execute(
        makeContext({
          temporal,
          persistenceData: {
            temporalWorkflowId: 'batch_1',
            temporalRunId: 'run-1',
            startedAt: Date.now() - 1000,
            deadline: Date.now() + 60000,
            started: true,
            reattached: false,
            attempts: 2,
          },
        }),
      );

      expect(result.control).toBeUndefined();
      expect(result.success).toBe(true);
      expect(result.outputs.result).toBe(42);
      // A resumed poll tick must not start anything.
      expect(temporal.startWorkflow).not.toHaveBeenCalled();
    });

    it('fails once the await deadline has passed', async () => {
      const temporal = fakeTemporal();
      const step = new TemporalWorkflowStep('payout', {
        workflowType: 'W',
        workflowId: 'batch_1',
        awaitMode: 'sleep_poll',
      });

      const result = await step.execute(
        makeContext({
          temporal,
          persistenceData: {
            temporalWorkflowId: 'batch_1',
            startedAt: Date.now() - 120000,
            deadline: Date.now() - 1000,
            started: true,
            reattached: false,
            attempts: 11,
          },
        }),
      );

      expect(result.success).toBe(false);
      expect(result.outputs.status).toBe('AWAIT_TIMEOUT');
      expect(result.error).toMatch(/Timed out awaiting/);
    });
  });

  describe('non-suspending executor', () => {
    it('falls back to block_poll and awaits in-step', async () => {
      const temporal = fakeTemporal();
      const step = new TemporalWorkflowStep('payout', {
        workflowType: 'W',
        workflowId: 'batch_1',
        awaitMode: 'event',
        resultPath: 'totalSettled',
      });

      const context = makeContext({ temporal, bridge: fakeBridge(), supportsSuspend: false });
      const result = await step.execute(context);

      expect(result.control).toBeUndefined();
      expect(temporal.awaitOutcome).toHaveBeenCalledTimes(1);
      expect(result.outputs.result).toBe(42);
      expect(context.logger.warn).toHaveBeenCalledWith(expect.stringMatching(/block_poll/));
    });
  });

  describe('error handling', () => {
    it('fails cleanly when the client service is unavailable', async () => {
      const step = new TemporalWorkflowStep('payout', { workflowType: 'W', workflowId: 'x' });
      const result = await step.execute(makeContext({ temporal: null }));
      expect(result.success).toBe(false);
      expect(result.error).toMatch(/is not available/);
    });

    it('fails cleanly when the start call throws', async () => {
      const temporal = fakeTemporal({
        startWorkflow: jest.fn().mockRejectedValue(new Error('cluster unreachable')),
      });
      const step = new TemporalWorkflowStep('payout', { workflowType: 'W', workflowId: 'batch_1' });
      const result = await step.execute(makeContext({ temporal }));
      expect(result.success).toBe(false);
      expect(result.error).toMatch(/cluster unreachable/);
    });
  });
});

describe('temporal_await', () => {
  it('rejects awaitMode "none" — awaiting is the point', () => {
    const step = new TemporalAwaitStep('await', {});
    const result = step.validateConfig({ workflowId: 'x', awaitMode: 'none' });
    expect(result.valid).toBe(false);
    expect(result.errors.join(' ')).toMatch(/temporal_await always waits/);
  });

  it('suspends on the settled topic for a run started elsewhere', async () => {
    const temporal = fakeTemporal();
    const bridge = fakeBridge();
    const step = new TemporalAwaitStep('await', {
      workflowId: '${steps.start.outputs.workflowId}',
    });

    const result = await step.execute(
      makeContext({
        temporal,
        bridge,
        stepResults: { start: { success: true, outputs: { workflowId: 'batch_9' }, metadata: {} } },
      }),
    );

    // The target came from an earlier step's outputs, and nothing was started here.
    expect(temporal.startWorkflow).not.toHaveBeenCalled();
    expect(result.control?.waitForEvent?.eventKey).toBe('batch_9');
    expect(result.outputs.started).toBe(false);
    expect(result.outputs.reattached).toBe(true);
  });

  it('returns the outcome immediately when the run is already terminal', async () => {
    const temporal = fakeTemporal({
      describeWorkflow: jest.fn().mockResolvedValue({ status: 'COMPLETED', runId: 'run-1' }),
    });
    const step = new TemporalAwaitStep('await', { workflowId: 'batch_1', resultPath: 'totalSettled' });
    const result = await step.execute(makeContext({ temporal, bridge: fakeBridge() }));
    expect(result.control).toBeUndefined();
    expect(result.outputs.result).toBe(42);
  });

  it('fails when the target run cannot be described', async () => {
    const temporal = fakeTemporal({
      describeWorkflow: jest.fn().mockRejectedValue(new Error('not found')),
    });
    const step = new TemporalAwaitStep('await', { workflowId: 'ghost' });
    const result = await step.execute(makeContext({ temporal }));
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/could not be described/);
  });
});

describe('temporal_signal', () => {
  it('signals a running execution with resolved args', async () => {
    const temporal = fakeTemporal();
    const step = new TemporalSignalStep('approve', {
      workflowId: 'batch_${input.batchId}',
      signalName: 'approveBatch',
      args: ['${variables.approverId}'],
    });

    const result = await step.execute(
      makeContext({ temporal, inputs: { batchId: '1' }, variables: { approverId: 'user_7' } }),
    );

    expect(result.success).toBe(true);
    expect(temporal.signalWorkflow).toHaveBeenCalledWith(
      expect.objectContaining({
        workflowId: 'batch_1',
        signalName: 'approveBatch',
        args: ['user_7'],
      }),
      undefined,
    );
  });

  it('treats an unresolved optional runId as absent', async () => {
    const temporal = fakeTemporal();
    const step = new TemporalSignalStep('approve', {
      workflowId: 'batch_1',
      runId: '${input.runId}', // never supplied
      signalName: 'go',
    });
    await step.execute(makeContext({ temporal }));
    // The literal "${input.runId}" must never be sent to Temporal as a run id.
    expect(temporal.signalWorkflow.mock.calls[0][0].runId).toBeUndefined();
  });

  it('uses signalWithStart when startIfNotRunning is configured', async () => {
    const temporal = fakeTemporal();
    const step = new TemporalSignalStep('approve', {
      workflowId: 'batch_1',
      signalName: 'approveBatch',
      startIfNotRunning: { workflowType: 'W', taskQueue: 'q' },
    });

    const result = await step.execute(makeContext({ temporal }));

    expect(temporal.signalWorkflow).not.toHaveBeenCalled();
    expect(temporal.startWorkflow).toHaveBeenCalledWith(
      expect.objectContaining({ signalOnStart: { name: 'approveBatch', args: [] } }),
      undefined,
    );
    expect(result.outputs.started).toBe(true);
  });

  it('fails when the signal cannot be delivered', async () => {
    const temporal = fakeTemporal({
      signalWorkflow: jest.fn().mockRejectedValue(new Error('workflow not found')),
    });
    const step = new TemporalSignalStep('approve', { workflowId: 'ghost', signalName: 'go' });
    const result = await step.execute(makeContext({ temporal }));
    expect(result.success).toBe(false);
    expect(result.outputs.signalled).toBe(false);
  });
});

describe('temporal_query', () => {
  it('returns the query result and applies resultPath', async () => {
    const temporal = fakeTemporal();
    const step = new TemporalQueryStep('progress', {
      workflowId: 'batch_1',
      queryType: 'getBatchProgress',
      resultPath: 'processedCount',
    });
    const result = await step.execute(makeContext({ temporal }));
    expect(result.success).toBe(true);
    expect(result.outputs.result).toBe(7);
  });

  it('drops an oversized result rather than persisting it into the instance', async () => {
    const big = 'x'.repeat(2048);
    const temporal = fakeTemporal({ queryWorkflow: jest.fn().mockResolvedValue({ blob: big }) });
    const step = new TemporalQueryStep('progress', {
      workflowId: 'batch_1',
      queryType: 'q',
      maxResultBytes: 512,
    });
    const result = await step.execute(makeContext({ temporal }));
    expect(result.success).toBe(true);
    expect(result.outputs.truncated).toBe(true);
    expect(result.outputs.result).toBeUndefined();
    expect(result.outputs.bytes).toBeGreaterThan(512);
  });
});

describe('temporal_cancel', () => {
  it('cancels a run', async () => {
    const temporal = fakeTemporal();
    const step = new TemporalCancelStep('stop', { workflowId: 'batch_1' });
    const result = await step.execute(makeContext({ temporal }));
    expect(result.success).toBe(true);
    expect(temporal.cancelWorkflow).toHaveBeenCalledWith('batch_1', undefined, undefined);
  });

  it('is idempotent for a missing run when ignoreNotFound is set', async () => {
    const temporal = fakeTemporal({
      cancelWorkflow: jest.fn().mockRejectedValue(new Error('workflow execution already completed')),
    });
    const step = new TemporalCancelStep('stop', { workflowId: 'batch_1', ignoreNotFound: true });
    const result = await step.execute(makeContext({ temporal }));
    // Compensation paths re-run; a already-closed run must not fail them.
    expect(result.success).toBe(true);
    expect(result.outputs.notFound).toBe(true);
  });

  it('fails on a missing run by default', async () => {
    const temporal = fakeTemporal({
      cancelWorkflow: jest.fn().mockRejectedValue(new Error('NOT_FOUND')),
    });
    const step = new TemporalCancelStep('stop', { workflowId: 'batch_1' });
    const result = await step.execute(makeContext({ temporal }));
    expect(result.success).toBe(false);
  });
});

describe('temporal_describe', () => {
  it('reports terminal state for branching', async () => {
    const temporal = fakeTemporal({
      describeWorkflow: jest.fn().mockResolvedValue({
        workflowId: 'batch_1',
        runId: 'run-1',
        status: 'COMPLETED',
        historyLength: 12,
      }),
    });
    const step = new TemporalDescribeStep('check', { workflowId: 'batch_1' });
    const result = await step.execute(makeContext({ temporal }));
    expect(result.outputs).toMatchObject({ status: 'COMPLETED', terminal: true, found: true });
  });

  it('reports UNKNOWN instead of failing when the run cannot be found', async () => {
    const temporal = fakeTemporal({
      describeWorkflow: jest.fn().mockRejectedValue(new Error('not found')),
    });
    const step = new TemporalDescribeStep('check', { workflowId: 'ghost' });
    const result = await step.execute(makeContext({ temporal }));
    expect(result.success).toBe(true);
    expect(result.outputs).toMatchObject({ status: 'UNKNOWN', found: false, terminal: false });
  });
});
