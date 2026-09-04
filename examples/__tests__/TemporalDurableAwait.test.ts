/**
 * End-to-end integration: a Temporal step suspending and resuming on the REAL
 * durable engine.
 *
 * This is the test that proves the whole integration hangs together — YAML →
 * YamlFlowBuilder → workflow-es → YamlStepBody → control directive → engine
 * suspension → bridge publish → resumption of the same step with the outcome.
 * Everything runs in-process against an in-memory persistence provider; only
 * Temporal itself and the bridge's Mongo watch store are faked.
 *
 * It also covers the two failure modes the design is built around:
 *   • a Temporal run that settles BEFORE the step suspends (the engine drops an
 *     event with no subscription, so the step must resolve inline instead);
 *   • a run that settles while nothing is listening, delivered by a later sweep.
 */

import {
  configureWorkflow,
  MemoryPersistenceProvider,
  WorkflowInstance,
} from '@reactorynet/workflow-es';
import {
  buildYamlWorkflowClass,
  engineWorkflowId,
} from '@reactory/server-modules/reactory-core/workflow/YamlFlow/YamlFlowBuilder';
import { YamlStepRegistry } from '@reactory/server-modules/reactory-core/workflow/YamlFlow/steps/registry/YamlStepRegistry';
import { configureYamlFlowRuntime } from '@reactory/server-modules/reactory-core/workflow/YamlFlow/execution/YamlFlowRuntime';
import { workflowSteps } from '../../workflow/steps';
import { DEFAULT_SETTLED_EVENT, TEMPORAL_EVENT_BRIDGE_SERVICE_ID } from '../../workflow/events';

jest.setTimeout(30000);

const COMPLETE = 2;
const DEAD_LETTERED = 4;
const TERMINATED = 3;

const TEMPORAL_CLIENT_SERVICE_ID = 'reactory.TemporalClientService@1.0.0';

function freshData(extra: Record<string, any> = {}) {
  return { inputs: {}, variables: {}, stepResults: {}, env: {}, outputs: {}, ...extra };
}

async function waitFor(
  predicate: () => Promise<boolean>,
  timeoutMs = 20000,
  label = 'condition',
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 40));
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for ${label}`);
}

async function runToTerminal(
  persistence: MemoryPersistenceProvider,
  id: string,
): Promise<WorkflowInstance> {
  let instance: WorkflowInstance | undefined;
  await waitFor(async () => {
    instance = await persistence.getWorkflowInstance(id);
    return (
      !!instance &&
      [COMPLETE, TERMINATED, DEAD_LETTERED].includes(instance.status)
    );
  }, 20000, `workflow ${id} to finish`);
  return instance!;
}

/** The YAML under test: start a Temporal run and durably await its outcome. */
const DEFINITION = {
  nameSpace: 'test-temporal',
  name: 'durable-await',
  version: '1.0.0',
  steps: [
    {
      id: 'payout',
      type: 'temporal_workflow',
      config: {
        workflowType: 'B2BMassPayoutWorkflow',
        workflowId: 'batch_${input.batchId}',
        taskQueue: 'payouts',
        awaitMode: 'event',
        resultPath: 'totalSettled',
        args: [{ batchId: '${input.batchId}' }],
      },
    },
    {
      id: 'record',
      type: 'set_variable',
      dependsOn: 'payout',
      config: {
        action: 'set',
        key: 'settlementStatus',
        value: '${steps.payout.outputs.status}',
      },
    },
  ],
};

/**
 * Same flow, but with the retry budget bounded so a FAILED Temporal outcome reaches
 * a terminal engine state promptly (the engine's default error behaviour is Retry on
 * a 60s interval, which would outlast the test).
 */
const FAIL_FAST_DEFINITION = {
  ...DEFINITION,
  name: 'durable-await-failfast',
  steps: [
    { ...DEFINITION.steps[0], retryPolicy: { maxAttempts: 0, initialDelay: 1 } },
    DEFINITION.steps[1],
  ],
};

describe('Temporal durable await (engine integration)', () => {
  let persistence: MemoryPersistenceProvider;
  let host: any;
  /** Runs whose outcome is available; used by the fake Temporal service. */
  let terminalRuns: Map<string, any>;
  /** Watches the fake bridge was asked to register. */
  let watches: Array<Record<string, any>>;

  /** Fake Temporal: a run is RUNNING until its outcome is put in terminalRuns. */
  function fakeTemporalService() {
    return {
      startWorkflow: jest.fn(async (input: any) => ({
        workflowId: input.workflowId,
        runId: `run-${input.workflowId}`,
        started: true,
        reattached: false,
      })),
      describeWorkflow: jest.fn(async (workflowId: string) => ({
        workflowId,
        runId: `run-${workflowId}`,
        status: terminalRuns.has(workflowId) ? terminalRuns.get(workflowId).status : 'RUNNING',
      })),
      awaitOutcome: jest.fn(async (workflowId: string) => {
        await waitFor(async () => terminalRuns.has(workflowId), 20000, `run ${workflowId} to settle`);
        return { workflowId, runId: `run-${workflowId}`, ...terminalRuns.get(workflowId) };
      }),
    };
  }

  /**
   * Fake bridge with the real bridge's ORDERING contract: record the watch, then
   * publish into the engine once the run settles. Publication goes through
   * host.publishEvent, the same primitive WorkflowRunner.publishEvent uses.
   */
  function fakeBridge(temporal: any) {
    return {
      watch: jest.fn(async (input: any) => {
        watches.push(input);
        void (async () => {
          const outcome = await temporal.awaitOutcome(input.workflowId);
          await host.publishEvent(
            input.eventName,
            input.eventKey,
            { workflowId: input.workflowId, runId: outcome.runId, ...outcome },
            new Date(),
          );
        })();
        return { watchId: `watch-${input.workflowId}` };
      }),
      unwatch: jest.fn(async () => undefined),
      publish: jest.fn(async () => undefined),
      settleFromCallback: jest.fn(),
      sweep: jest.fn(),
      start: jest.fn(),
      stop: jest.fn(),
    };
  }

  function configureRuntime(services: Record<string, any>): void {
    const registry = new YamlStepRegistry();
    for (const provider of workflowSteps) {
      registry.registerStep(
        provider.stepType,
        provider.constructor as any,
        provider.options || {},
        undefined,
        'module',
      );
    }
    // YamlStepBody rehydrates a context per step run; with no identity in the
    // workflow data it falls back to this system context.
    const systemContext: any = {
      getService: (id: string) => {
        if (!services[id]) throw new Error(`service ${id} not registered`);
        return services[id];
      },
      partner: { key: 'acme' },
      user: { email: 'tester@reactory.net' },
      utils: {},
      log: jest.fn(),
    };
    configureYamlFlowRuntime({ registry, systemContext });
  }

  async function startHost(definition: any = DEFINITION, pollIntervalMs?: number): Promise<string> {
    const config = configureWorkflow(pollIntervalMs ? { pollIntervalMs } : undefined);
    config.usePersistence(persistence);
    config.allowSingleNodeProviders(true);
    host = config.getHost();
    host.registerWorkflow(buildYamlWorkflowClass(definition));
    await host.start();
    return host.startWorkflow(
      engineWorkflowId(definition),
      definition.version,
      freshData({ inputs: { batchId: '2026_001' } }),
    );
  }

  beforeEach(() => {
    persistence = new MemoryPersistenceProvider();
    terminalRuns = new Map();
    watches = [];
  });

  afterEach(async () => {
    if (host) {
      await host.stop();
      host = null;
    }
  });

  it('suspends on the settled topic, then resumes with the outcome when Temporal finishes', async () => {
    const temporal = fakeTemporalService();
    const bridge = fakeBridge(temporal);
    configureRuntime({
      [TEMPORAL_CLIENT_SERVICE_ID]: temporal,
      [TEMPORAL_EVENT_BRIDGE_SERVICE_ID]: bridge,
    });

    const id = await startHost();

    // The step starts the Temporal run, registers a watch and parks.
    await waitFor(async () => watches.length === 1, 20000, 'the watch to be registered');
    expect(watches[0]).toMatchObject({
      workflowId: 'batch_2026_001',
      eventName: DEFAULT_SETTLED_EVENT,
      eventKey: 'batch_2026_001',
      stepId: 'payout',
    });

    await waitFor(async () => {
      const instance = await persistence.getWorkflowInstance(id);
      return !!(instance?.executionPointers || []).some(
        (pointer: any) => pointer.eventName === DEFAULT_SETTLED_EVENT && !pointer.eventPublished,
      );
    }, 20000, 'the instance to park on the settled event');

    // Nothing downstream has run while suspended — the flow really is paused.
    const parked = await persistence.getWorkflowInstance(id);
    expect(parked.data.stepResults.record).toBeUndefined();
    expect(parked.data.stepResults.payout.outputs).toMatchObject({
      workflowId: 'batch_2026_001',
      status: 'RUNNING',
    });

    // Temporal settles: the fake bridge publishes the outcome.
    terminalRuns.set('batch_2026_001', { status: 'COMPLETED', result: { totalSettled: 42 } });

    const instance = await runToTerminal(persistence, id);
    expect(instance.status).toBe(COMPLETE);
    // The resumed step surfaced the outcome, and the downstream step consumed it.
    expect(instance.data.stepResults.payout.outputs).toMatchObject({
      status: 'COMPLETED',
      result: 42,
      awaited: true,
    });
    expect(instance.data.variables.settlementStatus).toBe('COMPLETED');
  });

  it('resolves inline without suspending when the run settles before the step parks', async () => {
    const temporal = fakeTemporalService();
    const bridge = fakeBridge(temporal);
    // Already terminal when the step first runs — the classic lost-wakeup window,
    // since an event published now would have no subscription to match and would be
    // discarded by the engine.
    terminalRuns.set('batch_2026_001', { status: 'COMPLETED', result: { totalSettled: 7 } });

    configureRuntime({
      [TEMPORAL_CLIENT_SERVICE_ID]: temporal,
      [TEMPORAL_EVENT_BRIDGE_SERVICE_ID]: bridge,
    });

    const id = await startHost();
    const instance = await runToTerminal(persistence, id);

    expect(instance.status).toBe(COMPLETE);
    expect(instance.data.stepResults.payout.outputs).toMatchObject({ status: 'COMPLETED', result: 7 });
    // The step checked before parking and released the redundant watch.
    expect(bridge.unwatch).toHaveBeenCalled();
    // It never actually waited on an event.
    const parkedPointer = (instance.executionPointers || []).find((p: any) => p.eventName);
    expect(parkedPointer?.eventPublished ?? true).toBe(true);
  });

  it('delivers a late outcome published after the step has parked', async () => {
    const temporal = fakeTemporalService();
    // A bridge that records the watch but does NOT publish — simulating the process
    // that owned the listener dying. Delivery then depends on a later sweep.
    const bridge = {
      watch: jest.fn(async (input: any) => {
        watches.push(input);
        return { watchId: 'w1' };
      }),
      unwatch: jest.fn(async () => undefined),
      publish: jest.fn(async () => undefined),
      settleFromCallback: jest.fn(),
      sweep: jest.fn(),
      start: jest.fn(),
      stop: jest.fn(),
    };

    configureRuntime({
      [TEMPORAL_CLIENT_SERVICE_ID]: temporal,
      [TEMPORAL_EVENT_BRIDGE_SERVICE_ID]: bridge,
    });

    const id = await startHost(FAIL_FAST_DEFINITION);
    await waitFor(async () => {
      const instance = await persistence.getWorkflowInstance(id);
      return !!(instance?.executionPointers || []).some(
        (pointer: any) => pointer.eventName === DEFAULT_SETTLED_EVENT && !pointer.eventPublished,
      );
    }, 20000, 'the instance to park');

    // The sweep's republication, once the run has settled.
    terminalRuns.set('batch_2026_001', { status: 'FAILED', failure: { message: 'screening alert' } });
    await host.publishEvent(
      DEFAULT_SETTLED_EVENT,
      'batch_2026_001',
      { workflowId: 'batch_2026_001', status: 'FAILED', failure: { message: 'screening alert' } },
      new Date(),
    );

    const instance = await runToTerminal(persistence, id);
    // A Temporal failure fails the step by default, so the run does not complete.
    expect([TERMINATED, DEAD_LETTERED]).toContain(instance.status);
    expect(instance.data.__errors?.[0]?.message).toMatch(/screening alert/);
    expect(instance.data.stepResults.payout.outputs.status).toBe('FAILED');
  });
});
