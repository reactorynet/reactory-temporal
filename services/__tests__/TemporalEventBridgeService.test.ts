/**
 * Tests for the bridge's delivery guarantee.
 *
 * The engine DISCARDS an event that mockMatches no subscription (an unmatched event is
 * marked processed and dropped, with no replay), so "publish once and hope" loses
 * work whenever the publish beats the awaiting step into suspension. These tests pin
 * the compensating behaviour:
 *
 *   • the outcome is persisted BEFORE the first publish attempt;
 *   • a watch stays unacked while the waiting pointer has not consumed the event,
 *     and is re-published on the next sweep;
 *   • the watch is acked once the pointer shows the event was delivered;
 *   • publication is retried to a bounded limit, then abandoned loudly;
 *   • events are published under the WAITING INSTANCE's tenant, not the sweeper's;
 *   • a run that outlives its await deadline yields an AWAIT_TIMEOUT outcome.
 *
 * The mongoose model is stubbed with an in-memory double so the suite needs no
 * database.
 */

import { TemporalEventBridgeService } from '../TemporalEventBridgeService';
import { DEFAULT_SETTLED_EVENT, TEMPORAL_EVENTS } from '../../workflow/events';

// ── in-memory stand-in for the TemporalWorkflowWatch mongoose model ─────────────

interface FakeWatch extends Record<string, any> {
  _id: string;
  save: () => Promise<FakeWatch>;
}

const mockStore: FakeWatch[] = [];
let mockIdSeq = 0;

function mockMakeWatch(fields: Record<string, any>): FakeWatch {
  const watch: FakeWatch = {
    _id: `watch-${++mockIdSeq}`,
    status: 'pending',
    attempts: 0,
    createdAt: new Date(),
    ...fields,
    save: async () => watch,
  };
  return watch;
}

function mockMatches(watch: FakeWatch, query: Record<string, any>): boolean {
  return Object.entries(query).every(([key, value]) => {
    if (value === undefined) return true;
    if (value && typeof value === 'object' && '$lte' in value) {
      return watch[key] && watch[key].getTime() <= (value as any).$lte.getTime();
    }
    return watch[key] === value;
  });
}

/** Chainable query stub covering the find().sort().limit().exec() shape. */
function mockQueryStub(results: FakeWatch[]) {
  const chain: any = {
    sort: () => chain,
    limit: () => chain,
    exec: async () => results,
  };
  return chain;
}

jest.mock('../../models/TemporalWorkflowWatch', () => ({
  __esModule: true,
  default: {
    findOneAndUpdate: jest.fn(async (query: any, update: any) => {
      const existing = mockStore.find((watch) => mockMatches(watch, query));
      if (existing) {
        Object.assign(existing, update.$set || {});
        return existing;
      }
      const created = mockMakeWatch({ ...query, ...(update.$set || {}), ...(update.$setOnInsert || {}) });
      mockStore.push(created);
      return created;
    }),
    find: jest.fn((query: any) => mockQueryStub(mockStore.filter((watch) => mockMatches(watch, query)))),
    findOne: jest.fn((query: any) => mockQueryStub(mockStore.filter((watch) => mockMatches(watch, query)))),
    findById: jest.fn((id: string) => ({
      exec: async () => mockStore.find((watch) => watch._id === id) || null,
    })),
  },
}));

// findOne must resolve to a single document, not an array.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const WatchModel = require('../../models/TemporalWorkflowWatch').default;
WatchModel.findOne = jest.fn((query: any) => {
  const results = mockStore.filter((watch) => mockMatches(watch, query));
  const chain: any = {
    sort: () => chain,
    limit: () => chain,
    exec: async () => results[0] || null,
  };
  return chain;
});

/** The sweep lock — always won in these single-process tests. */
jest.mock('@reactory/server-modules/reactory-core/workflow/Scheduler/SchedulerLock.model', () => ({
  SchedulerLockModel: { create: jest.fn(async () => ({})) },
}));

// ── fakes ───────────────────────────────────────────────────────────────────────

function fakeTemporalService(overrides: Record<string, any> = {}) {
  return {
    describeWorkflow: jest.fn(async (workflowId: string) => ({ workflowId, status: 'RUNNING' })),
    awaitOutcome: jest.fn(async (workflowId: string) => ({
      workflowId,
      runId: 'run-1',
      status: 'COMPLETED',
      result: { totalSettled: 42 },
    })),
    ...overrides,
  };
}

/**
 * @param waitingPointers execution pointers reported for the target instance —
 *   an entry with `eventName` and `eventPublished: false` means "still waiting".
 */
function fakeWorkflowService(opts: {
  waitingPointers?: any[];
  tenantId?: string | null;
  instanceMissing?: boolean;
} = {}) {
  return {
    publishWorkflowEvent: jest.fn(async () => ({ success: true })),
    getWorkflowHistoryById: jest.fn(async () =>
      opts.instanceMissing
        ? null
        : {
            id: 'instance-1',
            tenantId: opts.tenantId === undefined ? 'acme' : opts.tenantId,
            executionPointers: opts.waitingPointers || [],
          },
    ),
  };
}

function makeService(temporal: any, workflow: any) {
  const context: any = {
    getService: (id: string) => {
      if (id === 'reactory.TemporalClientService@1.0.0') return temporal;
      if (id === 'core.ReactoryWorkflowService@1.0.0') return workflow;
      throw new Error(`unexpected service ${id}`);
    },
    partner: { key: 'acme' },
    log: jest.fn(),
  };
  return new TemporalEventBridgeService({}, context);
}

/** A pointer that is still parked on the settled topic. */
const stillWaiting = [{ eventName: DEFAULT_SETTLED_EVENT, eventPublished: false }];
/** A pointer that has consumed the event. */
const consumed = [{ eventName: DEFAULT_SETTLED_EVENT, eventPublished: true }];

describe('TemporalEventBridgeService', () => {
  beforeEach(() => {
    mockStore.length = 0;
    mockIdSeq = 0;
    jest.clearAllMocks();
  });

  describe('watch registration', () => {
    it('creates one pending watch and attaches a result listener', async () => {
      const temporal = fakeTemporalService();
      const service = makeService(temporal, fakeWorkflowService({ waitingPointers: consumed }));

      const { watchId } = await service.watch({
        workflowId: 'batch_1',
        runId: 'run-1',
        instanceId: 'instance-1',
        stepId: 'payout',
        awaitTimeout: 60000,
      });

      expect(watchId).toBeTruthy();
      expect(mockStore).toHaveLength(1);
      expect(mockStore[0]).toMatchObject({
        workflowId: 'batch_1',
        eventName: DEFAULT_SETTLED_EVENT,
        eventKey: 'batch_1',
        instanceId: 'instance-1',
        status: 'pending',
      });
      expect(mockStore[0].awaitDeadline).toBeInstanceOf(Date);
      // The listener awaits the run's outcome in the background.
      expect(temporal.awaitOutcome).toHaveBeenCalledWith('batch_1', 'run-1', undefined, expect.any(Number));
    });

    it('re-registering the same target updates the existing watch instead of duplicating', async () => {
      const service = makeService(fakeTemporalService(), fakeWorkflowService());
      await service.watch({ workflowId: 'batch_1', instanceId: 'instance-1' });
      await service.watch({ workflowId: 'batch_1', instanceId: 'instance-1', stepId: 'payout' });

      // An engine retry of the awaiting step must not create a second watch that
      // would publish the same outcome twice.
      expect(mockStore).toHaveLength(1);
      expect(mockStore[0].stepId).toBe('payout');
    });
  });

  describe('delivery guarantee', () => {
    it('persists the outcome before publishing, and acks once the pointer consumed it', async () => {
      const workflow = fakeWorkflowService({ waitingPointers: consumed });
      const service = makeService(fakeTemporalService(), workflow);

      await service.watch({ workflowId: 'batch_1', instanceId: 'instance-1' });
      // Let the in-process listener settle the watch.
      await new Promise((r) => setTimeout(r, 20));

      const watch = mockStore[0];
      expect(watch.outcome).toMatchObject({ status: 'COMPLETED', result: { totalSettled: 42 } });
      expect(watch.status).toBe('acked');
      expect(watch.ackedAt).toBeInstanceOf(Date);

      // The settled topic carries the outcome...
      expect(workflow.publishWorkflowEvent).toHaveBeenCalledWith(
        DEFAULT_SETTLED_EVENT,
        'batch_1',
        expect.objectContaining({ status: 'COMPLETED', result: { totalSettled: 42 } }),
        'acme',
      );
      // ...and the informational per-outcome topic is published alongside it.
      expect(workflow.publishWorkflowEvent).toHaveBeenCalledWith(
        TEMPORAL_EVENTS.completed,
        'batch_1',
        expect.anything(),
        'acme',
      );
    });

    it('leaves a watch settled-but-unacked while the step has not consumed the event', async () => {
      const workflow = fakeWorkflowService({ waitingPointers: stillWaiting });
      const service = makeService(fakeTemporalService(), workflow);

      await service.watch({ workflowId: 'batch_1', instanceId: 'instance-1' });
      await new Promise((r) => setTimeout(r, 20));

      // This is the lost-wakeup case: published, but nothing consumed it.
      expect(mockStore[0].status).toBe('settled');
      expect(mockStore[0].attempts).toBe(1);
    });

    it('re-publishes an unacked watch on the next sweep', async () => {
      const workflow = fakeWorkflowService({ waitingPointers: stillWaiting });
      const service = makeService(fakeTemporalService(), workflow);

      await service.watch({ workflowId: 'batch_1', instanceId: 'instance-1' });
      await new Promise((r) => setTimeout(r, 20));
      expect(mockStore[0].attempts).toBe(1);

      // The backoff gate normally holds the next attempt back; simulate the wait.
      mockStore[0].lastAttemptAt = new Date(Date.now() - 60 * 60 * 1000);
      const report = await service.sweep();

      expect(report.republished).toBe(1);
      expect(mockStore[0].attempts).toBe(2);
      expect(mockStore[0].status).toBe('settled');
    });

    it('acks on a later sweep once the step finally consumes the event', async () => {
      const workflow = fakeWorkflowService({ waitingPointers: stillWaiting });
      const service = makeService(fakeTemporalService(), workflow);

      await service.watch({ workflowId: 'batch_1', instanceId: 'instance-1' });
      await new Promise((r) => setTimeout(r, 20));

      // The awaiting step has now suspended and received the republished event.
      workflow.getWorkflowHistoryById = jest.fn(async () => ({
        id: 'instance-1',
        tenantId: 'acme',
        executionPointers: consumed,
      }));
      mockStore[0].lastAttemptAt = new Date(Date.now() - 60 * 60 * 1000);

      const report = await service.sweep();
      expect(report.acked).toBe(1);
      expect(mockStore[0].status).toBe('acked');
    });

    it('abandons delivery after the attempt budget is exhausted', async () => {
      const workflow = fakeWorkflowService({ waitingPointers: stillWaiting });
      const service = makeService(fakeTemporalService(), workflow);

      await service.watch({ workflowId: 'batch_1', instanceId: 'instance-1' });
      await new Promise((r) => setTimeout(r, 20));

      // Fast-forward to the last permitted attempt.
      mockStore[0].attempts = 11;
      mockStore[0].lastAttemptAt = new Date(Date.now() - 24 * 60 * 60 * 1000);

      const report = await service.sweep();
      expect(mockStore[0].status).toBe('abandoned');
      expect(report.abandoned).toBe(1);
    });

    it('publishes under the waiting instance tenant, not the sweeper context', async () => {
      // The bridge context is partner "acme" but the instance belongs to "globex".
      const workflow = fakeWorkflowService({ waitingPointers: consumed, tenantId: 'globex' });
      const service = makeService(fakeTemporalService(), workflow);

      await service.watch({ workflowId: 'batch_1', instanceId: 'instance-1', clientKey: 'globex' });
      await new Promise((r) => setTimeout(r, 20));

      // Subscription matching is tenant-scoped: the wrong tenant is silently dropped.
      expect(workflow.publishWorkflowEvent).toHaveBeenCalledWith(
        DEFAULT_SETTLED_EVENT,
        'batch_1',
        expect.anything(),
        'globex',
      );
    });

    it('treats an unverifiable delivery as delivered when there is no instance to check', async () => {
      const workflow = fakeWorkflowService();
      const service = makeService(fakeTemporalService(), workflow);

      // No instanceId — nothing to verify against.
      await service.watch({ workflowId: 'batch_1' });
      await new Promise((r) => setTimeout(r, 20));

      expect(mockStore[0].status).toBe('acked');
    });
  });

  describe('sweep reconciliation', () => {
    it('settles a pending watch whose run finished while nothing was listening', async () => {
      const workflow = fakeWorkflowService({ waitingPointers: consumed });
      const temporal = fakeTemporalService({
        describeWorkflow: jest.fn(async (workflowId: string) => ({ workflowId, status: 'COMPLETED' })),
      });
      const service = makeService(temporal, workflow);

      // A watch left behind by a previous process: pending, no live listener here.
      mockStore.push(
        mockMakeWatch({
          workflowId: 'batch_orphan',
          eventName: DEFAULT_SETTLED_EVENT,
          eventKey: 'batch_orphan',
          instanceId: 'instance-1',
          clientKey: 'acme',
        }),
      );

      const report = await service.sweep();

      expect(temporal.describeWorkflow).toHaveBeenCalledWith('batch_orphan', undefined, undefined);
      expect(report.republished).toBe(1);
      expect(mockStore[0].status).toBe('acked');
    });

    it('publishes AWAIT_TIMEOUT for a watch that outlived its deadline', async () => {
      const workflow = fakeWorkflowService({ waitingPointers: consumed });
      const service = makeService(fakeTemporalService(), workflow);

      mockStore.push(
        mockMakeWatch({
          workflowId: 'batch_slow',
          eventName: DEFAULT_SETTLED_EVENT,
          eventKey: 'batch_slow',
          instanceId: 'instance-1',
          clientKey: 'acme',
          awaitDeadline: new Date(Date.now() - 1000),
          createdAt: new Date(Date.now() - 120000),
        }),
      );

      const report = await service.sweep();

      expect(report.timedOut).toBe(1);
      expect(workflow.publishWorkflowEvent).toHaveBeenCalledWith(
        DEFAULT_SETTLED_EVENT,
        'batch_slow',
        expect.objectContaining({ status: 'AWAIT_TIMEOUT', waitedMs: expect.any(Number) }),
        'acme',
      );
      expect(mockStore[0].status).toBe('timed_out');
    });
  });

  describe('external callbacks', () => {
    it('records the reported outcome on the matching watch and publishes it', async () => {
      const workflow = fakeWorkflowService({ waitingPointers: consumed });
      const service = makeService(fakeTemporalService(), workflow);

      mockStore.push(
        mockMakeWatch({
          workflowId: 'batch_ext',
          eventName: DEFAULT_SETTLED_EVENT,
          eventKey: 'batch_ext',
          instanceId: 'instance-1',
          clientKey: 'acme',
        }),
      );

      const result = await service.settleFromCallback({
        workflowId: 'batch_ext',
        runId: 'run-9',
        eventName: DEFAULT_SETTLED_EVENT,
        eventKey: 'batch_ext',
        data: { status: 'COMPLETED', result: { ok: true } },
      });

      expect(result).toEqual({ published: true, watchAcked: true });
      expect(mockStore[0].outcome).toMatchObject({ status: 'COMPLETED', result: { ok: true } });
      expect(mockStore[0].status).toBe('acked');
    });

    it('publishes an unwatched event without persisting anything', async () => {
      const workflow = fakeWorkflowService();
      const service = makeService(fakeTemporalService(), workflow);

      const result = await service.settleFromCallback({
        workflowId: 'never_watched',
        eventName: TEMPORAL_EVENTS.progress,
        eventKey: 'never_watched',
        data: { stage: 'screening' },
      });

      // A YAML author may still be waiting with an explicit wait_event step.
      expect(result).toEqual({ published: true, watchAcked: false });
      expect(workflow.publishWorkflowEvent).toHaveBeenCalledWith(
        TEMPORAL_EVENTS.progress,
        'never_watched',
        { stage: 'screening' },
        undefined,
      );
      expect(mockStore).toHaveLength(0);
    });
  });
});
