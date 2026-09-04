/**
 * Tests for TemporalClientService.
 *
 * Focused on the connection-management contract, because that is where the
 * previous implementation was wrong in ways a multi-tenant deployment would only
 * discover in production:
 *
 *   • one cached client for ALL configurations meant a second tenant silently used
 *     the first tenant's cluster and namespace;
 *   • TLS material was passed as a FILE PATH where the SDK expects certificate
 *     bytes, so mTLS could never have worked;
 *
 * plus the outcome/status normalisation the bridge and steps depend on.
 */

import { mkdtempSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

/** Captured Connection.connect / new Client arguments. */
const mockConnectCalls: any[] = [];
const mockClientCalls: any[] = [];
const mockClosed: string[] = [];

jest.mock('@temporalio/client', () => {
  class FakeWorkflowFailedError extends Error {
    constructor(message: string, public cause?: any) {
      super(message);
      this.name = 'WorkflowFailedError';
    }
  }
  return {
    __esModule: true,
    Connection: {
      connect: jest.fn(async (options: any) => {
        mockConnectCalls.push(options);
        return {
          close: jest.fn(async () => {
            mockClosed.push(options.address);
          }),
        };
      }),
    },
    Client: jest.fn().mockImplementation((options: any) => {
      mockClientCalls.push(options);
      return {
        namespace: options.namespace,
        workflow: {
          start: jest.fn(),
          signalWithStart: jest.fn(),
          getHandle: jest.fn(),
        },
      };
    }),
    WorkflowFailedError: FakeWorkflowFailedError,
    WorkflowHandle: class {},
  };
});

jest.mock('@temporalio/common', () => ({
  __esModule: true,
  WorkflowExecutionAlreadyStartedError: class WorkflowExecutionAlreadyStartedError extends Error {
    constructor(message = 'already started') {
      super(message);
      this.name = 'WorkflowExecutionAlreadyStartedError';
    }
  },
}));

// eslint-disable-next-line @typescript-eslint/no-var-requires
const {
  TemporalClientService,
  mapExecutionStatus,
  isTerminalStatus,
  normaliseResult,
  DEFAULT_MAX_RESULT_BYTES,
} = require('../TemporalClientService');

function service(context?: any) {
  return new TemporalClientService({}, context);
}

describe('TemporalClientService connection management', () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    mockConnectCalls.length = 0;
    mockClientCalls.length = 0;
    mockClosed.length = 0;
    delete process.env.TEMPORAL_ADDRESS;
    delete process.env.TEMPORAL_NAMESPACE;
    delete process.env.TEMPORAL_API_KEY;
    delete process.env.TEMPORAL_TLS_CERT_PATH;
    delete process.env.TEMPORAL_TLS_KEY_PATH;
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  it('defaults to localhost and the default namespace', async () => {
    const svc = service();
    await svc.getClient();
    expect(mockConnectCalls[0].address).toBe('localhost:7233');
    expect(mockClientCalls[0].namespace).toBe('default');
  });

  it('reuses one client per resolved configuration', async () => {
    const svc = service();
    const a = await svc.getClient({ address: 'temporal:7233', namespace: 'payments' });
    const b = await svc.getClient({ address: 'temporal:7233', namespace: 'payments' });
    expect(a).toBe(b);
    expect(mockConnectCalls).toHaveLength(1);
  });

  it('creates SEPARATE clients for different namespaces on the same cluster', async () => {
    const svc = service();
    const payments = await svc.getClient({ address: 'temporal:7233', namespace: 'payments' });
    const kyc = await svc.getClient({ address: 'temporal:7233', namespace: 'kyc' });

    // The bug this replaces: a single cached client meant the second tenant's calls
    // silently went to the first tenant's namespace.
    expect(payments).not.toBe(kyc);
    expect(mockClientCalls.map((call) => call.namespace)).toEqual(['payments', 'kyc']);
  });

  it('creates separate clients when credentials differ for the same address', async () => {
    const svc = service();
    await svc.getClient({ address: 'temporal:7233', namespace: 'ns', apiKey: 'key-a' });
    await svc.getClient({ address: 'temporal:7233', namespace: 'ns', apiKey: 'key-b' });
    expect(mockConnectCalls).toHaveLength(2);
  });

  it('shares one connect attempt between concurrent callers', async () => {
    const svc = service();
    await Promise.all([svc.getClient(), svc.getClient(), svc.getClient()]);
    expect(mockConnectCalls).toHaveLength(1);
  });

  it('reads TLS certificates from disk rather than passing the path through', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'temporal-tls-'));
    const certPath = join(dir, 'client.crt');
    const keyPath = join(dir, 'client.key');
    writeFileSync(certPath, '-----BEGIN CERTIFICATE-----\nCERTBYTES\n-----END CERTIFICATE-----');
    writeFileSync(keyPath, '-----BEGIN PRIVATE KEY-----\nKEYBYTES\n-----END PRIVATE KEY-----');

    const svc = service();
    await svc.getClient({
      address: 'temporal.cloud:7233',
      namespace: 'ns',
      tls: { clientCertPath: certPath, clientKeyPath: keyPath },
    });

    const tls = mockConnectCalls[0].tls;
    expect(tls.clientCertPair.crt.toString()).toContain('CERTBYTES');
    expect(tls.clientCertPair.key.toString()).toContain('KEYBYTES');
    // Not the filename — the previous implementation handed Temporal the path text.
    expect(tls.clientCertPair.crt.toString()).not.toContain(certPath);
  });

  it('accepts inline PEM material as well as a path', async () => {
    const svc = service();
    const pem = '-----BEGIN CERTIFICATE-----\nINLINE\n-----END CERTIFICATE-----';
    await svc.getClient({
      address: 'temporal:7233',
      tls: { clientCertPath: pem, clientKeyPath: pem },
    });
    expect(mockConnectCalls[0].tls.clientCertPair.crt.toString()).toContain('INLINE');
  });

  it('fails loudly when a configured certificate cannot be read', async () => {
    const svc = service();
    await expect(
      svc.getClient({ tls: { clientCertPath: '/no/such/cert.pem', clientKeyPath: '/no/such/key.pem' } }),
    ).rejects.toThrow(/Unable to read Temporal client certificate/);
  });

  it('enables TLS implicitly for API-key auth', async () => {
    const svc = service();
    await svc.getClient({ address: 'temporal.cloud:7233', apiKey: 'secret' });
    // Temporal Cloud API keys are only valid over TLS.
    expect(mockConnectCalls[0].tls).toBe(true);
    expect(mockConnectCalls[0].apiKey).toBe('secret');
  });

  it('uses no TLS for a plain local connection', async () => {
    const svc = service();
    await svc.getClient();
    expect(mockConnectCalls[0].tls).toBeUndefined();
  });

  it('resolves a named connection from partner settings ahead of the environment', async () => {
    process.env.TEMPORAL_ADDRESS = 'env-host:7233';
    const context = {
      partner: {
        key: 'acme',
        getSetting: (name: string) =>
          name === 'temporal/payments'
            ? { data: { address: 'settings-host:7233', namespace: 'payments' } }
            : null,
      },
    };

    const svc = service(context);
    const resolved = svc.resolveConnectionConfig({ connectionKey: 'payments' });
    expect(resolved.address).toBe('settings-host:7233');
    expect(resolved.namespace).toBe('payments');
  });

  it('falls back to a per-key environment variable, then the conventional one', async () => {
    process.env.TEMPORAL_ADDRESS = 'default-host:7233';
    process.env.TEMPORAL_PAYMENTS_ADDRESS = 'payments-host:7233';

    const svc = service();
    expect(svc.resolveConnectionConfig({ connectionKey: 'payments' }).address).toBe('payments-host:7233');
    expect(svc.resolveConnectionConfig({}).address).toBe('default-host:7233');

    delete process.env.TEMPORAL_PAYMENTS_ADDRESS;
  });

  it('lets an explicit config win over settings and environment', async () => {
    process.env.TEMPORAL_ADDRESS = 'env-host:7233';
    const svc = service({
      partner: { getSetting: () => ({ data: { address: 'settings-host:7233' } }) },
    });
    expect(
      svc.resolveConnectionConfig({ connectionKey: 'x', address: 'explicit:7233' }).address,
    ).toBe('explicit:7233');
  });

  it('closes every cached connection on close()', async () => {
    const svc = service();
    await svc.getClient({ address: 'a:7233' });
    await svc.getClient({ address: 'b:7233' });
    await svc.close();
    expect(mockClosed.sort()).toEqual(['a:7233', 'b:7233']);
    // A subsequent call reconnects rather than handing back a closed client.
    await svc.getClient({ address: 'a:7233' });
    expect(mockConnectCalls).toHaveLength(3);
  });
});

describe('status mapping', () => {
  it('normalises the Temporal status names the module reports', () => {
    expect(mapExecutionStatus('RUNNING')).toBe('RUNNING');
    expect(mapExecutionStatus('COMPLETED')).toBe('COMPLETED');
    expect(mapExecutionStatus('FAILED')).toBe('FAILED');
    expect(mapExecutionStatus('TERMINATED')).toBe('TERMINATED');
    expect(mapExecutionStatus('TIMED_OUT')).toBe('TIMED_OUT');
    expect(mapExecutionStatus('CONTINUED_AS_NEW')).toBe('CONTINUED_AS_NEW');
    // Both spellings appear across SDK versions and proto enums.
    expect(mapExecutionStatus('CANCELLED')).toBe('CANCELED');
    expect(mapExecutionStatus('CANCELED')).toBe('CANCELED');
    expect(mapExecutionStatus('something-else')).toBe('UNKNOWN');
    expect(mapExecutionStatus(undefined)).toBe('UNKNOWN');
  });

  it('treats only settled states as terminal', () => {
    expect(isTerminalStatus('RUNNING')).toBe(false);
    // UNKNOWN is deliberately NOT terminal: a describe failure must not be read as
    // "the run finished", which would resolve an await with no outcome.
    expect(isTerminalStatus('UNKNOWN')).toBe(false);
    expect(isTerminalStatus('COMPLETED')).toBe(true);
    expect(isTerminalStatus('FAILED')).toBe(true);
    expect(isTerminalStatus('CANCELED')).toBe(true);
  });
});

describe('result normalisation', () => {
  it('round-trips a serializable result', () => {
    expect(normaliseResult({ totalSettled: 42 })).toEqual({ result: { totalSettled: 42 } });
  });

  it('preserves null and undefined', () => {
    expect(normaliseResult(null)).toEqual({ result: null });
    expect(normaliseResult(undefined)).toEqual({ result: null });
  });

  it('reports oversized payloads instead of storing them', () => {
    const outcome = normaliseResult({ blob: 'x'.repeat(1024) }, 256);
    // Workflow step outputs are persisted into the durable instance; an oversized
    // payload would bloat every later persist.
    expect(outcome.truncated).toBe(true);
    expect(outcome.result).toBeUndefined();
    expect(outcome.bytes).toBeGreaterThan(256);
  });

  it('strips non-serializable values rather than breaking instance persistence', () => {
    const circular: any = { name: 'loop' };
    circular.self = circular;
    const outcome = normaliseResult(circular);
    // A circular structure would throw on JSON.stringify inside the engine's
    // persistence, silently discarding the step advance and re-running it forever.
    expect(outcome.result).toBeDefined();
    expect(typeof outcome.result).toBe('string');
  });

  it('defaults the cap to 64KB', () => {
    expect(DEFAULT_MAX_RESULT_BYTES).toBe(64 * 1024);
  });
});
