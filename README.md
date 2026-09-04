# Reactory Temporal Integration Module (`reactory-temporal`)

The `reactory-temporal` module bridges the **Reactory Application & Presentation Platform** with the **Temporal.io Distributed Execution Engine**. It provides native service bindings, GraphQL BFF resolvers, dynamic configuration forms, Reactory step workflow macros, and CLI management tools.

---

## 1. Architectural Philosophy: The Two Engines

Reactory applications leverage two distinct orchestration engines, each specialized for different failure domains and execution models:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                   REACTORY WORKFLOW ENGINE (Declarative Pipeline)                      │
│                                                                                        │
│   YAML / JSON Definition ──► Step 1 (Macro) ──► Step 2 (Macro) ──► Step 3 (Macro)      │
│                                    │                 │                 │               │
│                                    ▼                 ▼                 ▼               │
│                             DB State Snapshot  DB State Snapshot  DB State Snapshot    │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                           ▲
                                           │ Bridges via `reactory-temporal`
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                   TEMPORAL WORKFLOW ENGINE (Deterministic Replay)                      │
│                                                                                        │
│   Imperative TS Code ──► Event History Log (Append-Only) ──► Distributed Worker Replay │
│                                    │                                                   │
│                                    ▼                                                   │
│                  [Event 1: Started] ──► [Event 2: Activity 1] ──► [Event 3: Signal]   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### The Difference: State-Snapshot vs. Event-Replay

| Dimension | `reactory-workflow-es` (v2.5.0) | `Temporal.io` Engine |
|---|---|---|
| **Architecture** | **Embedded in-process engine** inside Node.js / Electron. | **Distributed cluster + stateless worker processes**. |
| **Persistence Model** | **Mutable State Snapshot (`WorkflowInstance`):** Writes full execution pointers, status, and JSON data blobs to MongoDB/PostgreSQL on step boundaries. | **Append-Only Event Sourcing (History Log):** State is never saved directly; every event, timer, and signal is appended to an immutable history. |
| **Failure Recovery** | Resumes from the last saved `ExecutionPointer`. Ambiguous network timeouts require explicit compensation steps. | **Thread Replay (Virtual Synchrony):** A new worker downloads history, fast-forwards through code in microseconds, and restores runtime call stacks. |
| **Sleeping & Timers** | Background poll-worker queries the database table (`pollIntervalMs`) for overdue sleep timestamps. | Server-side distributed timer priority queues. Millions of concurrent timers wait with **zero CPU / memory overhead**. |
| **Worker State** | Worker process manages local workflow execution and optimistic DB locks. | Workers are **100% stateless**. Any worker can be killed (`kill -9`) at any second with zero data loss. |
| **Human-in-the-Loop** | Database polling, custom event queues, or webhook resume handlers. | Built-in **Signals** and `await condition()` pauses code execution safely for days or weeks. |
| **Primary Sweet Spot** | Form lifecycles, report generation, system ETL routines, and desktop/Electron applications. | High-volume financial payouts, distributed multi-service sagas, and long-running cloud infrastructure leases. |

---

## 2. Complementary Roles: Experience Plane vs. Execution Kernel

Rather than competing, the two systems serve complementary layers of the stack:

```
                       ┌──────────────────────────────────────────────┐
                       │               PRESENTATION TIER              │
                       │   Reactory Dynamic Forms & React UI Shell    │
                       │   (Maker CSV Upload, Checker Review Screen)  │
                       └──────────────────────┬───────────────────────┘
                                              │ Form Submission / Action
                                              ▼
                       ┌──────────────────────────────────────────────┐
                       │           APPLICATION / BFF CONTROL          │
                       │   Reactory Workflow & GraphQL BFF Layer      │
                       │   • RBAC and Tenant Authorization            │
                       │   • Client-side Data Formatting & PDF Render │
                       │   • Invokes Macro: `temporalStartWorkflow`   │
                       └──────────────────────┬───────────────────────┘
                                              │ gRPC Task Dispatch
                                              ▼
                       ┌──────────────────────────────────────────────┐
                       │           DURABLE EXECUTION KERNEL           │
                       │   Temporal Distributed Cluster               │
                       │   • 10,000-Row Payout Fan-Out                │
                       │   • Multi-Day Maker/Checker Approval Pause   │
                       │   • Ledger Hold/Settle Idempotent Retries    │
                       └──────────────────────┬───────────────────────┘
                                              │ Signal `approveBatch`
                                              ▲
                       ┌──────────────────────┴───────────────────────┐
                       │            OPS & COMPLIANCE COCKPIT          │
                       │   Reactory Admin UI (LOD1 / LOD2 Analysts)   │
                       │   • Reviews Alerts, Clears False Positives   │
                       │   • Clicks "Approve" -> Sends Temporal Signal│
                       └──────────────────────────────────────────────┘
```

- **Reactory acts as the Experience and Control Plane**: It handles user authentication, RBAC authorization, multi-tenant UI theming, dynamic form generation, and GraphQL composition.
- **Temporal acts as the Durable Execution Kernel**: It executes mission-critical sagas, handles distributed retries, prevents double-spending, and guarantees execution to completion across infrastructure failures.

---

## 3. Module Structure

```
reactory-temporal/
├── package.json                     # @temporalio/client, /worker, /workflow, /common
├── index.ts                         # IReactoryModule registration (workflows + workflowSteps)
├── types/                           # Connection, start/signal/query/update, outcome, watch contracts
├── services/
│   ├── TemporalClientService.ts     # Per-config connection cache + workflow lifecycle operations
│   └── TemporalEventBridgeService.ts# Delivers Temporal outcomes into the Reactory workflow engine
├── models/
│   └── TemporalWorkflowWatch.ts     # Durable "an instance is awaiting this run" record
├── workflow/                        # ── Reactory workflow-engine integration ──
│   ├── events.ts                    # Event/topic contract + bridge interface
│   └── steps/
│       ├── BaseTemporalStep.ts      # Connection resolution, templating, result extraction
│       ├── TemporalWorkflowStep.ts  # temporal_workflow — start (+ optional durable await)
│       ├── TemporalAwaitStep.ts     # temporal_await — await a run started elsewhere
│       ├── TemporalSignalStep.ts    # temporal_signal
│       ├── TemporalQueryStep.ts     # temporal_query
│       ├── TemporalUpdateStep.ts    # temporal_update
│       ├── TemporalCancelStep.ts    # temporal_cancel
│       ├── TemporalTerminateStep.ts # temporal_terminate
│       ├── TemporalDescribeStep.ts  # temporal_describe
│       ├── designer/definitions.ts  # Visual Workflow Designer definitions
│       └── index.ts                 # IWorkflowStepProvider[] (module.workflowSteps)
├── workflows/                       # ── Engine workflows + Temporal-side sample ──
│   ├── TemporalBridgeBootstrapWorkflow.ts  # autoStart: activates the bridge at boot
│   ├── activities.ts                # Sample activities + notifyReactoryActivity
│   ├── workflows.ts                 # Deterministic B2BMassPayoutWorkflow (runs on a worker)
│   ├── worker.ts                    # Standalone worker runtime
│   └── clientTest.ts                # End-to-end integration test runner
├── examples/                        # YAML workflows demonstrating the integration
│   ├── DispatchBatchPayout.yaml     # Durable single-step await
│   └── ApproveBatchPayout.yaml      # Maker/checker across both engines
├── graphql/                         # Queries, mutations, watch introspection
├── routes/                          # POST /api/temporal/v1/events (external callbacks)
├── forms/                           # Dynamic admin connection form schema
├── cli/                             # CLI commands (ping, describe, worker, test-batch)
└── docs/
    └── WORKFLOW_INTEGRATION.md      # Design: engine internals, contracts, failure modes
```

## 4. Invoking Temporal from Reactory Workflows

The module contributes **step types** to the Reactory YAML workflow engine (registered
through `module.workflowSteps` and discovered at server startup), so Temporal is
addressed by `type:` like any other step — and appears in the Visual Workflow Designer
catalog with its own config forms.

| Step type | What it does |
|---|---|
| `temporal_workflow` | Start an execution and, optionally, await its outcome |
| `temporal_await` | Await an execution started elsewhere (another step, a mutation, an operator) |
| `temporal_signal` | Send a Signal — releases a Temporal `await condition(...)` gate |
| `temporal_query` | Run a read-only Query |
| `temporal_update` | Execute a Workflow Update (validated request/response) |
| `temporal_cancel` | Request graceful cancellation |
| `temporal_terminate` | Terminate immediately (no cleanup — prefer cancel) |
| `temporal_describe` | Read status/metadata without waiting, for branching |

### 4.1 Await modes

`temporal_workflow` and `temporal_await` accept `awaitMode`:

| Mode | Behaviour | Use when |
|---|---|---|
| `none` | Start and continue | fire-and-forget, or you will wait later in the flow |
| `event` | **Durable suspend.** Registers a completion watch, then parks the instance — no worker held, survives restarts. The bridge publishes the outcome and the same step resumes with it. | long runs, multi-day human-in-the-loop gates |
| `sleep_poll` | **Durable poll.** Sleeps, re-runs, re-checks; restart-safe and needs no bridge. | short runs, or deployments without the bridge |
| `block_poll` | Waits inside the step | the non-durable executor (CLI/tests); restarts its wait after a crash |

`event` degrades to `sleep_poll` when the bridge is unavailable, and both degrade to
`block_poll` on an executor that cannot suspend — a suspension nobody can satisfy
would hang the instance forever.

### 4.2 A durable await in one step

```yaml
nameSpace: my-app
name: DispatchBatchPayout
version: 1.0.0
inputs:
  batchId: { type: string, required: true }
steps:
  - id: payout
    type: temporal_workflow
    config:
      workflowType: B2BMassPayoutWorkflow
      # MUST be deterministic: an engine retry or a post-restart re-run re-attaches
      # to this execution instead of starting a second payout.
      workflowId: "batch_${input.batchId}"
      taskQueue: b2b-payouts-queue
      awaitMode: event
      awaitTimeout: 604800000        # 7 days, delivered as an AWAIT_TIMEOUT outcome
      resultPath: totalSettled       # keeps large payloads out of the instance
      args:
        - batchId: "${input.batchId}"
          currency: GBP
  - id: report
    type: log
    dependsOn: payout
    config:
      message: "Batch ${input.batchId}: ${steps.payout.outputs.status} (${steps.payout.outputs.result})"
```

Outputs: `{ workflowId, runId, status, started, reattached, awaited, result?, failure? }`.
Set `failOnWorkflowFailure: false` to branch on `status` instead of failing the run.

### 4.3 Human-in-the-loop, both directions

Reactory owns the approval experience; Temporal owns the money. See
`examples/ApproveBatchPayout.yaml` for the full flow: start the batch → suspend on a
core `wait_event` until an operator approves → `temporal_signal` to release Temporal's
gate → `temporal_await` the settlement.

```yaml
  - id: approveInTemporal
    type: temporal_signal
    dependsOn: awaitApproval
    config:
      workflowId: "${steps.startPayout.outputs.workflowId}"
      signalName: approveBatch
      args: ["${approval.approverId}"]
```

### 4.4 GraphQL BFF

#### Start Workflow (Maker Submission)
```graphql
mutation StartBatch {
  temporalStartWorkflow(input: {
    workflowType: "B2BMassPayoutWorkflow"
    workflowId: "payroll_batch_2026_001"
    taskQueue: "b2b-payouts-queue"
    args: [{ batchId: "payroll_batch_2026_001", accountId: "acc_acme_payroll_gbp", currency: "GBP" }]
  }) { workflowId runId status }
}
```

#### Signal Workflow (Checker Approval Gate)
```graphql
mutation ApproveBatch {
  temporalSignalWorkflow(input: {
    workflowId: "payroll_batch_2026_001"
    signalName: "approveBatch"
    args: ["user_checker_werner"]
  })
}
```

#### Query In-Flight Progress
```graphql
query GetBatchProgress {
  temporalQueryWorkflow(input: { workflowId: "payroll_batch_2026_001", queryType: "getBatchProgress" })
}
```

#### Inspect outstanding completion watches
```graphql
query Watches {
  temporalWorkflowWatches(filter: { status: settled }) {
    workflowId eventName status outcomeStatus instanceId attempts lastError
  }
}
```

---

## 5. How Temporal wakes a Reactory workflow

`TemporalEventBridgeService` turns a Temporal outcome into a workflow-engine event
that resumes the waiting step. It is built around three facts about the engine:

1. **An event with no matching subscription is discarded.** The event queue marks it
   processed and drops it; there is no replay. A publish that beats the awaiting step
   into suspension would be lost, so the bridge **persists the outcome to a
   `TemporalWorkflowWatch` before publishing** and re-publishes on every sweep until
   the waiting execution pointer is observed to have consumed it. The step also
   `describe()`s before parking, which closes the fast-completion window entirely.
2. **Restarts kill in-memory listeners.** Watches are durable; the sweeper re-attaches
   and reconciles at boot, so a completion that happened while Reactory was down is
   delivered on the first sweep.
3. **Subscriptions are tenant-scoped.** Instances are started under `partner.key`, so
   the bridge publishes under the *waiting instance's* tenant, not the sweeper's.

### 5.1 Event topics

| Topic | Meaning |
|---|---|
| `temporal.workflow.settled` | **The topic awaiting steps subscribe to** — published for every terminal outcome, with `status` in the payload |
| `temporal.workflow.completed` / `.failed` / `.cancelled` / `.timeout` | Informational per-outcome topics, for observability or an explicit `wait_event` |
| `temporal.workflow.progress` | Optional mid-run progress, pushed by an activity |

The correlation key (`eventKey`) is the Temporal **workflowId** — deterministic, known
before the run starts, and stable across continue-as-new.

> A step cannot subscribe to four topics at once: workflow-es matches one
> `(eventName, eventKey)` pair per execution pointer. That is why the collapsed
> `settled` topic exists.

### 5.2 Externally hosted workers

A Temporal worker Reactory did not start (another cluster, another language) can wake
a waiting instance directly:

```bash
curl -X POST "$REACTORY_API_URL/api/temporal/v1/events" \
  -H 'content-type: application/json' \
  -H "x-client-key: $REACTORY_CLIENT_KEY" -H "x-client-pwd: $REACTORY_CLIENT_PWD" \
  -d '{"eventKey":"batch_2026_001","data":{"status":"COMPLETED","result":{"totalSettled":42}}}'
```

`GET /api/temporal/v1/watches` lists outstanding watches for the calling partner.
From inside a Temporal workflow, use the `notifyReactoryActivity` activity (never
inline workflow code — workflow functions must stay deterministic).


## 6. Local Development with Podman

A pre-configured Podman Compose stack is included in `docker/config/docker-compose-temporal.yaml`.

### Start the Temporal Stack
```bash
cd /Users/wweber/Source/reactory/reactory-express-server
podman-compose -f docker/config/docker-compose-temporal.yaml up -d
```

### Stack Components & Port Mappings
- **Temporal Frontend (gRPC):** `localhost:7233`
- **Temporal Web UI:** [http://localhost:8233](http://localhost:8233)
- **PostgreSQL Database:** Internal container on `temporal-network`
- **Admin Tools CLI:** `reactory-temporal-admin-tools`

### Run the Sample Worker & Test Client
```bash
# Terminal 1: Start Worker
npx ts-node src/modules/reactory-temporal/workflows/worker.ts

# Terminal 2: Run End-to-End Payout Test
npx ts-node src/modules/reactory-temporal/workflows/clientTest.ts
```

### Stop the Stack
```bash
podman-compose -f docker/config/docker-compose-temporal.yaml down
```

---

## 7. Configuration & Environment Variables

| Variable | Default | Description |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Host and port of the Temporal frontend service. |
| `TEMPORAL_NAMESPACE` | `default` | Target Temporal namespace for workflow execution. |
| `TEMPORAL_API_KEY` | *(optional)* | API Key for authenticating with Temporal Cloud. |
| `TEMPORAL_TLS_CERT_PATH` | *(optional)* | Path or PEM string for client certificate (mTLS). |
| `TEMPORAL_TLS_KEY_PATH` | *(optional)* | Path or PEM string for client private key (mTLS). |
| `TEMPORAL_TLS_CA_PATH` | *(optional)* | Path or PEM string for the server root CA certificate. |
| `TEMPORAL_TLS_SERVER_NAME` | *(optional)* | SNI override for TLS host-name checking. |
| `TEMPORAL_TASK_QUEUE` | `b2b-payouts-queue` | Default task queue for worker listeners. |
| `TEMPORAL_<KEY>_ADDRESS` | *(optional)* | Address for the named connection `<key>` (e.g. `TEMPORAL_PAYMENTS_ADDRESS`), used by a step's `connectionKey`. Also `_NAMESPACE` and `_API_KEY`. |
| `TEMPORAL_BRIDGE_SWEEP_INTERVAL_MS` | `30000` | Cadence of the bridge's reconciliation sweep. |
| `REACTORY_API_URL` | *(worker-side)* | Base URL `notifyReactoryActivity` posts callbacks to. |
| `REACTORY_CLIENT_KEY` / `REACTORY_CLIENT_PWD` | *(worker-side)* | Credentials for that callback. |

Connection resolution order for a step's `connectionKey`: explicit `connection` config →
partner setting `temporal/<key>` → `TEMPORAL_<KEY>_*` → `TEMPORAL_*` → `localhost:7233`.
A certificate value may be either a filesystem path or the PEM contents themselves.

---

## 8. Determinism Rules for Workflow Authors

Because Temporal workflows are replayed from event logs, workflow functions **must be 100% deterministic**:

| ❌ NEVER DO inside a Workflow Function | ✅ DO INSTEAD |
|---|---|
| `Math.random()` or UUID generators | Generate IDs in an Activity or pass as workflow arguments |
| `Date.now()` or `new Date()` | Use Temporal's deterministic `workflow.now()` |
| `setTimeout()` or `setInterval()` | Use `await workflow.sleep('2h')` or `condition()` |
| Direct `fetch()`, `axios`, or DB queries | Put all I/O and network requests inside **Activities** |
| Mutable global state across workflow runs | Keep variables local to the workflow function scope |
| Calling back into Reactory from workflow code | Use the `notifyReactoryActivity` **activity** |
| Non-deterministic thread race conditions | Use deterministic promise combinators (`Promise.all()`, `condition()`) |

---

---

## 9. Further Reading

- [`docs/WORKFLOW_INTEGRATION.md`](docs/WORKFLOW_INTEGRATION.md) — the design behind the
  workflow-engine integration: how the YAML engine executes steps, the durable control
  directive that lets a module step suspend, the event contract, the delivery
  guarantee, failure modes, and the test plan.

## License

MIT © [Reactory](https://github.com/reactorynet)
