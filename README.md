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
├── package.json               # Dependencies: @temporalio/client, @temporalio/worker, @temporalio/workflow
├── index.ts                   # IReactoryModule registration (reactory.Temporal@1.0.0)
├── types/                     # Connection configurations, input contracts
├── services/
│   └── TemporalClientService.ts # Core client connection pooling & workflow lifecycle
├── graphql/
│   ├── schema/Temporal.graphql # Queries and mutations for Temporal operations
│   └── resolvers/index.ts     # Resolvers mapping GraphQL -> TemporalClientService
├── workflow/
│   └── TemporalStepMacros.ts  # Macros for Reactory YAML/JSON workflows
├── forms/
│   └── TemporalConfigForm.ts  # Dynamic admin connection form schema
├── cli/
│   └── TemporalCli.ts         # CLI commands (ping, describe, worker, test-batch)
└── workflows/                 # Sample B2B Payout workflows, activities & worker
    ├── activities.ts          # Side-effect activities (Ledger hold, Bank API, Settle)
    ├── workflows.ts           # Deterministic B2BMassPayoutWorkflow
    ├── worker.ts              # Standalone worker runtime
    └── clientTest.ts          # End-to-end integration test runner
```

---

## 4. Usage & Integration Examples

### 4.1 Invoking Temporal from Reactory YAML Workflows
Reactory declarative workflows can start, signal, and query Temporal workflows using built-in macros:

```yaml
name: ProcessBatchPayoutPipeline
description: Validates CSV rows in Reactory and dispatches execution to Temporal
steps:
  - id: validateCsvInput
    macro: csvValidator
    params:
      file: "{{vars.uploadedFile}}"

  - id: startTemporalWorkflow
    macro: temporalStartWorkflow
    params:
      workflowType: "B2BMassPayoutWorkflow"
      workflowId: "batch_{{vars.batchId}}"
      taskQueue: "b2b-payouts-queue"
      args:
        - batchId: "{{vars.batchId}}"
          accountId: "acc_acme_payroll_gbp"
          currency: "GBP"
          rows: "{{vars.validatedRows}}"
```

### 4.2 GraphQL BFF Mutations (Maker / Checker Approvals)

#### Start Workflow (Maker Submission)
```graphql
mutation StartBatch {
  temporalStartWorkflow(input: {
    workflowType: "B2BMassPayoutWorkflow"
    workflowId: "payroll_batch_2026_001"
    taskQueue: "b2b-payouts-queue"
    args: [{
      batchId: "payroll_batch_2026_001",
      accountId: "acc_acme_payroll_gbp",
      currency: "GBP",
      rows: [
        { rowId: "1", beneficiaryName: "Sarah Jenkins", accountNumber: "GB82WEST12345678901234", amount: 3200, currency: "GBP" },
        { rowId: "2", beneficiaryName: "Alex Thorne", accountNumber: "GB29NWBK60161331926819", amount: 2850, currency: "GBP" }
      ]
    }]
  }) {
    workflowId
    runId
    status
  }
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

#### Query In-Flight Progress (Live Status)
```graphql
query GetBatchProgress {
  temporalQueryWorkflow(input: {
    workflowId: "payroll_batch_2026_001"
    queryType: "getBatchProgress"
  })
}
```

---

## 5. Local Development with Podman

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

## 6. Configuration & Environment Variables

| Variable | Default | Description |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Host and port of the Temporal frontend service. |
| `TEMPORAL_NAMESPACE` | `default` | Target Temporal namespace for workflow execution. |
| `TEMPORAL_API_KEY` | *(optional)* | API Key for authenticating with Temporal Cloud. |
| `TEMPORAL_TLS_CERT_PATH` | *(optional)* | Path or PEM string for client certificate (mTLS). |
| `TEMPORAL_TLS_KEY_PATH` | *(optional)* | Path or PEM string for client private key (mTLS). |
| `TEMPORAL_TASK_QUEUE` | `b2b-payouts-queue` | Default task queue for worker listeners. |

---

## 7. Determinism Rules for Workflow Authors

Because Temporal workflows are replayed from event logs, workflow functions **must be 100% deterministic**:

| ❌ NEVER DO inside a Workflow Function | ✅ DO INSTEAD |
|---|---|
| `Math.random()` or UUID generators | Generate IDs in an Activity or pass as workflow arguments |
| `Date.now()` or `new Date()` | Use Temporal's deterministic `workflow.now()` |
| `setTimeout()` or `setInterval()` | Use `await workflow.sleep('2h')` or `condition()` |
| Direct `fetch()`, `axios`, or DB queries | Put all I/O and network requests inside **Activities** |
| Mutable global state across workflow runs | Keep variables local to the workflow function scope |
| Non-deterministic thread race conditions | Use deterministic promise combinators (`Promise.all()`, `condition()`) |

---

## License

MIT © [Reactory](https://github.com/reactorynet)
