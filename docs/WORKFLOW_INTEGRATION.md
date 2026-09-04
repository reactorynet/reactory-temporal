# Temporal ⇄ Reactory Workflow Engine Integration — Design

Status: **implemented** (branch `feature/temporal-workflow-integration` / `feature/workflow-step-integration`) · Author: W. Weber (with Claude) · Date: 2026-09-04
Scope: `reactory-temporal` module + a small, generic change to the core YAML workflow engine.

> **Implementation notes.** Phases 0–4 are built and tested (76 tests). Two things were
> found during implementation and fixed beyond the original design:
>
> * `ServiceManager` activates services by **calling** `definition.service(props, context)`,
>   never with `new`. The module registered bare classes, so `getService('reactory.TemporalClientService@1.0.0')`
>   threw `Class constructor cannot be invoked without 'new'` — nothing in the module was
>   reachable at runtime. Both service definitions are now factories.
> * Instances are started under `partner.key` as their engine tenant (`WorkflowRunner.ts:911`),
>   but `publishEvent` published under the engine's *default* tenant, and subscription
>   matching is strictly tenant-scoped. **No tenant instance could ever be woken by an
>   event.** `WorkflowRunner.publishEvent` and `ReactoryWorkflowService.publishWorkflowEvent`
>   now take an optional `tenantId` (defaulting to the context partner),
>   `signalWorkflowInstance` publishes under the instance's own tenant, and
>   `IWorkflowHistoryItem` carries `tenantId` so it can.

Goal: make Temporal durable executions **first-class, invokable steps** in Reactory YAML
workflows, and let a YAML instance **suspend on, and be woken by, Temporal signals and
completions** — with no polling threads held open across multi-day human-in-the-loop gates.

---

## 1. How the Reactory workflow engine executes steps (as-built)

Two execution paths sit over **one** step registry:

| | Path A — standalone | Path B — durable |
|---|---|---|
| Entry | `YamlWorkflowExecutor.executeWorkflow()` | `YamlFlowBuilder.buildYamlWorkflowClass()` → `workflow-es` host |
| File | `reactory-core/workflow/YamlFlow/execution/YamlWorkflowExecutor.ts` | `reactory-core/workflow/YamlFlow/YamlFlowBuilder.ts` + `execution/YamlStepBody.ts` |
| Control flow | topological `dependsOn` order, sequential | engine combinators (`if`/`while`/`foreach`/`parallel`/`saga`/`waitFor`) |
| State | in-process `variables` + `stepOutputs` | `YamlWorkflowData` (TData), persisted per step boundary |
| Suspend / resume | **not supported** | supported (`wait_event`, sleep, retry, compensation) |
| Used by | CLI, tests, ad-hoc invocation | server runtime (`WorkflowRunner`) |

Facts that constrain the design:

1. **Module steps are leaves.** `WorkflowRunner.discoverModuleSteps()` (`WorkflowRunner.ts:364`)
   reads `module.workflowSteps` and registers each `IWorkflowStepProvider` into the shared
   `YamlStepRegistry`, optionally with an `IWorkflowStepDesignerDefinition` surfaced to the
   Visual Designer via `workflowStepCatalog`. Both execution paths resolve through that registry,
   so **one class written against `BaseYamlStep` runs in both** — the pattern
   `reactory-reactor` uses for `agent_conversation` / `graph_query`.
2. **Structural (suspend-capable) step types are hardcoded in core.** `YamlFlowBuilder`'s
   `STRUCTURAL_TYPES` = `condition, for_each, while, parallel, join, saga, wait_event`. A module
   cannot contribute a suspending step today: `YamlStepBody.run()` always returns
   `ExecutionResult.next()`. (This is also why `user_activity`'s `requiresUserInput: true`
   is silently ignored.)
3. **Suspend primitive.** `wait_event` → `WaitForEventStepBody` → `ExecutionResult.waitForEvent(eventName, eventKey, effectiveDate)`.
   Wake path: `WorkflowRunner.publishEvent()` → exposed as
   `core.ReactoryWorkflowService@1.0.0.publishWorkflowEvent(eventName, eventKey, data)` and
   `signalWorkflowInstance(instanceId, data, stepId)` (`ReactoryWorkflowService.ts:1156`).
   On resume, `workflow-es` **re-runs the same step body** with `pointer.eventPublished = true`
   and `pointer.eventData` set (`event-queue-worker.ts:206`).
4. **A durable sleep also exists but is unused by YAML leaves**: `ExecutionResult.sleep(until, persistenceData)`,
   with `context.persistenceData` restored on the re-run (`workflow-executor.ts:95`). This enables
   a *durable* poll loop that survives restarts and holds no resources.
5. **Step outputs must be JSON-serializable.** `YamlStepBody` runs `toSerializable()` because a
   non-serializable output breaks instance persistence and re-runs the step forever. Temporal
   payloads must be normalised before they land in `outputs`.
6. **Step types are Ajv-validated against a hardcoded enum** in
   `reactory-core/workflow/YamlFlow/schema/WorkflowSchema.json` (line ~127). New module step
   types **must** be added there or every YAML using them fails to parse.

### 1.1 Two engine hazards this design must work around

**H1 — lost wakeup (critical).** `event-queue-worker.processEvent()` matches subscriptions at
*event-processing* time; if no subscription matches, the loop is empty, `success` stays `true`, and
the event is `markEventProcessed()` — **consumed and discarded**. An event published *before* the
waiting step suspends is gone. `poll-worker` only re-queues events that are `!isProcessed`, so
there is no replay. A Temporal workflow that finishes in 200 ms would strand the YAML instance.

**H2 — `wait_event`'s `timeout` is not a timeout.** The builder sets
`effectiveDate = now + timeout`, and matching requires `subscription.subscribeAsOf <= event.eventTime`
(`memory-persistence-provider.ts:73`). So `timeout` *delays event eligibility* rather than aborting
the wait. Temporal awaits must not rely on it; the timeout must be delivered as an event
(see §5.4) or handled with a durable sleep.

---

## 2. Current state of `reactory-temporal`

Working: `TemporalClientService` (connect / start / signal / query / cancel / terminate),
GraphQL query+mutation surface, config form, CLI (`ping`, `describe`, `worker`, `test-batch`),
and a complete sample B2B payout workflow + worker + activities.

Gaps:

| # | Gap | Evidence |
|---|---|---|
| G1 | **Nothing is reachable from the workflow engine.** The four macros are assigned to `module.workflows`, but `getDefaultWorkflows()` (`WorkflowRunner.ts:172`) discards any entry without `category: 'workflow'` — they are logged as *"bad shape"* and dropped. `module.workflowSteps` is absent. | `reactory-temporal/index.ts`, `workflow/index.ts` |
| G2 | **The documented YAML syntax does not exist.** README §4.1 uses `macro: temporalStartWorkflow`; the engine has no `macro:` step form — steps are `type:` only, and the Ajv schema would reject it. | `README.md` §4.1 |
| G3 | **No inbound path.** `routes/index.ts` → `[]`, `models/index.ts` → `[]`. Nothing bridges a Temporal completion or signal back to `publishEvent`. | module source |
| G4 | **Client service is not multi-tenant or TLS-correct.** `getClient()` caches the first client and ignores every later `config` (so a per-tenant `connectionConfig` silently uses the first tenant's connection); TLS uses `Buffer.from(config.tls.clientCertPath)` — the *path string* is passed as the certificate rather than the file contents; there is no `close()`. `searchAttributes` is deprecated in the installed `@temporalio/client` **1.23.0** (declared `^1.11.7`) in favour of `typedSearchAttributes`. | `services/TemporalClientService.ts:20-60` |

---

## 3. Design overview

```
  YAML workflow (durable path)                            Temporal cluster
  ────────────────────────────                            ────────────────
  ┌────────────────────────┐   start (idempotent id)   ┌──────────────────────┐
  │ temporal_workflow      │ ────────────────────────► │ B2BMassPayoutWorkflow│
  │  awaitMode: event      │                           │  ...activities...    │
  │  → control.waitForEvent│ ◄── suspends, 0 threads   │  await condition()   │
  └────────────────────────┘                           └──────────┬───────────┘
        ▲                                                         │ result / signal
        │ publishWorkflowEvent(temporal.workflow.completed, wfId)  │
        │                                                         ▼
  ┌─────┴───────────────────────────┐  handle.result()   ┌──────────────────────┐
  │ TemporalEventBridgeService      │ ◄───────────────── │ in-process watcher   │
  │  + TemporalWorkflowWatch model  │                    └──────────────────────┘
  │  + republish sweeper (H1)       │ ◄───────────────── POST /api/temporal/v1/events
  └─────────────────────────────────┘                    (external workers / activities)
```

Three layers:

* **Layer A — steps** (module only): `temporal_workflow`, `temporal_signal`, `temporal_query`,
  `temporal_update`, `temporal_cancel`, `temporal_terminate`, `temporal_describe`, `temporal_await`.
* **Layer B — inbound events** (module only): watcher service + persisted watch model + REST
  callback route + a Temporal-side helper activity.
* **Layer C — one generic core change**: a **step control directive** so any registry step can ask
  the durable engine to suspend on an event or sleep. Approved in review; without it, awaits can
  only be blocking in-step polls that restart from scratch after a server restart.

---

## 4. Layer C — the step control directive (core change)

### 4.1 Contract

Declared server-side in `reactory-core/workflow/YamlFlow/steps/interfaces/IYamlStep.ts` (which is
already the extension point — it re-exports the published `Reactory.Workflow.*` types). This avoids
a `@reactorynet/reactory-core` package rebuild + redeploy; the fields are upstreamed into
`reactory-core/src/types/workflow/index.d.ts` **and** the monolithic `src/types/index.d.ts` in a
follow-up (the server consumes the packaged `1.1.9` typings from `node_modules`).

```ts
/** A step's request for durable engine control after it returns. */
export interface StepControlDirective {
  /** Suspend until (eventName, eventKey) is published. */
  waitForEvent?: { eventName: string; eventKey: string };
  /** Suspend for a duration, then re-run this same step. */
  sleep?: { durationMs?: number; until?: string /* ISO-8601 */ };
  /** Opaque state restored as context.control.persistenceData on the re-run. Sleep only. */
  persist?: Record<string, any>;
}

export type StepExecutionResult =
  Reactory.Workflow.IYamlStepExecutionResult & { control?: StepControlDirective };

/** Added to the step execution context. */
export interface StepControlContext {
  /** True when the durable engine can honour a directive (Path B only). */
  supportsSuspend: boolean;
  /** True on the run that follows a satisfied waitForEvent. */
  eventPublished: boolean;
  /** Payload delivered with the event. */
  eventData?: any;
  /** State from a previous directive's `persist`. */
  persistenceData?: any;
}
```

`waitForEvent` carries **no** `persist`: `ExecutionResult.waitForEvent()` does not set
`persistenceData` (see `models/execution-result.ts`). Steps that need resumption state across an
event suspend must write it into their own `outputs` — `YamlStepBody` records
`data.stepResults[stepId]` into TData *before* returning the directive, so the re-run reads it back
from `context.stepResults[this.id].outputs`. This is specified behaviour, not incidental.

### 4.2 `YamlStepBody` changes (Path B)

```
run(context):
  ...unchanged setup...
  stepCtx.control = {
    supportsSuspend: true,
    eventPublished: !!context.pointer?.eventPublished,
    eventData: context.pointer?.eventData,
    persistenceData: context.persistenceData,
  }
  result = await step.execute(stepCtx)
  record data.stepResults[def.id] / data.outputs[def.id]   // ALWAYS, before any suspend
  if (result.success && result.control?.waitForEvent && !stepCtx.control.eventPublished)
      return ExecutionResult.waitForEvent(name, key, new Date())   // effectiveDate = now, see H2
  if (result.success && result.control?.sleep)
      return ExecutionResult.sleep(until, { ...result.control.persist, __stepId: def.id })
  ...unchanged success/failure handling...
```

Rules:

* `effectiveDate` is always **now** — never `now + timeout` (H2). Timeouts are events (§5.4).
* A directive on a **failed** result is ignored; failure handling wins.
* A directive is ignored when `eventPublished` is already true, so a resumed step cannot re-suspend
  on the same event (no infinite suspend loop).
* Backwards compatible: a step that never sets `control` behaves exactly as today.

### 4.3 `YamlWorkflowExecutor` changes (Path A)

Path A cannot suspend. It sets `control.supportsSuspend = false` and, if a step returns a directive
anyway, fails that step with a clear message
(`Step "x" requested waitForEvent but this executor does not support suspension — use awaitMode: sleep_poll or run on the durable engine`).
Steps are expected to check `supportsSuspend` and degrade themselves (the temporal steps do — §5.1).

### 4.4 Bonus

`user_activity` can then return `control.waitForEvent({ eventName: 'workflow.task.completed', eventKey: taskId })`
and finally behave as its own docblock claims. Out of scope here; noted as a follow-on.

---

## 5. Layer A — step catalog

All steps extend `BaseYamlStep`, live in `reactory-temporal/workflow/steps/`, resolve every string
config value through `resolveTemplate()` (so `${input.x}`, `${variables.y}`, `${steps.a.outputs.b}`
all work), and are registered via `module.workflowSteps` with an `IWorkflowStepDesignerDefinition`.

Shared config (all steps):

| Key | Type | Notes |
|---|---|---|
| `connection` | object | `{ address, namespace, apiKey, tls }` — per-step override |
| `connectionKey` | string | Named connection from module config (preferred over inline secrets) |
| `namespace` | string | Shorthand for `connection.namespace` |

### 5.1 `temporal_workflow` — start (and optionally await) an execution

| Key | Type | Default | Notes |
|---|---|---|---|
| `workflowType` | string | — | **required**; Temporal workflow name |
| `workflowId` | string | — | **required**; templatable. **Must be deterministic** (see §7) |
| `taskQueue` | string | `env.TEMPORAL_TASK_QUEUE` | templatable |
| `args` | any[] | `[]` | deep-templated |
| `awaitMode` | enum | `none` | `none` \| `event` \| `sleep_poll` \| `block_poll` |
| `awaitTimeout` | number (ms) | `86400000` | delivered as a timeout **event**, not `effectiveDate` (H2) |
| `pollInterval` | number (ms) | `10000` | `sleep_poll` / `block_poll` |
| `eventName` | string | `temporal.workflow.settled` | override for a custom bridge topic (§5.3) |
| `eventKey` | string | `${workflowId}` | correlation key; see §5.3 |
| `signalOnStart` | object | — | `{ name, args }` → `client.workflow.signalWithStart` |
| `idConflictPolicy` | enum | `USE_EXISTING` | `USE_EXISTING` \| `FAIL` \| `TERMINATE_EXISTING` |
| `idReusePolicy` | enum | — | passed through when set (mutually exclusive with `TERMINATE_IF_RUNNING` per the 1.23 API) |
| `memo` / `searchAttributes` / `typedSearchAttributes` | object | — | `typedSearchAttributes` preferred (G4) |
| `cronSchedule` | string | — | pass-through |
| `workflowExecutionTimeout` / `workflowRunTimeout` / `workflowTaskTimeout` | string \| number | — | pass-through |
| `failOnWorkflowFailure` | boolean | `true` | when `false`, a failed Temporal run yields `success: true` with `status: FAILED` so the YAML can branch |
| `resultPath` | string | — | lodash path into the Temporal result to place in `outputs.result` (keeps TData small) |

Outputs: `{ workflowId, runId, firstExecutionRunId, status, started, reattached, awaited, result?, failure?, historyLength? }`
— `status` mirrors `TemporalWorkflowExecutionStatus` from the module's GraphQL enum.

Behaviour by `awaitMode`:

* **`none`** — start, return. The YAML author may pair it with a core `wait_event` step
  (`eventName: temporal.workflow.settled`, `eventKey: "steps.start.outputs.workflowId"`). Works
  today with zero core changes; H1 applies unless the bridge sweeper (§5.4) is deployed.
* **`event`** *(recommended for long runs)* — first run: resolve/start the execution, register a
  bridge watch, then **`describe()` before suspending** (closes the fast-completion window of H1);
  if already terminal, return immediately, else return
  `control.waitForEvent({ eventName, eventKey })`. Resume run: read `context.control.eventData`,
  normalise, return outputs.
* **`sleep_poll`** *(recommended for short runs, and the zero-infrastructure option)* — first run:
  start, then `control.sleep({ durationMs: pollInterval, persist: { workflowId, runId, deadline } })`.
  Each re-run `describe()`s; terminal → outputs; else sleep again; past `deadline` → timeout failure.
  **No lost-wakeup race, no bridge, restart-safe.**
* **`block_poll`** — in-step `await`/poll loop (the `invoke_workflow` shape). The only mode available
  on Path A; not restart-safe. Chosen automatically when `control.supportsSuspend === false` and
  `awaitMode` was `event`/`sleep_poll`, with a warning logged.

### 5.2 The remaining steps

| Step type | Purpose | Key config | Outputs |
|---|---|---|---|
| `temporal_signal` | Send a signal to a running execution | `workflowId`, `runId?`, `signalName`, `args`, `startIfNotRunning?` (→ `signalWithStart`) | `{ workflowId, runId, signalled: true }` |
| `temporal_query` | Read in-flight state | `workflowId`, `runId?`, `queryType`, `args`, `resultPath?` | `{ result }` |
| `temporal_update` | Synchronous update (1.23 `executeUpdate`) | `workflowId`, `updateName`, `args`, `waitForStage?` | `{ result, updateId }` |
| `temporal_cancel` | Graceful cancel | `workflowId`, `runId?` | `{ cancelled: true }` |
| `temporal_terminate` | Hard terminate | `workflowId`, `runId?`, `reason` | `{ terminated: true }` |
| `temporal_describe` | Status / history metadata | `workflowId`, `runId?` | `{ status, workflowType, taskQueue, startTime, closeTime, historyLength }` |
| `temporal_await` | Await a run started elsewhere (e.g. by a GraphQL mutation) | `workflowId`, `runId?`, `awaitMode`, `awaitTimeout`, `pollInterval` | same await outputs as `temporal_workflow` |

`temporal_signal` is what closes the human-in-the-loop loop in the other direction: a Reactory
`user_activity` / form approval publishes into the YAML instance, and the next step signals
`approveBatch` on the Temporal side.

### 5.3 Event names and correlation keys

| Event name | Published when | `eventKey` | Payload |
|---|---|---|---|
| `temporal.workflow.completed` | run reaches `COMPLETED` | `<workflowId>` | `{ workflowId, runId, status: 'COMPLETED', result }` |
| `temporal.workflow.failed` | `FAILED` / `TERMINATED` / `TIMED_OUT` | `<workflowId>` | `{ workflowId, runId, status, failure: { message, type, cause? } }` |
| `temporal.workflow.cancelled` | `CANCELED` | `<workflowId>` | `{ workflowId, runId, status: 'CANCELED' }` |
| `temporal.workflow.timeout` | bridge sweeper passes a watch's `awaitTimeout` | `<workflowId>` | `{ workflowId, status: 'AWAIT_TIMEOUT', waitedMs }` |
| `temporal.workflow.progress` | optional; from an activity callback | `<workflowId>` | author-defined |
| `temporal.signal.<name>` | external signal routed into a YAML instance | author-defined | author-defined |

`eventKey` is the **Temporal `workflowId`**, not `runId` — the id is deterministic and known to the
waiting step before start, and it survives continue-as-new (where `runId` changes).

**One subscription cannot cover four outcomes**: `workflow-es` matches a single
`(eventName, eventKey)` pair per execution pointer, so a step cannot subscribe to `completed`,
`failed`, `cancelled` and `timeout` at once. The bridge therefore publishes a **collapsed topic,
`temporal.workflow.settled`**, for *every* terminal outcome with `status` in the payload — that is
the default `eventName` for `awaitMode: event` and `temporal_await`. The per-outcome names in the
table are published *in addition*, for observability and for authors who write an explicit
`wait_event` step against one specific outcome.

### 5.4 `TemporalEventBridgeService`

`reactory.TemporalEventBridgeService@1.0.0`, singleton.

```ts
watch(input: {
  workflowId: string; runId?: string;
  connectionKey?: string; namespace?: string;
  eventName?: string;            // default 'temporal.workflow.settled'
  eventKey?: string;             // default workflowId
  awaitTimeout?: number;         // ms
  instanceId?: string;           // engine instance, for ack verification
  stepId?: string;
  clientKey?: string;            // tenant, for the publish context
}): Promise<{ watchId: string }>

publish(eventName: string, eventKey: string, data: any): Promise<void>  // → core workflow service
sweep(): Promise<{ reattached: number; republished: number; timedOut: number; acked: number }>
unwatch(watchId: string): Promise<void>
```

**Watch model** — `TemporalWorkflowWatch` (new mongoose model; `models/index.ts` is empty today):

| Field | Type | Notes |
|---|---|---|
| `workflowId`, `runId` | string | `workflowId` + `clientKey` unique index |
| `namespace`, `connectionKey` | string | which cluster/tenant to re-attach to |
| `eventName`, `eventKey` | string | what to publish |
| `instanceId`, `stepId` | string | engine target, used for ack verification |
| `clientKey`, `userEmail` | string | context for the publish + audit |
| `status` | enum | `pending` → `settled` → `acked`; or `timed_out`, `abandoned` |
| `outcome` | object | `{ status, result?, failure? }`, JSON-normalised |
| `attempts`, `lastAttemptAt`, `lastError` | number/date/string | republish accounting |
| `awaitDeadline` | date | drives `temporal.workflow.timeout` |
| `createdAt`, `settledAt`, `ackedAt` | date | TTL index on `ackedAt` (e.g. 7 days) |

**At-least-once publication (mitigates H1).** On settle, the outcome is written to the watch
*before* publishing, then published. `status` becomes `acked` only once the target instance no
longer has an unconsumed subscription for `(eventName, eventKey)` — read from
`core.ReactoryWorkflowService.getWorkflowHistoryById(instanceId).executionPointers`
(`p.eventName && !p.eventPublished`), the same predicate `signalWorkflowInstance` uses. A watch in
`settled` is **re-published on every sweep** until acked, with exponential backoff and an
`abandoned` terminal state after N attempts. Repeated publishes are harmless: `seedSubscription`
sets `eventPublished` once, and a publish with no subscription is simply dropped.

**Restart re-attach + sweeper trigger.** The module contributes an auto-start code workflow
(`Reactory.Workflow.IWorkflow` with `category: 'workflow'`, `autoStart: true`,
`props.interval: 15000`) — the pattern `CleanCacheWorkflow` uses and the only boot hook
`IReactoryModule` offers (`startAutoStartWorkflows()`, `WorkflowRunner.ts:414`). Each tick calls
`sweep()`, which:

1. re-attaches `handle.result()` watchers for `pending` watches with no live in-process watcher
   (i.e. after a server restart), reconciling immediately via `describe()`;
2. re-publishes `settled` unacked watches;
3. publishes `temporal.workflow.timeout` for `pending` watches past `awaitDeadline`.

### 5.5 REST callback route

`POST /api/temporal/v1/events` — mounted from `routes/index.ts` behind
`context.auth.authenticatePartner` (the `reactory-communicator` webhook pattern):

```json
{ "eventName": "temporal.workflow.settled",
  "eventKey": "batch_2026_001",
  "data": { "status": "COMPLETED", "result": { "totalSettled": 42 } },
  "workflowId": "batch_2026_001", "runId": "…" }
```

`200 { accepted: true, watchAcked: boolean }`. The handler validates the event name against an
allowlist (`temporal.*` only), records the outcome on the matching watch if one exists (so the
sweeper's ack/retry logic still applies), and publishes. This is the path for **externally hosted
Temporal workers** that Reactory did not start and cannot watch in-process.

Companion Temporal-side helper in `workflows/activities.ts`:
`notifyReactoryActivity(eventName, eventKey, data)` — a plain activity that POSTs to the route, so
authors can notify Reactory from mid-workflow (progress, approval-needed) rather than only at
completion.

### 5.6 Designer + schema registration

* One `IWorkflowStepDesignerDefinition` per step type: `category: 'integration'`, `icon: 'schedule'`
  / `'bolt'` / `'cancel'`, colour `#5c6bc0`, `previous`/`next` control ports plus a typed result
  port, `propertySchema` + `uiSchema` (select widgets for `awaitMode` / policies), and a
  `rendering.webgl.circuit` block (`labelPrefix: 'TMP'`) matching the reactor steps' circuit theme.
* **Add every new step type to `reactory-core/workflow/YamlFlow/schema/WorkflowSchema.json`'s
  `type` enum** — Ajv rejects unknown types at parse time (§1.6). The enum's own comment concedes
  it must be hand-synced; a follow-up could generate it from the live registry.

---

## 6. Execution walkthroughs

### 6.1 Fire-and-forget with an explicit wait (no core change needed)

```yaml
nameSpace: reactory-temporal
name: DispatchBatchPayout
version: 1.0.0
inputs:
  batchId: { type: string, required: true }
steps:
  - id: startPayout
    type: temporal_workflow
    config:
      workflowType: B2BMassPayoutWorkflow
      workflowId: "batch_${input.batchId}"
      taskQueue: b2b-payouts-queue
      awaitMode: none
      args:
        - batchId: "${input.batchId}"
          accountId: "${variables.accountId}"
          currency: GBP
          rows: "${variables.validatedRows}"
  - id: awaitSettled
    type: wait_event
    dependsOn: startPayout
    config:
      eventName: temporal.workflow.settled
      eventKey: "steps.startPayout.outputs.workflowId"
      outputVariable: payoutOutcome
  - id: report
    type: log
    dependsOn: awaitSettled
    config:
      message: "batch ${input.batchId} → ${payoutOutcome}"
```

### 6.2 One durable step (Layer C)

```yaml
  - id: payout
    type: temporal_workflow
    config:
      workflowType: B2BMassPayoutWorkflow
      workflowId: "batch_${input.batchId}"
      taskQueue: b2b-payouts-queue
      awaitMode: event          # suspends; woken by the bridge
      awaitTimeout: 604800000   # 7 days, delivered as temporal.workflow.timeout
      resultPath: totalSettled
```

Sequence: run 1 starts the execution (or re-attaches via `USE_EXISTING`), registers the watch,
`describe()`s, suspends → engine persists the instance and frees the worker. Temporal runs for days.
On completion the in-process watcher (or the callback route) settles the watch and publishes
`temporal.workflow.settled` keyed on `batch_<id>`. The engine seeds the pointer and re-runs the step,
which reads `context.control.eventData` and returns `{ status: 'COMPLETED', result: 42 }`.

### 6.3 Human-in-the-loop, both directions

Reactory owns the UI gate; Temporal owns the money. `user_activity` (or a form mutation) →
`temporal_signal { signalName: approveBatch, args: ["${variables.approverId}"] }` → the Temporal
`await condition(() => isApproved)` releases → completion event wakes the YAML instance's
`temporal_workflow`/`temporal_await` step.

### 6.4 Restart mid-await

Server dies while suspended. On boot: `WorkflowRunner` re-registers definitions, `host.start()`
resumes runnable instances (the suspended pointer stays subscribed), and the bridge's auto-start
sweeper re-attaches `handle.result()` for every `pending` watch and reconciles with `describe()` —
so a completion that happened *while Reactory was down* is published on the first sweep, not lost.

---

## 7. Idempotency and determinism rules

1. **`workflowId` must be deterministic** — derived from workflow inputs (`batch_${input.batchId}`),
   never `Date.now()`/uuid. Engine retries, `continueOnError`, and post-restart re-runs all
   re-execute the step; a deterministic id plus `idConflictPolicy: USE_EXISTING` makes the start
   idempotent (`reattached: true` in outputs). `validateConfig` **warns** (never fails) when
   `workflowId` contains no `${...}` reference — a static id is legitimate for a singleton
   workflow, so hard-failing there would be wrong.
2. **Normalise Temporal payloads before they enter `outputs`** — `toSerializable()` protects TData,
   but a 10k-row result would bloat every subsequent persist. Use `resultPath`, and cap stored
   results (default 64 KB; overflow → `{ truncated: true, bytes }` plus a pointer to the Temporal
   run for full detail).
3. **Never resolve a Temporal *client* inside a Temporal *workflow*** — the module's sample
   workflows are determinism-constrained (README §7); all Reactory callbacks go through activities.
4. **Tenant scoping** — watches, publishes, and connections all carry `clientKey`; the engine's
   event subscriptions are tenant-scoped (`event-queue-worker.ts:170`), so a cross-tenant publish
   simply will not match.

---

## 8. Change list

### `reactory-express-server/src/modules/reactory-temporal` (own git repo)

```
workflow/
  index.ts                     ← REWRITE: export workflowSteps (providers + designer defs)
  steps/
    BaseTemporalStep.ts        ← shared: client resolution, template helpers, status mapping
    TemporalWorkflowStep.ts
    TemporalAwaitStep.ts
    TemporalSignalStep.ts
    TemporalQueryStep.ts
    TemporalUpdateStep.ts
    TemporalCancelStep.ts
    TemporalTerminateStep.ts
    TemporalDescribeStep.ts
    designer/definitions.ts    ← IWorkflowStepDesignerDefinition per step
  TemporalStepMacros.ts        ← DELETE (dead; superseded by steps) or keep for the reactor macro registry
services/
  TemporalEventBridgeService.ts ← NEW
  TemporalClientService.ts      ← FIX G4: per-config client cache, real TLS file reads, close(), typedSearchAttributes
models/
  TemporalWorkflowWatch.ts      ← NEW mongoose model + index.ts registration
routes/
  index.ts                      ← NEW: POST /api/temporal/v1/events (authenticatePartner)
workflows/
  TemporalWatchSweeperWorkflow.ts ← NEW auto-start code workflow (interval 15s → bridge.sweep())
  activities.ts                 ← ADD notifyReactoryActivity
graphql/
  schema/Temporal.graphql       ← ADD temporalPublishWorkflowEvent mutation + watch queries
examples/
  DispatchBatchPayout.yaml      ← end-to-end example (§6.2)
docs/
  WORKFLOW_INTEGRATION.md       ← this document
index.ts                        ← ADD workflowSteps; fix `workflows` (sweeper only, category: 'workflow')
README.md                       ← FIX §4.1 (macro → type), document the steps and events
```

### `reactory-express-server/src/modules/reactory-core` (core)

```
workflow/YamlFlow/steps/interfaces/IYamlStep.ts   ← StepControlDirective / StepControlContext
workflow/YamlFlow/execution/YamlStepBody.ts       ← honour control (waitForEvent / sleep)
workflow/YamlFlow/execution/YamlWorkflowExecutor.ts ← supportsSuspend: false + clear failure
workflow/YamlFlow/schema/WorkflowSchema.json      ← add temporal_* to the step type enum
```

### `reactory-core` (types package, follow-up)

`src/types/workflow/index.d.ts` **and** `src/types/index.d.ts` — add `control?` to
`IYamlStepExecutionResult` and `control?` to `IStepExecutionContext`; rebuild + bump (server pins
`@reactorynet/reactory-core@1.1.9`). Until then the server-local types in `IYamlStep.ts` carry it,
with a documented cast at the `step.execute()` call site in `YamlStepBody`.

---

## 9. Failure modes

| Failure | Mitigation |
|---|---|
| Temporal completes before the step suspends (H1) | `describe()` pre-check before suspending; watch persisted before publish; sweeper re-publishes until acked |
| Reactory down when Temporal completes | Watch is `pending` in Mongo; sweeper re-attaches and reconciles on boot (§6.4) |
| Event published, instance never resumes | Ack verification reads `executionPointers`; unacked watches re-publish with backoff, then `abandoned` + error log |
| Engine retry re-starts the Temporal workflow | Deterministic `workflowId` + `idConflictPolicy: USE_EXISTING`; `WorkflowExecutionAlreadyStartedError` caught and treated as re-attach |
| Await never ends | `awaitDeadline` → `temporal.workflow.timeout` published on the awaited topic with `status: AWAIT_TIMEOUT`; step fails cleanly |
| Huge Temporal result bloats TData | `resultPath` + 64 KB cap + truncation marker (§7.2) |
| Temporal cluster unreachable at step time | Step fails with the connection error; per-step `retryPolicy` in YAML maps to engine retry |
| Wrong tenant's cluster used | Fix G4 (per-config client cache); `connectionKey` resolved against module config, `clientKey` recorded on the watch |
| Callback route abused to wake arbitrary instances | Partner auth + `temporal.*` event-name allowlist + watch-existence check before publish |

---

## 10. Test plan

**Unit (module, `__tests__`)** — config validation per step; template resolution into `args`;
`awaitMode: event` first-run returns a `waitForEvent` directive; resume run reads `eventData`;
`sleep_poll` returns a sleep directive and terminates on a terminal `describe()`;
`supportsSuspend: false` degrades to `block_poll`; already-started run reports `reattached: true`.

**Unit (core)** — `YamlStepBody` returns `waitForEvent`/`sleep` for a directive, records
`stepResults` *before* suspending, ignores directives on failure and on a resumed pointer;
`YamlWorkflowExecutor` fails clearly on a directive. Mirror the existing
`YamlFlow/__tests__/YamlFlowBuilder.test.ts` in-memory host harness (it already exercises
`host.publishEvent`).

**Integration (in-memory engine + mocked Temporal client)** — the §6.2 workflow suspends, a
simulated bridge publish resumes it, outputs assert the result; a publish *before* suspension is
recovered by the sweeper (H1 regression test); a restart (fresh host, same persistence) re-attaches.

**End-to-end (podman Temporal, manual/CI-optional)** — extend `workflows/clientTest.ts`: real
`B2BMassPayoutWorkflow`, YAML instance suspends at the approval gate, `temporal_signal` approves,
completion wakes the YAML instance.

Note per project convention: server jest needs `--maxWorkers=1`.

---

## 11. Phasing

| Phase | Contents | Status |
|---|---|---|
| **0** | Fix G4 (client service), fix the service-registration factory bug, fix README §4.1, drop the dead macro wiring | ✅ done |
| **1** | `temporal_workflow` (`none`/`block_poll`), `temporal_signal`, `temporal_query`, `temporal_describe`, `temporal_cancel`, `temporal_terminate` + designer defs + schema enum + `workflowSteps` | ✅ done |
| **2** | Layer C control directive (core) + `awaitMode: event`/`sleep_poll` + `temporal_await` + the tenant-scoped publish fix | ✅ done |
| **3** | Bridge service, watch model, bootstrap workflow + sweep timer, callback route, `notifyReactoryActivity` | ✅ done |
| **4** | `temporal_update`, GraphQL watch surface, example YAML, e2e test, README/docs | ✅ done |

### Test coverage as built

| Suite | Tests | Covers |
|---|---|---|
| `reactory-core/.../execution/StepControlDirective.test.ts` | 6 | The core contract on a real workflow-es host: suspend + resume with payload, outputs recorded before the suspend, no re-suspend on resume, durable sleep with persisted state, failure beating a directive, and the standalone executor's clear refusal |
| `reactory-temporal/workflow/steps/__tests__/TemporalSteps.test.ts` | 33 | Every step's config validation, template resolution, all four await modes, idempotent re-attach, degradation paths, failure policy, result caps |
| `reactory-temporal/services/__tests__/TemporalEventBridgeService.test.ts` | 13 | The delivery guarantee: persist-before-publish, unacked republication, ack on consumption, bounded abandonment, tenant-correct publishing, deadline timeout, external callbacks |
| `reactory-temporal/services/__tests__/TemporalClientService.test.ts` | 21 | Per-config connection cache, TLS file reads vs. inline PEM, API-key TLS, connection resolution precedence, close(), status mapping, result normalisation |
| `reactory-temporal/examples/__tests__/TemporalExampleWorkflows.test.ts` | 9 | Schema-enum registration, designer catalog, registry construction, both example YAMLs parsing and validating |
| `reactory-temporal/examples/__tests__/TemporalDurableAwait.test.ts` | 3 | End to end on the durable engine: park → bridge publish → resume with outcome; inline resolution when the run settles first; late delivery of a failure |

Run them with `NODE_OPTIONS=--max-old-space-size=8192 npx jest <path> --maxWorkers=1`.

---

## 12. Decisions taken during implementation

1. **Connection config source of truth** — resolved in this order, mirroring
   `ReactorySQLService`: explicit `connection` on the step → partner setting
   `temporal/<connectionKey>` (or `<connectionKey>`) → `TEMPORAL_<KEY>_ADDRESS|_NAMESPACE|_API_KEY`
   → conventional `TEMPORAL_*` → `localhost:7233`/`default`. No new model.
   `TemporalConfigForm` still does not write those settings — wiring the admin form to
   them is open follow-up work.
2. **Per-outcome topics** — both are published: awaiting steps subscribe to the collapsed
   `temporal.workflow.settled`, and the per-outcome topics go out alongside it for
   observability and for authors writing an explicit `wait_event` against one outcome.
3. **Sweeper cadence and ownership** — the bridge owns its own timer
   (`TEMPORAL_BRIDGE_SWEEP_INTERVAL_MS`, default 30 s, unref'd), activated once at boot by
   an auto-start code workflow. An instance per sweep tick would have created thousands
   of workflow instances a day. Multi-node safety reuses `SchedulerLockModel`: the unique
   `(scheduleId, slotTime)` insert elects one sweeper per minute, with its TTL as the
   crash backstop.
4. **`user_activity`** — deliberately left alone. It *can* now return
   `control.waitForEvent` and finally honour its own `requiresUserInput` contract, but
   changing it would alter existing task-approval behaviour, so it stays a follow-up.
5. **Result size cap** — 64 KB default, overridable per step via `maxResultBytes`.
   Overflow is dropped with `{ truncated: true, bytes }` plus a warning naming the run to
   query for the full payload; spilling to `InstanceResourceManager` remains follow-up work.

### Remaining follow-ups

* Upstream `control` into `@reactorynet/reactory-core`'s published typings
  (`src/types/workflow/index.d.ts` **and** the monolithic `src/types/index.d.ts`), then
  reduce the server-local declarations in `IYamlStep.ts` to re-exports.
* Wire `TemporalConfigForm` to write the partner settings that `connectionKey` reads.
* Generate `WorkflowSchema.json`'s step-type enum from the live registry instead of
  hand-syncing it (its own comment already concedes the problem).
* Migrate `user_activity` onto the control directive.
* Pre-existing and unrelated: `reactory-communicator`'s `routes` export is a map of
  factory functions keyed by non-path names, which `ConfigureRoutes` cannot mount.
