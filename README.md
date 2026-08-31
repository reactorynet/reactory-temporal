# Reactory Temporal Integration Module (`reactory-temporal`)

The `reactory-temporal` module connects the Reactory application and presentation platform with the **Temporal.io** distributed execution engine.

## Key Capabilities

1. **`TemporalClientService`**:
   - Manages connection lifecycle to Temporal clusters (local, self-hosted, or Temporal Cloud).
   - Starts, signals, queries, cancels, and terminates durable workflows.
   - Supports mTLS certificate pairs and API key authentication.

2. **Workflow Bridge**:
   - Allows Reactory step workflows and forms to trigger Temporal workflows asynchronously.
   - Provides signal dispatchers for Human-in-the-Loop workflows (Maker/Checker, LOD1/LOD2 compliance reviews).

3. **GraphQL & REST APIs**:
   - Queries real-time workflow state and execution progress.
   - Exposes mutations for starting and signaling workflows from the Reactory frontend.

## Configuration

Environment variables:
- `TEMPORAL_ADDRESS`: Temporal frontend service address (default: `localhost:7233`).
- `TEMPORAL_NAMESPACE`: Temporal namespace (default: `default`).
- `TEMPORAL_API_KEY`: API key for Temporal Cloud.
- `TEMPORAL_TLS_CERT_PATH`: Optional client certificate path for mTLS.
- `TEMPORAL_TLS_KEY_PATH`: Optional client key path for mTLS.
