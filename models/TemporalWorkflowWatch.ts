/**
 * TemporalWorkflowWatch — the durable record of "a Reactory workflow instance is
 * waiting for this Temporal run to settle".
 *
 * The record exists because a single in-process `handle.result()` listener is not a
 * delivery guarantee:
 *
 *   • the server can restart mid-run, losing every in-memory listener — the sweeper
 *     re-attaches from these records and reconciles with describe();
 *   • the workflow engine DISCARDS an event that has no matching subscription at the
 *     moment it is processed (an unmatched event is marked processed and dropped), so
 *     a publish that lands before the awaiting step suspends is lost — the outcome is
 *     persisted here first and re-published until the waiting pointer consumes it.
 *
 * Lifecycle: pending → settled → acked, or pending → timed_out, or settled →
 * abandoned (after `maxAttempts` failed publications).
 */

import mongoose, { Schema, Document } from 'mongoose';
import Reactory from '@reactorynet/reactory-core';
import { TemporalWatchStatus } from '../types';

export interface TemporalWorkflowWatchDocument extends Document {
  /** Temporal execution being watched. */
  workflowId: string;
  runId?: string;
  /** Which cluster/namespace to re-attach to after a restart. */
  namespace?: string;
  address?: string;
  connectionKey?: string;
  /** What to publish into the workflow engine when this run settles. */
  eventName: string;
  eventKey: string;
  /** Engine target — used to verify the event was actually consumed. */
  instanceId?: string;
  stepId?: string;
  /** Tenant + actor context for the publish and for audit. */
  clientKey?: string;
  userEmail?: string;
  status: TemporalWatchStatus;
  /** The terminal outcome, recorded BEFORE the first publish attempt. */
  outcome?: {
    status: string;
    result?: any;
    failure?: { message?: string; type?: string; cause?: string };
    truncated?: boolean;
    bytes?: number;
  };
  /** Publication accounting. */
  attempts: number;
  lastAttemptAt?: Date;
  lastError?: string;
  /** Cap on result bytes carried in the published payload. */
  maxResultBytes?: number;
  /** When the bridge gives up waiting and publishes AWAIT_TIMEOUT. */
  awaitDeadline?: Date;
  createdAt: Date;
  updatedAt: Date;
  settledAt?: Date;
  ackedAt?: Date;
}

const TemporalWorkflowWatchSchema = new Schema<TemporalWorkflowWatchDocument>(
  {
    workflowId: { type: String, required: true, index: true },
    runId: { type: String },
    namespace: { type: String },
    address: { type: String },
    connectionKey: { type: String },
    eventName: { type: String, required: true },
    eventKey: { type: String, required: true, index: true },
    instanceId: { type: String, index: true },
    stepId: { type: String },
    clientKey: { type: String, index: true },
    userEmail: { type: String },
    status: {
      type: String,
      required: true,
      enum: ['pending', 'settled', 'acked', 'timed_out', 'abandoned'],
      default: 'pending',
      index: true,
    },
    outcome: {
      status: { type: String },
      result: { type: Schema.Types.Mixed },
      failure: {
        message: { type: String },
        type: { type: String },
        cause: { type: String },
      },
      truncated: { type: Boolean },
      bytes: { type: Number },
    },
    attempts: { type: Number, default: 0 },
    lastAttemptAt: { type: Date },
    lastError: { type: String },
    maxResultBytes: { type: Number },
    awaitDeadline: { type: Date },
    settledAt: { type: Date },
    ackedAt: { type: Date },
  },
  { timestamps: true },
);

// One watch per (run, topic, tenant): a step that re-executes after an engine retry
// must update its existing watch rather than create a second one that would publish
// the same outcome twice.
TemporalWorkflowWatchSchema.index(
  { workflowId: 1, eventKey: 1, eventName: 1, clientKey: 1 },
  { unique: true, name: 'temporal_watch_unique_target' },
);

// The sweeper's hot query: everything still needing attention, oldest first.
TemporalWorkflowWatchSchema.index({ status: 1, updatedAt: 1 }, { name: 'temporal_watch_sweep' });

// Acked watches are audit trail only — expire them after 7 days so the collection
// does not grow without bound. (Mongo's TTL monitor keys off ackedAt.)
TemporalWorkflowWatchSchema.index(
  { ackedAt: 1 },
  { expireAfterSeconds: 7 * 24 * 60 * 60, name: 'temporal_watch_ttl' },
);

const TemporalWorkflowWatchModelName = 'TemporalWorkflowWatch';

// Reuse an already-compiled model rather than re-registering: the model is imported
// from the services, the routes and the GraphQL resolvers, and a second
// mongoose.model() call for the same name throws OverwriteModelError.
const TemporalWorkflowWatchModel =
  (mongoose.models[TemporalWorkflowWatchModelName] as mongoose.Model<TemporalWorkflowWatchDocument>) ||
  mongoose.model<TemporalWorkflowWatchDocument>(
    TemporalWorkflowWatchModelName,
    TemporalWorkflowWatchSchema,
    'temporal_workflow_watches',
  );

export const TemporalWorkflowWatchModelComponentRegistryEntry: Reactory.IReactoryComponentDefinition<
  typeof TemporalWorkflowWatchModel
> = {
  name: 'TemporalWorkflowWatchModel',
  nameSpace: 'reactory',
  description: 'Durable record of a Reactory workflow instance awaiting a Temporal execution',
  version: '1.0.0',
  component: TemporalWorkflowWatchModel,
};

export default TemporalWorkflowWatchModel;
