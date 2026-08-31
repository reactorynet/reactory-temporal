import { Worker, NativeConnection } from '@temporalio/worker';
import * as activities from './activities';

export async function runTemporalWorker(options?: { taskQueue?: string; address?: string; namespace?: string }): Promise<void> {
  const taskQueue = options?.taskQueue || process.env.TEMPORAL_TASK_QUEUE || 'b2b-payouts-queue';
  const address = options?.address || process.env.TEMPORAL_ADDRESS || 'localhost:7233';
  const namespace = options?.namespace || process.env.TEMPORAL_NAMESPACE || 'default';

  console.log(`🔌 Connecting Temporal Worker to ${address} (namespace: ${namespace})...`);

  const connection = await NativeConnection.connect({
    address,
  });

  const worker = await Worker.create({
    connection,
    namespace,
    taskQueue,
    workflowsPath: require.resolve('./workflows'),
    activities,
  });

  console.log(`🚀 Temporal Worker successfully started on task queue: [${taskQueue}]`);
  console.log(`⏳ Listening for B2BMassPayoutWorkflow tasks... (Press Ctrl+C to stop)`);

  await worker.run();
}

// If invoked directly from command line (ts-node / node)
if (require.main === module) {
  runTemporalWorker().catch((err) => {
    console.error('❌ Temporal Worker fatal error:', err);
    process.exit(1);
  });
}
