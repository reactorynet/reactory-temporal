import { Connection, Client, WorkflowHandle } from '@temporalio/client';
import { ITemporalConnectionConfig, ITemporalStartWorkflowInput, ITemporalSignalWorkflowInput, ITemporalQueryWorkflowInput } from '../types';

export class TemporalClientService {
  name: string = 'TemporalClientService';
  nameSpace: string = 'reactory';
  version: string = '1.0.0';

  private client: Client | null = null;
  private connection: Connection | null = null;
  private context: any = null;

  constructor(props?: any, context?: any) {
    this.context = context;
  }

  /**
   * Initializes or returns the cached Temporal Client
   */
  async getClient(config?: ITemporalConnectionConfig): Promise<Client> {
    if (this.client) return this.client;

    const address = config?.address || process.env.TEMPORAL_ADDRESS || 'localhost:7233';
    const namespace = config?.namespace || process.env.TEMPORAL_NAMESPACE || 'default';
    const apiKey = config?.apiKey || process.env.TEMPORAL_API_KEY;

    this.connection = await Connection.connect({
      address,
      apiKey,
      tls: config?.tls ? {
        clientCertPair: config.tls.clientCertPath && config.tls.clientKeyPath ? {
          crt: Buffer.from(config.tls.clientCertPath),
          key: Buffer.from(config.tls.clientKeyPath),
        } : undefined,
        serverRootCACertificate: config.tls.serverRootCACertPath ? Buffer.from(config.tls.serverRootCACertPath) : undefined,
        serverNameOverride: config.tls.serverNameOverride,
      } : undefined,
    });

    this.client = new Client({
      connection: this.connection,
      namespace,
    });

    return this.client;
  }

  /**
   * Starts a workflow in Temporal
   */
  async startWorkflow(input: ITemporalStartWorkflowInput, config?: ITemporalConnectionConfig): Promise<{ workflowId: string; runId: string }> {
    const client = await this.getClient(config);
    const handle = await client.workflow.start(input.workflowType, {
      workflowId: input.workflowId,
      taskQueue: input.taskQueue,
      args: input.args || [],
      searchAttributes: input.searchAttributes,
      memo: input.memo,
      cronSchedule: input.cronSchedule,
    });

    return {
      workflowId: handle.workflowId,
      runId: handle.firstExecutionRunId,
    };
  }

  /**
   * Sends a Signal to a running Temporal workflow
   */
  async signalWorkflow(input: ITemporalSignalWorkflowInput, config?: ITemporalConnectionConfig): Promise<void> {
    const client = await this.getClient(config);
    const handle: WorkflowHandle = client.workflow.getHandle(input.workflowId, input.runId);
    await handle.signal(input.signalName, ...(input.args || []));
  }

  /**
   * Queries a running or completed Temporal workflow
   */
  async queryWorkflow<T = any>(input: ITemporalQueryWorkflowInput, config?: ITemporalConnectionConfig): Promise<T> {
    const client = await this.getClient(config);
    const handle: WorkflowHandle = client.workflow.getHandle(input.workflowId, input.runId);
    return await handle.query(input.queryType, ...(input.args || []));
  }

  /**
   * Cancels a running workflow execution
   */
  async cancelWorkflow(workflowId: string, runId?: string, config?: ITemporalConnectionConfig): Promise<void> {
    const client = await this.getClient(config);
    const handle = client.workflow.getHandle(workflowId, runId);
    await handle.cancel();
  }

  /**
   * Terminates a running workflow execution
   */
  async terminateWorkflow(workflowId: string, reason?: string, runId?: string, config?: ITemporalConnectionConfig): Promise<void> {
    const client = await this.getClient(config);
    const handle = client.workflow.getHandle(workflowId, runId);
    await handle.terminate(reason);
  }
}

export const TemporalClientServiceDefinition: Reactory.Service.IReactoryServiceDefinition = {
  id: 'reactory.TemporalClientService@1.0.0',
  name: 'TemporalClientService',
  nameSpace: 'reactory',
  version: '1.0.0',
  service: TemporalClientService,
  dependencies: [],
};
