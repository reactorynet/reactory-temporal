import cli from './cli';
import forms from './forms';
import models from './models';
import graph from './graphql';
import routes from './routes';
import services from './services';
import translations from './i18n';
import workflows from './workflows';
import { workflowSteps } from './workflow';

const ReactoryTemporal: Reactory.Server.IReactoryModule = {
  id: 'reactory-temporal',
  nameSpace: 'reactory',
  name: 'Temporal',
  version: '1.0.0',
  description: 'Reactory Temporal.io Integration Module - Durable Workflow Execution Bridge',
  dependencies: ['core.ReactoryServer@1.0.0'],
  priority: 3,
  graphDefinitions: graph,
  /**
   * Engine workflows this module registers (the bridge bootstrap). Temporal-side
   * workflow code under ./workflows/workflows.ts runs on a Temporal worker and is
   * deliberately not registered here.
   */
  workflows,
  /**
   * Temporal step types (temporal_workflow, temporal_await, temporal_signal, …)
   * registered into the shared YAML step registry at startup, which is what makes
   * Temporal invokable by `type:` from any Reactory YAML workflow.
   */
  workflowSteps,
  forms,
  services,
  translations,
  models,
  clientPlugins: [],
  pdfs: [],
  passportProviders: [],
  grpc: null,
  routes,
  cli,
};

export default ReactoryTemporal;
