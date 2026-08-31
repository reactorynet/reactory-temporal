import cli from './cli';
import forms from './forms';
import models from './models';
import graph from './graphql';
import routes from './routes';
import services from './services';
import translations from './i18n';
import workflows from './workflow';

const ReactoryTemporal: Reactory.Server.IReactoryModule = {
  id: 'reactory-temporal',
  nameSpace: 'reactory',
  name: 'Temporal',
  version: '1.0.0',
  description: 'Reactory Temporal.io Integration Module - Durable Workflow Execution Bridge',
  dependencies: ['core.ReactoryServer@1.0.0'],
  priority: 3,
  graphDefinitions: graph,
  workflows,
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
