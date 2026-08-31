const TemporalConfigForm: Reactory.Forms.IReactoryForm = {
  id: 'reactory-temporal.TemporalConfigForm@1.0.0',
  name: 'TemporalConfigForm',
  nameSpace: 'reactory-temporal',
  version: '1.0.0',
  title: 'Temporal Connection Configuration',
  description: 'Configure cluster endpoints, namespaces, and TLS credentials for Temporal.io integration.',
  schema: {
    type: 'object',
    required: ['address', 'namespace'],
    properties: {
      address: {
        type: 'string',
        title: 'Temporal Host / Address',
        description: 'Host and port of the Temporal frontend service (e.g. localhost:7233 or temporal.corp:7233)',
        default: 'localhost:7233',
      },
      namespace: {
        type: 'string',
        title: 'Namespace',
        description: 'Target Temporal namespace for workflow execution',
        default: 'default',
      },
      apiKey: {
        type: 'string',
        title: 'API Key (Temporal Cloud)',
        description: 'Optional API Key for authenticating with Temporal Cloud',
      },
      useTls: {
        type: 'boolean',
        title: 'Enable mTLS Encryption',
        default: false,
      },
      clientCertPath: {
        type: 'string',
        title: 'Client Certificate Path / PEM',
        description: 'Path or raw PEM string of client certificate',
      },
      clientKeyPath: {
        type: 'string',
        title: 'Client Private Key Path / PEM',
        description: 'Path or raw PEM string of client private key',
      },
    },
  },
  uiSchema: {
    apiKey: {
      'ui:widget': 'password',
    },
    clientKeyPath: {
      'ui:widget': 'password',
    },
  },
};

export default TemporalConfigForm;
