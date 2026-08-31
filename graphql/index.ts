import TemporalSchema from './schema';
import TemporalResolvers from './resolvers';

const ReactoryTemporalGraphDefinition: Reactory.Graph.IGraphDefinitions = {
  Types: [TemporalSchema],
  Resolvers: TemporalResolvers,
  Plugins: [],
};

export default ReactoryTemporalGraphDefinition;
