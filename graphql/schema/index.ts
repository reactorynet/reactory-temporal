import { readFileSync } from 'fs';
import { join } from 'path';

const TemporalSchema = readFileSync(
  join(__dirname, 'Temporal.graphql'),
  'utf8'
);

export default TemporalSchema;
