/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Mapreduce runtime context for batch joiner
 */
public class MapReduceJoinerRuntimeContext extends MapReduceRuntimeContext implements BatchJoinerRuntimeContext {
  private final Map<String, Schema> inputSchemas;
  private final Schema outputSchema;

  public MapReduceJoinerRuntimeContext(MapReduceTaskContext context, Metrics metrics, LookupProvider lookup,
                                            String stageName, Map<String, String> runtimeArgs,
                                            Map<String, Schema> inputSchemas, Schema outputSchema) {
    super(context, metrics, lookup, stageName, runtimeArgs);
    this.inputSchemas = ImmutableMap.<String, Schema>copyOf(inputSchemas);
    this.outputSchema = outputSchema;
  }

  @Override
  public Map<String, Schema> getInputSchemas() {
    return inputSchemas;
  }

  @Override
  public Schema getOutputSchema() {
    return outputSchema;
  }
}
