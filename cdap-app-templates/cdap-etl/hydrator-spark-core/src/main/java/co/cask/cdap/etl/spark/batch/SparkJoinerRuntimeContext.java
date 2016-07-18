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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Joiner context in Spark.
 */
public class SparkJoinerRuntimeContext extends SparkBatchRuntimeContext implements BatchJoinerRuntimeContext {
  private final Map<String, Schema> inputSchemas;
  private final Schema outputSchema;

  public SparkJoinerRuntimeContext(PluginContext pluginContext, Metrics metrics,
                                   long logicalStartTime, Map<String, String> runtimeArgs,
                                   String stageName,
                                   Map<String, Schema> inputSchemas, Schema outputSchema) {
    super(pluginContext, metrics, logicalStartTime, runtimeArgs, stageName);
    this.inputSchemas = ImmutableMap.copyOf(inputSchemas);
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
