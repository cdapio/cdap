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

package io.cdap.cdap.etl.mock.batch.aggregator;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Identity aggregation for testing.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("Identity")
public class IdentityAggregator extends BatchAggregator<StructuredRecord, StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void groupBy(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(input);
  }

  @Override
  public void aggregate(StructuredRecord structuredRecord, Iterator<StructuredRecord> groupValues,
                        Emitter<StructuredRecord> emitter) throws Exception {
    while (groupValues.hasNext()) {
      emitter.emit(groupValues.next());
    }
  }

  public static ETLPlugin getPlugin() {
    Map<String, String> properties = new HashMap<>();
    return new ETLPlugin("Identity", BatchAggregator.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    return PluginClass.builder().setName("Identity").setType(BatchAggregator.PLUGIN_TYPE)
             .setDescription("").setClassName(IdentityAggregator.class.getName()).setProperties(properties).build();
  }
}
