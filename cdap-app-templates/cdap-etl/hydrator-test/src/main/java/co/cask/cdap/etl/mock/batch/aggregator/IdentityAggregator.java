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

package co.cask.cdap.etl.mock.batch.aggregator;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Identity aggregation for testing.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("Identity")
public class IdentityAggregator extends BatchAggregator<StructuredRecord, StructuredRecord, StructuredRecord> {

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
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
}
