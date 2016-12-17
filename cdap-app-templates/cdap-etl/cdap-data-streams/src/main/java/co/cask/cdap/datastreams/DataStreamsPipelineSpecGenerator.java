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

package co.cask.cdap.datastreams;

import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.etl.common.macro.TimeParser;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.spec.PipelineSpecGenerator;

import java.util.Set;

/**
 * Generates specs for data stream pipelines.
 */
public class DataStreamsPipelineSpecGenerator
  extends PipelineSpecGenerator<DataStreamsConfig, DataStreamsPipelineSpec> {

  public DataStreamsPipelineSpecGenerator(PluginConfigurer configurer, Set<String> sourcePluginTypes,
                                          Set<String> sinkPluginTypes) {
    super(configurer, sourcePluginTypes, sinkPluginTypes, null, null);
  }

  @Override
  public DataStreamsPipelineSpec generateSpec(DataStreamsConfig config) {
    long batchIntervalMillis;
    try {
      batchIntervalMillis = TimeParser.parseDuration(config.getBatchInterval());
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse batchInterval '%s'", config.getBatchInterval()));
    }
    DataStreamsPipelineSpec.Builder specBuilder = DataStreamsPipelineSpec.builder(batchIntervalMillis)
      .setExtraJavaOpts(config.getExtraJavaOpts())
      .setStopGracefully(config.getStopGracefully());
    configureStages(config, specBuilder);
    return specBuilder.build();
  }
}
