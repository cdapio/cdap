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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.etl.batch.BatchPipelineSpec;
import co.cask.cdap.etl.proto.v2.ETLRealtimeConfig;
import co.cask.cdap.etl.spec.PipelineSpec;
import co.cask.cdap.etl.spec.PipelineSpecGenerator;

import java.util.Set;

/**
 * Generates a pipeline spec for realtime apps.
 */
public class RealtimePipelineSpecGenerator extends PipelineSpecGenerator<ETLRealtimeConfig, PipelineSpec> {

  public RealtimePipelineSpecGenerator(PluginConfigurer configurer,
                                       Set<String> sourcePluginTypes,
                                       Set<String> sinkPluginTypes,
                                       Class<? extends Dataset> errorDatasetClass,
                                       DatasetProperties errorDatasetProperties) {
    super(configurer, sourcePluginTypes, sinkPluginTypes, errorDatasetClass, errorDatasetProperties);
  }

  @Override
  public PipelineSpec generateSpec(ETLRealtimeConfig config) {
    PipelineSpec.Builder specBuilder = PipelineSpec.builder();
    configureStages(config, specBuilder);
    return specBuilder.build();
  }
}
