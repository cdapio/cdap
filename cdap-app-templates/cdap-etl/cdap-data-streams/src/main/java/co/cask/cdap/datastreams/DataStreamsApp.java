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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.spec.PipelineSpecGenerator;
import com.google.common.collect.ImmutableSet;

/**
 * Data Streams Application.
 */
public class DataStreamsApp extends AbstractApplication<DataStreamsConfig> {
  public static final String CHECKPOINT_FILESET = "dataStreamsCheckpoints";

  @Override
  public void configure() {
    DataStreamsConfig config = getConfig();
    setDescription("Data Streams Application");

    PipelineSpecGenerator<DataStreamsConfig, DataStreamsPipelineSpec> specGenerator =
      new DataStreamsPipelineSpecGenerator(
        getConfigurer(),
        ImmutableSet.of(StreamingSource.PLUGIN_TYPE),
        ImmutableSet.of(BatchSink.PLUGIN_TYPE)
      );
    DataStreamsPipelineSpec spec = specGenerator.generateSpec(config);
    addSpark(new DataStreamsSparkLauncher(spec, config));

    if (!config.checkpointsDisabled()) {
      createDataset(CHECKPOINT_FILESET, FileSet.class);
    }
  }
}
