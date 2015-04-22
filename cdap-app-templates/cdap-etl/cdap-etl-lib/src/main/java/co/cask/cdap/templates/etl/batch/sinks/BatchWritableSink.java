/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.batch.sinks;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSinkContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import com.google.common.base.Preconditions;

/**
 * Sink for CDAP Datasets that are batch writable, which means they can be used as output of a
 * mapreduce job. User is responsible for providing any necessary dataset properties.
 *
 * @param <IN> the type of input object to the sink
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public class BatchWritableSink<IN, KEY_OUT, VAL_OUT> extends BatchSink<IN, KEY_OUT, VAL_OUT> {
  protected static final String NAME = "name";
  protected static final String TYPE = "type";

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("BatchWritableSink");
    configurer.setDescription("Sink for CDAP Datasets that are batch writable." +
                                " The name and type of dataset are required properties." +
                                " Any properties required by the desired dataset type must also be provided.");
    configurer.addProperty(new Property(NAME, "Name of the dataset. If the dataset does not already exist," +
      " one will be created.", true));
    configurer.addProperty(new Property(TYPE, "The type of batch writable dataset to use.", true));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    String datasetName = stageConfig.getProperties().get(NAME);
    Preconditions.checkArgument(datasetName != null && !datasetName.isEmpty(), "Dataset name must be given.");
    String datasetType = getDatasetType(stageConfig);
    Preconditions.checkArgument(datasetType != null && !datasetType.isEmpty(), "Dataset type must be given.");

    pipelineConfigurer.createDataset(datasetName, datasetType, DatasetProperties.builder()
      .addAll(stageConfig.getProperties())
      .build());
  }

  // this is a separate method so that it can be overriden by classes that extend this one.
  protected String getDatasetType(ETLStage stageConfig) {
    return stageConfig.getProperties().get(TYPE);
  }

  @Override
  public void prepareJob(BatchSinkContext context) {
    context.setOutput(context.getRuntimeArguments().get(NAME));
  }
}
