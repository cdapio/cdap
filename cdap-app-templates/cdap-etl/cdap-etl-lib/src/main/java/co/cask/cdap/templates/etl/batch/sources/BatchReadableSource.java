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

package co.cask.cdap.templates.etl.batch.sources;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.Properties;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * An abstract source for CDAP BatchReadable Datasets. Extending classes must provide implementation for
 * {@link BatchReadableSource#getType(ETLStage)} which should return the type of dataset used by the source
 *
 * @param <KEY_IN> the type of input key from the Batch job
 * @param <VAL_IN> the type of input value from the Batch job
 * @param <OUT> the type of output for the
 */
public abstract class BatchReadableSource<KEY_IN, VAL_IN, OUT> extends BatchSource<KEY_IN, VAL_IN, OUT> {

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    String name = stageConfig.getProperties().get(Properties.BatchReadableWritable.NAME);
    Preconditions.checkArgument(name != null && !name.isEmpty(), "Dataset name must be given.");
    String type = getType(stageConfig);
    Preconditions.checkArgument(type != null && !type.isEmpty(), "Dataset type must be given.");

    Map<String, String> properties = Maps.newHashMap(stageConfig.getProperties());
    properties.remove(Properties.BatchReadableWritable.NAME);
    properties.remove(Properties.BatchReadableWritable.TYPE);
    pipelineConfigurer.createDataset(name, type, DatasetProperties.builder().addAll(properties).build());
  }

  /**
   * An abstract method which the subclass should override to provide their types dataset types
   */
  protected abstract String getType(ETLStage stageConfig);

  @Override
  public void prepareJob(BatchSourceContext context) {
    PluginProperties pluginProperties = context.getPluginProperties();
    context.setInput(pluginProperties.getProperties().get(Properties.BatchReadableWritable.NAME));
  }
}
