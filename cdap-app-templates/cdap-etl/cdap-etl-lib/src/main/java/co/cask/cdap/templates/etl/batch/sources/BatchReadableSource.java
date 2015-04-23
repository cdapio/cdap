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
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Source for CDAP BatchReadable Datasets. Users must provide dataset name and type plus any properties
 * that type may need. Should be used only by advanced users.
 *
 * @param <KEY_IN> the type of input key from the Batch job
 * @param <VAL_IN> the type of input value from the Batch job
 * @param <OUT> the type of output for the
 */
public class BatchReadableSource<KEY_IN, VAL_IN, OUT> extends BatchSource<KEY_IN, VAL_IN, OUT> {
  protected static final String NAME = "name";
  private static final String TYPE = "type";

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(getClass().getSimpleName());
    configurer.setDescription(
      "CDAP BatchReadable Batch Source for advanced users. The dataset name and type must be given as properties, " +
        "along with any type specific properties that the dataset may need. If the dataset does not already exist, " +
        "it will be created by the pipeline.");
    configurer.addProperty(new Property(NAME, "Dataset Name", true));
    configurer.addProperty(new Property(TYPE, "Dataset Type", true));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    String name = stageConfig.getProperties().get(NAME);
    Preconditions.checkArgument(name != null && !name.isEmpty(), "Dataset name must be given.");
    String type = getType(stageConfig);
    Preconditions.checkArgument(type != null && !type.isEmpty(), "Dataset type must be given.");

    Map<String, String> properties = Maps.newHashMap(stageConfig.getProperties());
    properties.remove(NAME);
    properties.remove(TYPE);
    pipelineConfigurer.createDataset(name, type, DatasetProperties.builder().addAll(properties).build());
  }

  // a separate method so that subclasses can override
  protected String getType(ETLStage stageConfig) {
    return stageConfig.getProperties().get(TYPE);
  }

  @Override
  public void prepareJob(BatchSourceContext context) {
    PluginProperties pluginProperties = context.getPluginProperties();
    context.setInput(pluginProperties.getProperties().get(NAME));
  }
}
