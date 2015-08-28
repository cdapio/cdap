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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.template.etl.api.PipelineConfigurable;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.Transformation;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Base ETL Template.
 *
 * @param <T> type of the configuration object
 */
public abstract class ETLApplication<T extends ETLConfig> extends AbstractApplication<T> {

  protected void configure(PipelineConfigurable stage, ApplicationConfigurer configurer, String pluginPrefix)
    throws Exception {
    PipelineConfigurer pipelineConfigurer = new DefaultPipelineConfigurer(configurer, pluginPrefix);
    stage.configurePipeline(pipelineConfigurer);
  }

  private PluginProperties getPluginProperties(ETLStage config) {
    PluginProperties.Builder builder = PluginProperties.builder();
    if (config.getProperties() != null) {
      builder.addAll(config.getProperties());
    }
    return builder.build();
  }

  @Override
  public void configure() {
    ETLConfig etlConfig = getConfig();

    ETLStage sourceConfig = etlConfig.getSource();
    ETLStage sinkConfig = etlConfig.getSink();
    List<ETLStage> transformConfigs = etlConfig.getTransforms();
    String sourcePluginId = PluginID.from(Constants.Source.PLUGINTYPE, sourceConfig.getName(), 1).getID();
    // 2 + since we start at 1, and there is always a source.  For example, if there are 0 transforms, sink is stage 2.
    String sinkPluginId =
      PluginID.from(Constants.Sink.PLUGINTYPE,  sinkConfig.getName(), 2 + transformConfigs.size()).getID();

    // Instantiate Source, Transforms, Sink stages.
    // Use the plugin name as the plugin id for source and sink stages since there can be only one source and one sink.
    PluginProperties sourceProperties = getPluginProperties(sourceConfig);
    PipelineConfigurable source = usePlugin(Constants.Source.PLUGINTYPE, sourceConfig.getName(),
      sourcePluginId, sourceProperties);
    if (source == null) {
      throw new IllegalArgumentException(String.format("No Plugin of type '%s' named '%s' was found",
        Constants.Source.PLUGINTYPE, sourceConfig.getName()));
    }

    PluginProperties sinkProperties = getPluginProperties(sinkConfig);
    PipelineConfigurable sink = usePlugin(Constants.Sink.PLUGINTYPE, sinkConfig.getName(),
      sinkPluginId, sinkProperties);
    if (sink == null) {
      throw new IllegalArgumentException(String.format("No Plugin of type '%s' named '%s' was found",
        Constants.Sink.PLUGINTYPE, sinkConfig.getName()));
    }

    // Store transform id list to be serialized and passed to the driver program
    List<String> transformIds = Lists.newArrayListWithCapacity(transformConfigs.size());
    List<Transformation> transforms = Lists.newArrayListWithCapacity(transformConfigs.size());
    for (int i = 0; i < transformConfigs.size(); i++) {
      ETLStage transformConfig = transformConfigs.get(i);

      // Generate a transformId based on transform name and the array index (since there could
      // multiple transforms - ex, N filter transforms in the same pipeline)
      // stage number starts from 1, plus source is always #1, so add 2 for stage number.
      String transformId = PluginID.from(Constants.Transform.PLUGINTYPE, transformConfig.getName(), 2 + i).getID();
      PluginProperties transformProperties = getPluginProperties(transformConfig);
      Transform transformObj = usePlugin(Constants.Transform.PLUGINTYPE, transformConfig.getName(),
        transformId, transformProperties);
      if (transformObj == null) {
        throw new IllegalArgumentException(String.format("No Plugin of type '%s' named '%s' was found",
          Constants.Transform.PLUGINTYPE, transformConfig.getName()));
      }

      transformIds.add(transformId);
      transforms.add(transformObj);
    }

    ApplicationConfigurer configurer = getConfigurer();

    // Validate Source -> Transform -> Sink hookup
    try {
      PipelineValidator.validateStages(source, sink, transforms);
      configure(source, configurer, sourcePluginId);
      configure(sink, configurer, sinkPluginId);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
