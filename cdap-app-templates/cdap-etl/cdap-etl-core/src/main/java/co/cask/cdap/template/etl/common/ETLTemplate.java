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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.template.etl.api.PipelineConfigurable;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.Transformation;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Base ETL Template.
 *
 * @param <T> type of the configuration object
 */
public abstract class ETLTemplate<T extends ETLConfig> extends ApplicationTemplate<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ETLTemplate.class);
  private static final Gson GSON = new Gson();

  protected void configure(PipelineConfigurable stage, AdapterConfigurer configurer, String pluginPrefix)
    throws Exception {
    PipelineConfigurer pipelineConfigurer = new AdapterPipelineConfigurer(configurer, pluginPrefix);
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
  public void configureAdapter(String adapterName, T etlConfig, AdapterConfigurer configurer) throws Exception {
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
    PipelineConfigurable source = configurer.usePlugin(Constants.Source.PLUGINTYPE, sourceConfig.getName(),
                                                       sourcePluginId, sourceProperties);
    if (source == null) {
      throw new IllegalArgumentException(String.format("No Plugin of type '%s' named '%s' was found",
                                                       Constants.Source.PLUGINTYPE, sourceConfig.getName()));
    }

    PluginProperties sinkProperties = getPluginProperties(sinkConfig);
    PipelineConfigurable sink = configurer.usePlugin(Constants.Sink.PLUGINTYPE, sinkConfig.getName(),
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
      Transform transformObj = configurer.usePlugin(Constants.Transform.PLUGINTYPE, transformConfig.getName(),
                                                    transformId, transformProperties);
      configure(transformObj, configurer, null);

      if (transformObj == null) {
        throw new IllegalArgumentException(String.format("No Plugin of type '%s' named '%s' was found",
                                                         Constants.Transform.PLUGINTYPE, transformConfig.getName()));
      }

      transformIds.add(transformId);
      transforms.add(transformObj);
    }

    // Validate Source -> Transform -> Sink hookup
    PipelineValidator.validateStages(source, sink, transforms);

    configure(source, configurer, sourcePluginId);
    configure(sink, configurer, sinkPluginId);

    configurer.addRuntimeArgument(Constants.ADAPTER_NAME, adapterName);
    configurer.addRuntimeArgument(Constants.Source.PLUGINID, sourcePluginId);
    configurer.addRuntimeArgument(Constants.Sink.PLUGINID, sinkPluginId);
    configurer.addRuntimeArgument(Constants.Transform.PLUGINIDS, GSON.toJson(transformIds));

    Resources resources = etlConfig.getResources();
    if (resources != null) {
      configurer.setResources(resources);
    }
  }

  protected String getAppName(String key) {
    Properties prop = new Properties();
    InputStream input = getClass().getResourceAsStream("/etl.properties");
    try {
      prop.load(input);
      return prop.getProperty(key);
    } catch (IOException e) {
      LOG.warn("ETL properties not read: {}", e.getMessage(), e);
      throw Throwables.propagate(e);
    } finally {
      try {
        input.close();
      } catch (Exception e) {
        LOG.warn("ETL properties not read: {}", e.getMessage(), e);
        throw Throwables.propagate(e);
      }
    }
  }
}
