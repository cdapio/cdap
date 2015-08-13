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
import co.cask.cdap.template.etl.api.realtime.RealtimeSink;
import co.cask.cdap.template.etl.api.realtime.RealtimeSource;
import co.cask.cdap.template.etl.common.guice.TypeResolver;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
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
      if (transformObj == null) {
        throw new IllegalArgumentException(String.format("No Plugin of type '%s' named '%s' was found",
                                                         Constants.Transform.PLUGINTYPE, transformConfig.getName()));
      }

      transformIds.add(transformId);
      transforms.add(transformObj);
    }

    // Validate Source -> Transform -> Sink hookup
    validateStages(source, sink, transforms);

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

  private void validateStages(PipelineConfigurable source, PipelineConfigurable sink, List<Transformation> transforms)
    throws Exception {
    ArrayList<Type> unresTypeList = Lists.newArrayListWithCapacity(transforms.size() + 2);
    Type inType = Transformation.class.getTypeParameters()[0];
    Type outType = Transformation.class.getTypeParameters()[1];

    // Load the classes using the class names provided
    Class<?> sourceClass = source.getClass();
    Class<?> sinkClass = sink.getClass();
    TypeToken sourceToken = TypeToken.of(sourceClass);
    TypeToken sinkToken = TypeToken.of(sinkClass);

    // Extract the source's output type
    if (RealtimeSource.class.isAssignableFrom(sourceClass)) {
      Type type = RealtimeSource.class.getTypeParameters()[0];
      unresTypeList.add(sourceToken.resolveType(type).getType());
    } else {
      unresTypeList.add(sourceToken.resolveType(outType).getType());
    }

    // Extract the transforms' input and output type
    for (Transformation transform : transforms) {
      Class<?> klass = transform.getClass();
      TypeToken transformToken = TypeToken.of(klass);
      unresTypeList.add(transformToken.resolveType(inType).getType());
      unresTypeList.add(transformToken.resolveType(outType).getType());
    }

    // Extract the sink's input type
    if (RealtimeSink.class.isAssignableFrom(sinkClass)) {
      Type type = RealtimeSink.class.getTypeParameters()[0];
      unresTypeList.add(sinkToken.resolveType(type).getType());
    } else {
      unresTypeList.add(sinkToken.resolveType(inType).getType());
    }

    // Invoke validation method with list of unresolved types
    validateTypes(unresTypeList);
  }

  /**
   * Takes in an unresolved type list and resolves the types and verifies if the types are assignable.
   * Ex: An unresolved type could be : String, T, List<T>, List<String>
   *     The above will resolve to   : String, String, List<String>, List<String>
   *     And the assignability will be checked : String --> String && List<String> --> List<String>
   *     which is true in the case above.
   */
  @VisibleForTesting
  static void validateTypes(ArrayList<Type> unresTypeList) {
    Preconditions.checkArgument(unresTypeList.size() % 2 == 0, "ETL Stages validation expects even number of types");
    List<Type> resTypeList = Lists.newArrayListWithCapacity(unresTypeList.size());

    // Add the source output to resolved type list as the first resolved type.
    resTypeList.add(unresTypeList.get(0));
    try {
      // Resolve the second type using just the first resolved type.
      Type nType = (new TypeResolver()).where(unresTypeList.get(1), resTypeList.get(0)).resolveType(
        unresTypeList.get(1));
      resTypeList.add(nType);
    } catch (IllegalArgumentException e) {
      // If unable to resolve type, add the second type as is, to the resolved list.
      resTypeList.add(unresTypeList.get(1));
    }

    for (int i = 2; i < unresTypeList.size(); i++) {
      // ActualType is previous resolved type; FormalType is previous unresolved type;
      // ToResolveType is current unresolved type;
      // Ex: Actual = String; Formal = T; ToResolve = List<T>;  ==> newType = List<String>
      Type actualType = resTypeList.get(i - 1);
      Type formalType = unresTypeList.get(i - 1);
      Type toResolveType = unresTypeList.get(i);
      try {
        Type newType;
        // If the toResolveType is a TypeVariable or a Generic Array, then try to resolve
        // using just the previous resolved type.
        // Ex: Actual = List<String> ; Formal = List<T> ; ToResolve = T ==> newType = String which is not correct;
        // newType should be List<String>. Hence resolve only from the previous resolved type (Actual)
        if ((toResolveType instanceof TypeVariable) || (toResolveType instanceof GenericArrayType)) {
          newType = (new TypeResolver()).where(toResolveType, actualType).resolveType(toResolveType);
        } else {
          newType = (new TypeResolver()).where(formalType, actualType).resolveType(toResolveType);
        }
        resTypeList.add(newType);
      } catch (IllegalArgumentException e) {
        // If resolution failed, add the type as is to the resolved list.
        resTypeList.add(toResolveType);
      }
    }

    // Check isAssignable on the resolved list for every paired elements. 0 --> 1 | 2 --> 3 | 4 --> 5 where | is a
    // transform (which takes in type on its left and emits the type on its right).
    for (int i = 0; i < resTypeList.size(); i += 2) {
      Type firstType = resTypeList.get(i);
      Type secondType = resTypeList.get(i + 1);
      // Check if secondType can accept firstType
      Preconditions.checkArgument(TypeToken.of(secondType).isAssignableFrom(firstType),
                                  "Types between stages didn't match. Mismatch between {} -> {}",
                                  firstType, secondType);
    }
  }
}
