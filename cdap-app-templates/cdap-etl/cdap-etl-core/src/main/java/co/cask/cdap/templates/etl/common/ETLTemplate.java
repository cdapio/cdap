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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.TransformStage;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSink;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.common.guice.TypeResolver;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base ETL Template.
 *
 * @param <T> type of the configuration object
 */
public abstract class ETLTemplate<T> extends ApplicationTemplate<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ETLTemplate.class);
  private static final Gson GSON = new Gson();

  private final Map<String, String> sourceClassMap;
  private final Map<String, String> sinkClassMap;
  private final Map<String, String> transformClassMap;

  private Class<?> sourceClass;
  private Class<?> sinkClass;
  private List<Class<?>> transformClasses;

  protected EndPointStage source;
  protected EndPointStage sink;
  protected List<TransformStage> transforms;

  public ETLTemplate() {
    sourceClassMap = Maps.newHashMap();
    sinkClassMap = Maps.newHashMap();
    transformClassMap = Maps.newHashMap();
    transforms = Lists.newArrayList();
    transformClasses = Lists.newArrayList();
  }

  protected void initTable(List<Class> classList) throws Exception {
    for (Class klass : classList) {
      DefaultStageConfigurer configurer = new DefaultStageConfigurer(klass);
      if (RealtimeSource.class.isAssignableFrom(klass) || BatchSource.class.isAssignableFrom(klass)) {
        EndPointStage source = (EndPointStage) klass.newInstance();
        source.configure(configurer);
        sourceClassMap.put(configurer.createSpecification().getName(), configurer.createSpecification().getClassName());
      } else if (RealtimeSink.class.isAssignableFrom(klass) || BatchSink.class.isAssignableFrom(klass)) {
        EndPointStage sink = (EndPointStage) klass.newInstance();
        sink.configure(configurer);
        sinkClassMap.put(configurer.createSpecification().getName(), configurer.createSpecification().getClassName());
      } else {
        Preconditions.checkArgument(TransformStage.class.isAssignableFrom(klass));
        TransformStage transform = (TransformStage) klass.newInstance();
        transform.configure(configurer);
        transformClassMap.put(configurer.createSpecification().getName(),
                              configurer.createSpecification().getClassName());
      }
    }
  }

  protected void configure(EndPointStage stage, ETLStage stageConfig, AdapterConfigurer configurer,
                               String specKey) throws Exception {
    PipelineConfigurer pipelineConfigurer = new DefaultPipelineConfigurer(configurer);
    stage.configurePipeline(stageConfig, pipelineConfigurer);
    DefaultStageConfigurer defaultStageConfigurer = new DefaultStageConfigurer(stage.getClass());
    StageSpecification specification = defaultStageConfigurer.createSpecification();
    configurer.addRuntimeArgument(specKey, GSON.toJson(specification));
  }

  protected void configureTransforms(List<TransformStage> transformList, AdapterConfigurer configurer, String specKey) {
    List<StageSpecification> transformSpecs = Lists.newArrayList();
    for (Transform transformObj : transformList) {
      DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(transformObj.getClass());
      StageSpecification specification = stageConfigurer.createSpecification();
      transformSpecs.add(specification);
    }
    configurer.addRuntimeArgument(specKey, GSON.toJson(transformSpecs));
  }

  @Override
  public void configureAdapter(String adapterName, T config, AdapterConfigurer configurer) throws Exception {
    ETLConfig etlConfig = (ETLConfig) config;
    ETLStage sourceConfig = etlConfig.getSource();
    ETLStage sinkConfig = etlConfig.getSink();
    List<ETLStage> transformConfigs = etlConfig.getTransforms();

    // Get Source, Sink, Transform Class Names
    String sourceClassName = sourceClassMap.get(sourceConfig.getName());
    String sinkClassName = sinkClassMap.get(sinkConfig.getName());
    if (sourceClassName == null) {
      throw new IllegalArgumentException(String.format("No source named %s found.", sourceConfig.getName()));
    }
    if (sinkClassName == null) {
      throw new IllegalArgumentException(String.format("No sink named %s found.", sinkConfig.getName()));
    }

    List<String> transformClassNames = Lists.newArrayList();
    for (ETLStage transformStage : transformConfigs) {
      String transformName = transformClassMap.get(transformStage.getName());
      transformClassNames.add(transformName);
      if (transformName == null) {
        throw new IllegalArgumentException(String.format("No transform named %s found.", transformStage.getName()));
      }
    }

    // Validate Source -> Transform -> Sink hookup
    validateStages(sourceClassName, sinkClassName, transformClassNames);

    // Instantiate Source, Transforms, Sink stages.
    source = instantiateClass(sourceClass);
    sink = instantiateClass(sinkClass);
    for (Class transformClass : transformClasses) {
      TransformStage transformObj = instantiateClass(transformClass);
      transforms.add(transformObj);
    }

    configure(source, sourceConfig, configurer, Constants.Source.SPECIFICATION);
    configure(sink, sinkConfig, configurer, Constants.Sink.SPECIFICATION);
    configureTransforms(transforms, configurer, Constants.Transform.SPECIFICATIONS);
    configurer.addRuntimeArgument(Constants.ADAPTER_NAME, adapterName);

    Resources resources = etlConfig.getResources();
    if (resources != null) {
      configurer.setResources(resources);
    }
  }

  private void validateStages(String source, String sink, List<String> transforms) throws Exception {
    ArrayList<Type> unresTypeList = Lists.newArrayListWithCapacity(transforms.size() + 2);
    Type inType = Transform.class.getTypeParameters()[0];
    Type outType = Transform.class.getTypeParameters()[1];

    // Load the classes using the class names provided
    sourceClass = Class.forName(source);
    sinkClass = Class.forName(sink);
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
    for (String transform : transforms) {
      Class<?> klass = Class.forName(transform);
      transformClasses.add(klass);
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

  @SuppressWarnings("unchecked")
  private static <T> T instantiateClass(Class klass) {
    try {
      return (T) klass.newInstance();
    } catch (InstantiationException e) {
      LOG.error("Unable to instantiate {}", klass.getName(), e);
      throw Throwables.propagate(e);
    } catch (IllegalAccessException e) {
      LOG.error("Illegal access while instantiating {}", klass.getName(), e);
      throw Throwables.propagate(e);
    }
  }
}
