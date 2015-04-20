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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeResolution;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

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

  protected void instantiateStages()
    throws IllegalArgumentException {
    try {
      source = (EndPointStage) sourceClass.newInstance();
      sink = (EndPointStage) sinkClass.newInstance();

      for (Class transformClass : transformClasses) {
        TransformStage transformObj = (TransformStage) transformClass.newInstance();
        transforms.add(transformObj);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to load class. Check stage names. %s", e);
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

  private List<String> retrieveClassNames(ETLStage source, ETLStage sink, List<ETLStage> transforms) {
    List<String> classNameList = Lists.newArrayList();
    String sourceClassName = sourceClassMap.get(source.getName());
    String sinkClassName = sinkClassMap.get(sink.getName());
    classNameList.add(sourceClassName);
    for (ETLStage stage : transforms) {
      classNameList.add(transformClassMap.get(stage.getName()));
    }
    classNameList.add(sinkClassName);
    return classNameList;
  }

  private void validateTypes(String source, String sink, List<String> transforms) throws Exception {
    ArrayList<Type> unresTypeList = Lists.newArrayListWithCapacity(transforms.size() + 2);
    Type inType = Transform.class.getTypeParameters()[0];
    Type outType = Transform.class.getTypeParameters()[1];

    sourceClass = Class.forName(source);
    sinkClass = Class.forName(sink);
    TypeToken sourceToken = TypeToken.of(sourceClass);
    TypeToken sinkToken = TypeToken.of(sinkClass);

    if (RealtimeSource.class.isAssignableFrom(sourceClass)) {
      Type type = RealtimeSource.class.getTypeParameters()[0];
      unresTypeList.add(sourceToken.resolveType(type).getType());
    } else {
      unresTypeList.add(sourceToken.resolveType(outType).getType());
    }

    for (String transform : transforms) {
      Class<?> klass = Class.forName(transform);
      transformClasses.add(klass);
      TypeToken transformToken = TypeToken.of(klass);
      unresTypeList.add(transformToken.resolveType(inType).getType());
      unresTypeList.add(transformToken.resolveType(outType).getType());
    }

    if (RealtimeSink.class.isAssignableFrom(sinkClass)) {
      Type type = RealtimeSink.class.getTypeParameters()[0];
      unresTypeList.add(sinkToken.resolveType(type).getType());
    } else {
      unresTypeList.add(sinkToken.resolveType(inType).getType());
    }

    validateTypes(unresTypeList);
  }

  @VisibleForTesting
  static void validateTypes(ArrayList<Type> unresTypeList) {
    List<Type> resTypeList = Lists.newArrayListWithCapacity(unresTypeList.size());
    resTypeList.add(unresTypeList.get(0));
    try {
      Type nType = (new TypeResolution()).where(unresTypeList.get(1), resTypeList.get(0)).resolveType(
        unresTypeList.get(1));
      resTypeList.add(nType);
    } catch (IllegalArgumentException e) {
      resTypeList.add(unresTypeList.get(1));
    }

    for (int i = 2; i < unresTypeList.size(); i++) {
      Type actualType = resTypeList.get(i - 1);
      Type formalType = unresTypeList.get(i - 1);
      Type toResolveType = unresTypeList.get(i);
      try {
        Type newType;
        newType = (new TypeResolution()).where(formalType, actualType).resolveType(toResolveType);
        if (newType.equals(toResolveType) || (toResolveType instanceof TypeVariable)) {
          newType = (new TypeResolution()).where(toResolveType, actualType).resolveType(toResolveType);
        }
        resTypeList.add(newType);
      } catch (IllegalArgumentException e) {
        resTypeList.add(toResolveType);
      }
    }

    for (int i = 0; i < resTypeList.size(); i += 2) {
      Type firstType = resTypeList.get(i);
      Type secondType = resTypeList.get(i + 1);
      Preconditions.checkArgument(TypeToken.of(secondType).isAssignableFrom(firstType),
                                  "Types between stages didn't match. Mismatch between {} -> {}",
                                  firstType, secondType);
    }
  }

  @Override
  public void configureAdapter(String adapterName, T config, AdapterConfigurer configurer)
    throws Exception {
    ETLConfig etlConfig = (ETLConfig) config;
    ETLStage sourceConfig = etlConfig.getSource();
    ETLStage sinkConfig = etlConfig.getSink();
    List<ETLStage> transformConfigs = etlConfig.getTransforms();

    List<String> classNames = retrieveClassNames(sourceConfig, sinkConfig, transformConfigs);
    String sourceClassName = classNames.remove(0);
    String sinkClassName = classNames.remove(classNames.size() - 1);

    // Validate type matching
    validateTypes(sourceClassName, sinkClassName, classNames);

    // Instantiate Source, Transforms, Sink stages.
    instantiateStages();

    configure(source, sourceConfig, configurer, Constants.Source.SPECIFICATION);
    configure(sink, sinkConfig, configurer, Constants.Sink.SPECIFICATION);
    configureTransforms(transforms, configurer, Constants.Transform.SPECIFICATIONS);
    configurer.addRuntimeArgument(Constants.ADAPTER_NAME, adapterName);

    Resources resources = etlConfig.getResources();
    if (resources != null) {
      configurer.setResources(resources);
    }
  }
}
