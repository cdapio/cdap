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

import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSink;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

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

  protected EndPointStage source;
  protected EndPointStage sink;
  protected List<Transform> transforms;

  public ETLTemplate() {
    sourceClassMap = Maps.newHashMap();
    sinkClassMap = Maps.newHashMap();
    transformClassMap = Maps.newHashMap();
    transforms = Lists.newArrayList();
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
        Preconditions.checkArgument(Transform.class.isAssignableFrom(klass));
        Transform transform = (Transform) klass.newInstance();
        transform.configure(configurer);
        transformClassMap.put(configurer.createSpecification().getName(),
                              configurer.createSpecification().getClassName());
      }
    }
  }

  protected void instantiateStages(ETLStage sourceStage, ETLStage sinkStage, List<ETLStage> transformList)
    throws IllegalArgumentException {
    try {
      String sourceClassName = sourceClassMap.get(sourceStage.getName());
      String sinkClassName = sinkClassMap.get(sinkStage.getName());
      source = (EndPointStage) Class.forName(sourceClassName).newInstance();
      sink = (EndPointStage) Class.forName(sinkClassName).newInstance();

      for (ETLStage etlStage : transformList) {
        String transformName = transformClassMap.get(etlStage.getName());
        Transform transformObj = (Transform) Class.forName(transformName).newInstance();
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

  protected void configureTransforms(List<Transform> transformList, AdapterConfigurer configurer, String specKey) {
    List<StageSpecification> transformSpecs = Lists.newArrayList();
    for (Transform transformObj : transformList) {
      DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(transformObj.getClass());
      StageSpecification specification = stageConfigurer.createSpecification();
      transformSpecs.add(specification);
    }
    configurer.addRuntimeArgument(specKey, GSON.toJson(transformSpecs));
  }

  @Override
  public void configureAdapter(String adapterName, T config, AdapterConfigurer configurer)
    throws Exception {
    ETLConfig etlConfig = (ETLConfig) config;
    ETLStage sourceConfig = etlConfig.getSource();
    ETLStage sinkConfig = etlConfig.getSink();
    List<ETLStage> transformConfigs = etlConfig.getTransforms();

    // Instantiate Source, Transforms, Sink stages.
    instantiateStages(sourceConfig, sinkConfig, transformConfigs);

    // TODO: Validate Adapter by making sure the key-value types of stages match.

    configure(source, sourceConfig, configurer, Constants.Source.SPECIFICATION);
    configure(sink, sinkConfig, configurer, Constants.Sink.SPECIFICATION);
    configureTransforms(transforms, configurer, Constants.Transform.SPECIFICATIONS);
    configurer.addRuntimeArgument(Constants.ADAPTER_NAME, adapterName);
  }
}
