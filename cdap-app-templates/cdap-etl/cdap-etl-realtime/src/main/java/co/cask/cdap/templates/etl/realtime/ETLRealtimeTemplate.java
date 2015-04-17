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

package co.cask.cdap.templates.etl.realtime;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSink;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultPipelineConfigurer;
import co.cask.cdap.templates.etl.common.DefaultStageConfigurer;
import co.cask.cdap.templates.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.templates.etl.realtime.sinks.NoOpSink;
import co.cask.cdap.templates.etl.realtime.sources.TestSource;
import co.cask.cdap.templates.etl.transforms.IdentityTransform;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.util.List;
import java.util.Map;

/**
 * ETL Realtime Template.
 */
public class ETLRealtimeTemplate extends ApplicationTemplate<ETLRealtimeConfig> {
  public static final String STATE_TABLE = "etlrealtimesourcestate";
  private static final Gson GSON = new Gson();
  private final Map<String, String> sourceClassMap;
  private final Map<String, String> sinkClassMap;
  private final Map<String, String> transformClassMap;
  private RealtimeSource source;
  private RealtimeSink sink;
  private List<Transform> transforms;

  public ETLRealtimeTemplate() throws Exception {
    sourceClassMap = Maps.newHashMap();
    sinkClassMap = Maps.newHashMap();
    transformClassMap = Maps.newHashMap();
    transforms = Lists.newArrayList();

    initTable(Lists.<Class>newArrayList(IdentityTransform.class,
                                        NoOpSink.class,
                                        TestSource.class));
  }

  private void initTable(List<Class> classList) throws Exception {
    for (Class klass : classList) {
      DefaultStageConfigurer configurer = new DefaultStageConfigurer(klass);
      if (RealtimeSource.class.isAssignableFrom(klass)) {
        RealtimeSource source = (RealtimeSource) klass.newInstance();
        source.configure(configurer);
        sourceClassMap.put(configurer.createSpecification().getName(), configurer.createSpecification().getClassName());
      } else if (RealtimeSink.class.isAssignableFrom(klass)) {
        RealtimeSink sink = (RealtimeSink) klass.newInstance();
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

  @Override
  public void configureAdapter(String adapterName, ETLRealtimeConfig etlConfig, AdapterConfigurer configurer)
    throws Exception {
    int instances = etlConfig.getInstances();

    ETLStage sourceConfig = etlConfig.getSource();
    ETLStage sinkConfig = etlConfig.getSink();
    List<ETLStage> transformConfigs = etlConfig.getTransforms();

    // Instantiate Source, Transforms, Sink stages.
    instantiateStages(sourceConfig, sinkConfig, transformConfigs);

    // TODO: Validate Adapter

    PipelineConfigurer pipelineConfigurer = new DefaultPipelineConfigurer(configurer);
    configureSource(sourceConfig, configurer, pipelineConfigurer);
    configureSink(sinkConfig, configurer, pipelineConfigurer);
    configureTransforms(configurer);

    configurer.addRuntimeArgument(Constants.ADAPTER_NAME, adapterName);
    configurer.addRuntimeArgument(Constants.CONFIG_KEY, GSON.toJson(etlConfig));
    configurer.setInstances(instances);
  }

  private void configureSource(ETLStage sourceConfig, AdapterConfigurer configurer,
                               PipelineConfigurer pipelineConfigurer) throws Exception {
    source.configurePipeline(sourceConfig, pipelineConfigurer);
    DefaultStageConfigurer realtimeConfigurer = new DefaultStageConfigurer(source.getClass());
    StageSpecification specification = realtimeConfigurer.createSpecification();
    configurer.addRuntimeArgument(Constants.Source.SPECIFICATION, GSON.toJson(specification));
  }

  private void configureSink(ETLStage sinkConfig, AdapterConfigurer configurer,
                             PipelineConfigurer pipelineConfigurer) throws Exception {
    sink.configurePipeline(sinkConfig, pipelineConfigurer);
    DefaultStageConfigurer realtimeConfigurer = new DefaultStageConfigurer(sink.getClass());
    StageSpecification specification = realtimeConfigurer.createSpecification();
    configurer.addRuntimeArgument(Constants.Sink.SPECIFICATION, GSON.toJson(specification));
  }

  private void configureTransforms(AdapterConfigurer configurer) {
    List<StageSpecification> transformSpecs = Lists.newArrayList();
    for (Transform transformObj : transforms) {
      DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(transformObj.getClass());
      StageSpecification specification = stageConfigurer.createSpecification();
      transformSpecs.add(specification);
    }
    configurer.addRuntimeArgument(Constants.Transform.SPECIFICATIONS, GSON.toJson(transformSpecs));
  }

  private void instantiateStages(ETLStage sourceStage, ETLStage sinkStage, List<ETLStage> transformList)
    throws IllegalArgumentException {
    try {
      String sourceClassName = sourceClassMap.get(sourceStage.getName());
      String sinkClassName = sinkClassMap.get(sinkStage.getName());
      source = (RealtimeSource) Class.forName(sourceClassName).newInstance();
      sink = (RealtimeSink) Class.forName(sinkClassName).newInstance();

      for (ETLStage etlStage : transformList) {
        String transformName = transformClassMap.get(etlStage.getName());
        Transform transformObj = (Transform) Class.forName(transformName).newInstance();
        transforms.add(transformObj);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to load class. Check stage names. %s", e);
    }
  }

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName("etlrealtime");
    configurer.setDescription("Realtime Extract-Transform-Load (ETL) Adapter");
    configurer.addWorker(new ETLWorker());
    configurer.createDataset(STATE_TABLE, KeyValueTable.class, DatasetProperties.EMPTY);
  }
}
