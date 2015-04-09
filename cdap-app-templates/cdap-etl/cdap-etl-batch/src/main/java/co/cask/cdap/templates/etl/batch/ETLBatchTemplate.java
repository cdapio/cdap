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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.config.ETLConfig;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.batch.sinks.KVTableSink;
import co.cask.cdap.templates.etl.batch.sources.KVTableSource;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultPipelineConfigurer;
import co.cask.cdap.templates.etl.common.DefaultStageConfigurer;
import co.cask.cdap.templates.etl.transforms.IdentityTransform;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.util.List;
import java.util.Map;

/**
 * ETL Batch Template.
 */
public class ETLBatchTemplate extends ApplicationTemplate<ETLConfig> {
  private static final Gson GSON = new Gson();
  private final Map<String, String> sourceClassMap;
  private final Map<String, String> sinkClassMap;
  private final Map<String, String> transformClassMap;

  public ETLBatchTemplate() throws Exception {
    sourceClassMap = Maps.newHashMap();
    sinkClassMap = Maps.newHashMap();
    transformClassMap = Maps.newHashMap();

    //TODO: Add classes from Lib here to be available for use in the ETL Adapter. Remove this when
    //plugins management is completed.
    initTable(Lists.<Class>newArrayList(KVTableSource.class, KVTableSink.class, IdentityTransform.class));
  }

  private void initTable(List<Class> classList) throws Exception {
    for (Class klass : classList) {
      DefaultStageConfigurer configurer = new DefaultStageConfigurer(klass);
      if (BatchSource.class.isAssignableFrom(klass)) {
        BatchSource source = (BatchSource) klass.newInstance();
        source.configure(configurer);
        sourceClassMap.put(configurer.createSpecification().getName(), configurer.createSpecification().getClassName());
      } else if (BatchSink.class.isAssignableFrom(klass)) {
        BatchSink sink = (BatchSink) klass.newInstance();
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
  public void configureAdapter(String adapterName, ETLConfig etlConfig, AdapterConfigurer adapterConfigurer)
    throws Exception {
    // Get cronEntry string for ETL Batch Adapter
    String cronEntry = etlConfig.getSchedule();

    ETLStage sourceConfig = etlConfig.getSource();
    ETLStage sinkConfig = etlConfig.getSink();
    List<ETLStage> transformConfigs = etlConfig.getTransforms();

    // pipeline configurer is just a wrapper around an adapter configurer that limits what can be added,
    // since we don't want sources and sinks setting schedules or anything like that.
    PipelineConfigurer pipelineConfigurer = new DefaultPipelineConfigurer(adapterConfigurer);
    configureSource(sourceConfig, adapterConfigurer, pipelineConfigurer);
    configureSink(sinkConfig, adapterConfigurer, pipelineConfigurer);
    configureTransforms(transformConfigs, adapterConfigurer);

    //TODO: Validate if source, transforms, sink can be tied together

    adapterConfigurer.addRuntimeArgument(Constants.ADAPTER_NAME, adapterName);
    adapterConfigurer.addRuntimeArgument(Constants.CONFIG_KEY, GSON.toJson(etlConfig));
    adapterConfigurer.setSchedule(new TimeSchedule(String.format("etl.batch.adapter.%s.schedule", adapterName),
                                                   String.format("Schedule for %s Adapter", adapterName),
                                                   cronEntry));
  }

  private void configureSource(ETLStage sourceConfig, AdapterConfigurer configurer,
                               PipelineConfigurer pipelineConfigurer) throws Exception {
    String sourceName = sourceConfig.getName();
    String className = sourceClassMap.get(sourceName);
    BatchSource batchSource = (BatchSource) Class.forName(className).newInstance();
    batchSource.configurePipeline(sourceConfig, pipelineConfigurer);

    // TODO: after a few more use cases, determine if the spec is really needed at runtime
    //       since everything in the spec must be known at compile time, it seems like there shouldn't be a need
    DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(batchSource.getClass());
    StageSpecification specification = stageConfigurer.createSpecification();
    configurer.addRuntimeArgument(Constants.Source.SPECIFICATION, GSON.toJson(specification));
  }

  private void configureSink(ETLStage sinkConfig, AdapterConfigurer configurer,
                             PipelineConfigurer pipelineConfigurer) throws Exception {
    String sinkName = sinkConfig.getName();
    String className = sinkClassMap.get(sinkName);
    BatchSink batchSink = (BatchSink) Class.forName(className).newInstance();
    batchSink.configurePipeline(sinkConfig, pipelineConfigurer);

    // TODO: after a few more use cases, determine if the spec is really needed at runtime
    //       since everything in the spec must be known at compile time, it seems like there shouldn't be a need
    DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(batchSink.getClass());
    StageSpecification specification = stageConfigurer.createSpecification();
    configurer.addRuntimeArgument(Constants.Sink.SPECIFICATION, GSON.toJson(specification));
  }

  private void configureTransforms(List<ETLStage> transformConfigs, AdapterConfigurer configurer) throws Exception {
    List<StageSpecification> transformSpecs = Lists.newArrayList();
    for (ETLStage transformConfig : transformConfigs) {
      String transformName = transformConfig.getName();
      String className = transformClassMap.get(transformName);
      Transform transformObj = (Transform) Class.forName(className).newInstance();
      DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(transformObj.getClass());
      StageSpecification specification = stageConfigurer.createSpecification();
      transformSpecs.add(specification);
    }
    configurer.addRuntimeArgument(Constants.Transform.SPECIFICATIONS, GSON.toJson(transformSpecs));
  }

  @Override
  public void configure() {
    setName("etlbatch");
    setDescription("Batch Extract-Transform-Load (ETL) Adapter");
    addMapReduce(new ETLMapReduce());
    addWorkflow(new ETLWorkflow());
  }
}
