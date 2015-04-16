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
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.templates.etl.batch.sinks.BatchWritableSink;
import co.cask.cdap.templates.etl.batch.sinks.KVTableSink;
import co.cask.cdap.templates.etl.batch.sinks.TableSink;
import co.cask.cdap.templates.etl.batch.sinks.TimePartitionedFileSetDatasetAvroSink;
import co.cask.cdap.templates.etl.batch.sources.BatchReadableSource;
import co.cask.cdap.templates.etl.batch.sources.KVTableSource;
import co.cask.cdap.templates.etl.batch.sources.StreamBatchSource;
import co.cask.cdap.templates.etl.batch.sources.TableSource;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultPipelineConfigurer;
import co.cask.cdap.templates.etl.common.DefaultStageConfigurer;
import co.cask.cdap.templates.etl.transforms.GenericTypeToAvroKeyTransform;
import co.cask.cdap.templates.etl.transforms.IdentityTransform;
import co.cask.cdap.templates.etl.transforms.RowToStructuredRecordTransform;
import co.cask.cdap.templates.etl.transforms.StreamToStructuredRecordTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToPutTransform;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * ETL Batch Template.
 */
public class ETLBatchTemplate extends ApplicationTemplate<ETLBatchConfig> {
  private static final Gson GSON = new Gson();
  private final Map<String, String> sourceClassMap;
  private final Map<String, String> sinkClassMap;
  private final Map<String, String> transformClassMap;
  private BatchSource batchSource;
  private BatchSink batchSink;
  private List<Transform> transforms;

  public ETLBatchTemplate() throws Exception {
    sourceClassMap = Maps.newHashMap();
    sinkClassMap = Maps.newHashMap();
    transformClassMap = Maps.newHashMap();
    transforms = Lists.newArrayList();

    //TODO: Add classes from Lib here to be available for use in the ETL Adapter. Remove this when
    //plugins management is completed.
    initTable(Lists.<Class>newArrayList(KVTableSource.class,
                                        KVTableSink.class,
                                        BatchReadableSource.class,
                                        BatchWritableSink.class,
                                        TableSource.class,
                                        TableSink.class,
                                        IdentityTransform.class,
                                        StructuredRecordToPutTransform.class,
                                        RowToStructuredRecordTransform.class,
                                        StructuredRecordToGenericRecordTransform.class,
                                        StreamBatchSource.class,
                                        TimePartitionedFileSetDatasetAvroSink.class,
                                        StreamToStructuredRecordTransform.class,
                                        GenericTypeToAvroKeyTransform.class));
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
  public void configureAdapter(String adapterName, ETLBatchConfig etlBatchConfig, AdapterConfigurer adapterConfigurer)
    throws Exception {
    // Get cronEntry string for ETL Batch Adapter
    String cronEntry = etlBatchConfig.getSchedule();

    ETLStage sourceConfig = etlBatchConfig.getSource();
    ETLStage sinkConfig = etlBatchConfig.getSink();
    List<ETLStage> transformConfigs = etlBatchConfig.getTransforms();

    // Instantiate Source, Transform, Sink Stages.
    instantiateStages(sourceConfig, sinkConfig, transformConfigs);

    // Validate Adapter by making sure the key-value types of stages match.
    validateAdapter(sourceConfig, sinkConfig, transformConfigs);

    // pipeline configurer is just a wrapper around an adapter configurer that limits what can be added,
    // since we don't want sources and sinks setting schedules or anything like that.
    PipelineConfigurer pipelineConfigurer = new DefaultPipelineConfigurer(adapterConfigurer);
    configureSource(sourceConfig, adapterConfigurer, pipelineConfigurer);
    configureSink(sinkConfig, adapterConfigurer, pipelineConfigurer);
    configureTransforms(adapterConfigurer);

    adapterConfigurer.addRuntimeArgument(Constants.ADAPTER_NAME, adapterName);
    adapterConfigurer.addRuntimeArgument(Constants.CONFIG_KEY, GSON.toJson(etlBatchConfig));
    adapterConfigurer.setSchedule(new TimeSchedule(String.format("etl.batch.adapter.%s.schedule", adapterName),
                                                   String.format("Schedule for %s Adapter", adapterName),
                                                   cronEntry));
  }

  private void instantiateStages(ETLStage source, ETLStage sink, List<ETLStage> transformList)
    throws IllegalArgumentException {
    try {
      String sourceClassName = sourceClassMap.get(source.getName());
      String sinkClassName = sinkClassMap.get(sink.getName());
      batchSource = (BatchSource) Class.forName(sourceClassName).newInstance();
      batchSink = (BatchSink) Class.forName(sinkClassName).newInstance();

      for (ETLStage etlStage : transformList) {
        String transformName = transformClassMap.get(etlStage.getName());
        Transform transformObj = (Transform) Class.forName(transformName).newInstance();
        transforms.add(transformObj);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to load class. Check stage names. %s", e);
    }
  }

  private void validateAdapter(ETLStage source, ETLStage sink, List<ETLStage> transformList)
    throws IllegalArgumentException {
    if (transformList.size() == 0) {
      // No transforms. Check only source and sink.
      if (!(isAssignable(batchSource.getKeyType(), batchSink.getKeyType()) &&
        (isAssignable(batchSource.getValueType(), batchSink.getValueType())))) {
        throw new IllegalArgumentException(String.format("Source %s and Sink %s Types don't match",
                                                         source.getName(), sink.getName()));
      }
    } else {
      // Check the first and last transform with source and sink.
      ETLStage firstStage = Iterables.getFirst(transformList, null);
      ETLStage lastStage = Iterables.getLast(transformList);
      Transform firstTransform = Iterables.getFirst(transforms, null);
      Transform lastTransform = Iterables.getLast(transforms);

      if (!(isAssignable(batchSource.getKeyType(), firstTransform.getKeyInType()) &&
        (isAssignable(batchSource.getValueType(), firstTransform.getValueInType())))) {
        throw new IllegalArgumentException(String.format("Source %s and Transform %s Types don't match",
                                                         source.getName(), firstStage.getName()));
      }

      if (!(isAssignable(lastTransform.getKeyOutType(), batchSink.getKeyType()) &&
        (isAssignable(lastTransform.getValueOutType(), batchSink.getValueType())))) {
        throw new IllegalArgumentException(String.format("Sink %s and Transform %s Types don't match",
                                                         sink.getName(), lastStage.getName()));
      }

      if (transformList.size() > 1) {
        // Check transform stages.
        validateTransforms(transformList);
      }
    }
  }

  private boolean isAssignable(Type source, Type destination) {
    //TODO: Do correct validation.
    return true;
  }

  private void validateTransforms(List<ETLStage> transformList) throws IllegalArgumentException {
    for (int i = 0; i < transformList.size() - 1; i++) {
      ETLStage currStage = transformList.get(i);
      ETLStage nextStage = transformList.get(i + 1);
      Transform firstTransform = transforms.get(i);
      Transform secondTransform = transforms.get(i + 1);

      if (!(isAssignable(firstTransform.getKeyOutType(), secondTransform.getKeyInType()) &&
        (isAssignable(firstTransform.getValueOutType(), secondTransform.getValueInType())))) {
        throw new IllegalArgumentException(String.format("Transform %s and Transform %s Types don't match",
                                                         currStage.getName(), nextStage.getName()));
      }
    }
  }

  private void configureSource(ETLStage sourceConfig, AdapterConfigurer configurer,
                               PipelineConfigurer pipelineConfigurer) throws Exception {
    batchSource.configurePipeline(sourceConfig, pipelineConfigurer);

    // TODO: after a few more use cases, determine if the spec is really needed at runtime
    //       since everything in the spec must be known at compile time, it seems like there shouldn't be a need
    DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(batchSource.getClass());
    StageSpecification specification = stageConfigurer.createSpecification();
    configurer.addRuntimeArgument(Constants.Source.SPECIFICATION, GSON.toJson(specification));
  }

  private void configureSink(ETLStage sinkConfig, AdapterConfigurer configurer,
                             PipelineConfigurer pipelineConfigurer) throws Exception {
    batchSink.configurePipeline(sinkConfig, pipelineConfigurer);

    // TODO: after a few more use cases, determine if the spec is really needed at runtime
    //       since everything in the spec must be known at compile time, it seems like there shouldn't be a need
    DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(batchSink.getClass());
    StageSpecification specification = stageConfigurer.createSpecification();
    configurer.addRuntimeArgument(Constants.Sink.SPECIFICATION, GSON.toJson(specification));
  }

  private void configureTransforms(AdapterConfigurer configurer) throws Exception {
    List<StageSpecification> transformSpecs = Lists.newArrayList();
    for (Transform transformObj : transforms) {

      // TODO: after a few more use cases, determine if the spec is really needed at runtime
      //       since everything in the spec must be known at compile time, it seems like there shouldn't be a need
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
