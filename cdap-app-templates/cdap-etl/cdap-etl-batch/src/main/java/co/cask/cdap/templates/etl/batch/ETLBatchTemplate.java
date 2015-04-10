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
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.batch.sinks.KVTableSink;
import co.cask.cdap.templates.etl.batch.sources.KVTableSource;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultStageConfigurer;
import co.cask.cdap.templates.etl.common.config.ETLStage;
import co.cask.cdap.templates.etl.transforms.IdentityTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToGenericRecordTransform;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

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
    initTable(Lists.<Class>newArrayList(KVTableSource.class, KVTableSink.class, IdentityTransform.class,
                                        StructuredRecordToGenericRecordTransform.class));
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
  public void configureAdapter(String adapterName, ETLBatchConfig etlConfig, AdapterConfigurer configurer)
    throws Exception {
    // Get cronEntry string for ETL Batch Adapter
    String cronEntry = etlConfig.getSchedule();

    ETLStage source = etlConfig.getSource();
    ETLStage sink = etlConfig.getSink();
    List<ETLStage> transform = etlConfig.getTransforms();

    // Validate Adapter by making sure the key-value types match.
    validateAdapter(source, sink, transform);

    configureSource(configurer);
    configureSink(configurer);
    configureTransforms(configurer);

    configurer.addRuntimeArgument(Constants.ADAPTER_NAME, adapterName);
    configurer.addRuntimeArgument(Constants.CONFIG_KEY, GSON.toJson(etlConfig));
    configurer.setSchedule(new TimeSchedule(String.format("etl.batch.adapter.%s.schedule", adapterName),
                                            String.format("Schedule for %s Adapter", adapterName),
                                            cronEntry));
  }

  private void validateAdapter(ETLStage source, ETLStage sink, List<ETLStage> transformList) throws Exception {
    String sourceClassName = sourceClassMap.get(source.getName());
    String sinkClassName = sinkClassMap.get(sink.getName());
    batchSource = (BatchSource) Class.forName(sourceClassName).newInstance();
    batchSink = (BatchSink) Class.forName(sinkClassName).newInstance();

    if (transformList.size() == 0) {
      // No transforms. Check only source and sink.
      if (!(batchSink.getKeyType().equals(batchSource.getKeyType()) &&
        batchSink.getValueType().equals(batchSource.getValueType()))) {
        throw new Exception(String.format("Source %s and Sink %s Types doesn't match",
                                          source.getName(), sink.getName()));
      }
    } else {
      // Check the first and last transform with source and sink.
      ETLStage firstStage = transformList.get(0);
      ETLStage lastStage = transformList.get(transformList.size() - 1);
      String firstTransformClassName = transformClassMap.get(firstStage.getName());
      String lastTransformClassName = transformClassMap.get(lastStage.getName());
      Transform firstTransform = (Transform) Class.forName(firstTransformClassName).newInstance();
      Transform lastTransform = (Transform) Class.forName(lastTransformClassName).newInstance();

      if (!(firstTransform.getKeyInType().equals(batchSource.getKeyType()) &&
        firstTransform.getValueInType().equals(batchSource.getValueType()))) {
        throw new Exception(String.format("Source %s and Transform %s Types doesn't match",
                                          source.getName(), firstStage.getName()));
      }

      if (!(lastTransform.getKeyOutType().equals(batchSink.getKeyType()) &&
        lastTransform.getValueOutType().equals(batchSink.getValueType()))) {
        throw new Exception(String.format("Sink %s and Transform %s Types doesn't match",
                                          sink.getName(), lastStage.getName()));
      }

      if (transformList.size() > 1) {
        // Check transform stages.
        validateTransforms(transformList);
      } else {
        transforms.add(firstTransform);
      }
    }
  }

  private void validateTransforms(List<ETLStage> transformList) throws Exception {
    for (ETLStage etlStage : transformList) {
      String transformName = transformClassMap.get(etlStage.getName());
      Transform transformObj = (Transform) Class.forName(transformName).newInstance();
      transforms.add(transformObj);
    }

    for (int i = 0; i < transformList.size() - 1; i++) {
      ETLStage currStage = transformList.get(i);
      ETLStage nextStage = transformList.get(i + 1);
      Transform firstTransform = transforms.get(i);
      Transform secondTransform = transforms.get(i + 1);

      if (!(secondTransform.getKeyInType().equals(firstTransform.getKeyOutType()) &&
        secondTransform.getValueInType().equals(firstTransform.getValueOutType()))) {
        throw new Exception(String.format("Transform %s and Transform %s Types doesn't match",
                                          currStage.getName(), nextStage.getName()));
      }
    }
  }

  private void configureSource(AdapterConfigurer configurer) throws Exception {
    DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(batchSource.getClass());
    StageSpecification specification = stageConfigurer.createSpecification();
    configurer.addRuntimeArgument(Constants.Source.SPECIFICATION, GSON.toJson(specification));
  }

  private void configureSink(AdapterConfigurer configurer) throws Exception {
    DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(batchSink.getClass());
    StageSpecification specification = stageConfigurer.createSpecification();
    configurer.addRuntimeArgument(Constants.Sink.SPECIFICATION, GSON.toJson(specification));
  }

  private void configureTransforms(AdapterConfigurer configurer) throws Exception {
    List<StageSpecification> transformSpecs = Lists.newArrayList();
    for (Transform transform : transforms) {
      DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(transform.getClass());
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
