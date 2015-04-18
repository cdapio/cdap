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

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
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
import co.cask.cdap.templates.etl.batch.sources.DBSource;
import co.cask.cdap.templates.etl.batch.sources.KVTableSource;
import co.cask.cdap.templates.etl.batch.sources.StreamBatchSource;
import co.cask.cdap.templates.etl.batch.sources.TableSource;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultPipelineConfigurer;
import co.cask.cdap.templates.etl.common.DefaultStageConfigurer;
import co.cask.cdap.templates.etl.common.StageConfigurator;
import co.cask.cdap.templates.etl.transforms.IdentityTransform;
import co.cask.cdap.templates.etl.transforms.RowToStructuredRecordTransform;
import co.cask.cdap.templates.etl.transforms.ScriptFilterTransform;
import co.cask.cdap.templates.etl.transforms.StreamToStructuredRecordTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToPutTransform;
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

    // Add classes from Lib here to be available for use in the ETL Adapter.
    // TODO: Remove this when plugins management is available.
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
                                        ScriptFilterTransform.class,
                                        DBSource.class));
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
  public void configureAdapter(String adapterName, ETLBatchConfig etlBatchConfig, AdapterConfigurer configurer)
    throws Exception {
    ETLStage sourceConfig = etlBatchConfig.getSource();
    ETLStage sinkConfig = etlBatchConfig.getSink();
    List<ETLStage> transformConfigs = etlBatchConfig.getTransforms();

    // Instantiate Source, Transform, Sink Stages.
    instantiateStages(sourceConfig, sinkConfig, transformConfigs);

    // TODO: Validate Adapter by making sure the key-value types of stages match.

    // pipeline configurer is just a wrapper around an adapter configurer that limits what can be added,
    // since we don't want sources and sinks setting schedules or anything like that.
    PipelineConfigurer pipelineConfigurer = new DefaultPipelineConfigurer(configurer);
    StageConfigurator.configure(batchSource, sourceConfig, configurer, pipelineConfigurer,
                                Constants.Source.SPECIFICATION);
    StageConfigurator.configure(batchSink, sinkConfig, configurer, pipelineConfigurer,
                                Constants.Sink.SPECIFICATION);
    StageConfigurator.configureTransforms(transforms, configurer, Constants.Transform.SPECIFICATIONS);

    configurer.addRuntimeArgument(Constants.ADAPTER_NAME, adapterName);
    configurer.addRuntimeArgument(Constants.CONFIG_KEY, GSON.toJson(etlBatchConfig));
    configurer.setSchedule(new TimeSchedule(String.format("etl.batch.adapter.%s.schedule", adapterName),
                                                   String.format("Schedule for %s Adapter", adapterName),
                                                   etlBatchConfig.getSchedule()));
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
      throw new IllegalArgumentException("Unable to load class. Check stage names.", e);
    }
  }

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName("etlbatch");
    configurer.setDescription("Batch Extract-Transform-Load (ETL) Adapter");
    configurer.addMapReduce(new ETLMapReduce());
    configurer.addWorkflow(new ETLWorkflow());
  }
}
