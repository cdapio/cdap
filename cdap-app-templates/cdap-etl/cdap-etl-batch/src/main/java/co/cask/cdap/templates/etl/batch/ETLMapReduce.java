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

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSinkContext;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultTransformContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * MapReduce driver for Batch ETL Adapters.
 */
public class ETLMapReduce extends AbstractMapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final Gson GSON = new Gson();

  @Override
  public void configure() {
    setName(ETLMapReduce.class.getSimpleName());
    setDescription("MapReduce driver for Batch ETL Adapters");
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    Map<String, String> runtimeArgs = context.getRuntimeArguments();

    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.ADAPTER_NAME));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.CONFIG_KEY));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Source.SPECIFICATION));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Sink.SPECIFICATION));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Transform.SPECIFICATIONS));

    ETLBatchConfig config = GSON.fromJson(runtimeArgs.get(Constants.CONFIG_KEY), ETLBatchConfig.class);

    prepareSource(context, config.getSource());
    prepareSink(context, config.getSink());

    job.setMapperClass(ETLMapper.class);
    job.setNumReduceTasks(0);
  }

  private void prepareSource(MapReduceContext context, ETLStage sourceStage) throws Exception {
    StageSpecification sourceSpec = GSON.fromJson(
      context.getRuntimeArguments().get(Constants.Source.SPECIFICATION), StageSpecification.class);
    BatchSource source = (BatchSource) Class.forName(sourceSpec.getClassName()).newInstance();
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, sourceStage, sourceSpec);
    LOG.info("Source Stage : {}", sourceStage);
    LOG.info("Source Class : {}", source.getClass().getName());
    LOG.info("Specifications of Source : {}", sourceSpec);
    source.prepareJob(sourceContext);
  }

  private void prepareSink(MapReduceContext context, ETLStage sinkStage) throws Exception {
    StageSpecification sinkSpec = GSON.fromJson(
      context.getRuntimeArguments().get(Constants.Sink.SPECIFICATION), StageSpecification.class);
    BatchSink sink = (BatchSink) Class.forName(sinkSpec.getClassName()).newInstance();
    BatchSinkContext sinkContext = new MapReduceSinkContext(context, sinkStage, sinkSpec);
    LOG.info("Sink Stage : {}", sinkStage);
    LOG.info("Sink Class : {}", sink.getClass().getName());
    LOG.info("Specifications of Sink : {}", sinkSpec);
    sink.prepareJob(sinkContext);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) {
    LOG.info("Batch Run for Adapter {} : {}", context.getRuntimeArguments().get(Constants.ADAPTER_NAME), succeeded);
  }

  /**
   * Mapper Driver for ETL Transforms.
   */
  public static class ETLMapper extends Mapper implements ProgramLifecycle<MapReduceContext> {
    private static final Gson GSON = new Gson();
    private static final Type SPEC_LIST_TYPE = new TypeToken<List<StageSpecification>>() { }.getType();

    private BatchPipelineExecutor batchPipelineExecutor;

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      Map<String, String> runtimeArgs = context.getRuntimeArguments();
      ETLBatchConfig etlConfig = GSON.fromJson(runtimeArgs.get(Constants.CONFIG_KEY), ETLBatchConfig.class);

      List<StageSpecification> specificationList = GSON.fromJson(
        runtimeArgs.get(Constants.Transform.SPECIFICATIONS), SPEC_LIST_TYPE);
      StageSpecification sourceSpec = GSON.fromJson(
        runtimeArgs.get(Constants.Source.SPECIFICATION), StageSpecification.class);
      StageSpecification sinkSpec = GSON.fromJson(
        runtimeArgs.get(Constants.Sink.SPECIFICATION), StageSpecification.class);

      List<ETLStage> stageList = etlConfig.getTransforms();
      LOG.info("Transform Stages : {}", stageList);
      LOG.info("Specifications of Transforms : {}", specificationList);

      Preconditions.checkArgument(stageList.size() == specificationList.size());
      instantiateTransforms(specificationList, stageList);
      BatchSource source = instantiateStage(sourceSpec);
      source.initialize(etlConfig.getSource());
      BatchSink sink = instantiateStage(sinkSpec);
      sink.initialize(etlConfig.getSink());
      List<Transform> transforms = instantiateTransforms(specificationList, stageList);

      batchPipelineExecutor = new BatchPipelineExecutor(source, transforms, sink);
    }

    private List<Transform> instantiateTransforms(List<StageSpecification> specList, List<ETLStage> stageConfigs)
      throws Exception {
      List<Transform> transforms = Lists.newArrayListWithCapacity(specList.size());
      for (int i = 0; i < specList.size(); i++) {
        StageSpecification spec = specList.get(i);
        ETLStage stageConfig = stageConfigs.get(i);
        Transform transform = instantiateStage(spec);
        DefaultTransformContext transformContext = new DefaultTransformContext(spec, stageConfig.getProperties());
        transform.initialize(transformContext);
        transforms.add(transform);
      }
      return transforms;
    }

    @Override
    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
      try {
        batchPipelineExecutor.runOneIteration(key, value, new DefaultBatchSinkWriter(context));
      } catch (Exception e) {
        LOG.error("Exception thrown in BatchDriver Mapper : {}", e);
        Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      // no-op
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T instantiateStage(StageSpecification spec) {
    try {
      return (T) Class.forName(spec.getClassName()).newInstance();
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to find class {}", spec.getClassName(), e);
      throw Throwables.propagate(e);
    } catch (InstantiationException e) {
      LOG.error("Unable to instantiate {}", spec.getClassName(), e);
      throw Throwables.propagate(e);
    } catch (IllegalAccessException e) {
      LOG.error("Illegal access while instantiating {}", spec.getClassName(), e);
      throw Throwables.propagate(e);
    }
  }
}
