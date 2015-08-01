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

package co.cask.cdap.template.etl.batch;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.Transformation;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.common.Constants;
import co.cask.cdap.template.etl.common.Destroyables;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.PluginID;
import co.cask.cdap.template.etl.common.StageMetrics;
import co.cask.cdap.template.etl.common.TransformExecutor;
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

  private BatchSource batchSource;
  private BatchSink batchSink;
  private String sourcePluginId;
  private String sinkPluginId;
  private Metrics mrMetrics;

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
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Source.PLUGINID));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Sink.PLUGINID));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Transform.PLUGINIDS));

    ETLBatchConfig etlBatchConfig = GSON.fromJson(runtimeArgs.get(Constants.CONFIG_KEY), ETLBatchConfig.class);

    prepareSource(context, etlBatchConfig.getSource());
    prepareSink(context, etlBatchConfig.getSink());

    if (etlBatchConfig.getResources() != null) {
      context.setMapperResources(etlBatchConfig.getResources());
    }
    job.setMapperClass(ETLMapper.class);
    job.setNumReduceTasks(0);
  }

  private void prepareSource(MapReduceContext context, ETLStage sourceStage) throws Exception {
    sourcePluginId = context.getRuntimeArguments().get(Constants.Source.PLUGINID);
    batchSource = context.newPluginInstance(sourcePluginId);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics, sourcePluginId);
    LOG.debug("Source Stage : {}", sourceStage);
    LOG.debug("Source Class : {}", batchSource.getClass().getName());
    batchSource.prepareRun(sourceContext);
  }

  private void prepareSink(MapReduceContext context, ETLStage sinkStage) throws Exception {
    sinkPluginId = context.getRuntimeArguments().get(Constants.Sink.PLUGINID);
    batchSink = context.newPluginInstance(sinkPluginId);
    BatchSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics, sinkPluginId);
    LOG.debug("Sink Stage : {}", sinkStage);
    LOG.debug("Sink Class : {}", batchSink.getClass().getName());
    batchSink.prepareRun(sinkContext);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    onRunFinishSource(context, succeeded);
    onRunFinishSink(context, succeeded);
    LOG.info("Batch Run for Adapter {} : {}", context.getRuntimeArguments().get(Constants.ADAPTER_NAME), succeeded);
  }

  private void onRunFinishSource(MapReduceContext context, boolean succeeded) {
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics, sourcePluginId);
    LOG.info("On RunFinish Source : {}", batchSource.getClass().getName());
    try {
      batchSource.onRunFinish(succeeded, sourceContext);
    } catch (Throwable t) {
      LOG.warn("Exception when calling onRunFinish on {}", batchSource, t);
    }
  }

  private void onRunFinishSink(MapReduceContext context, boolean succeeded) {
    BatchSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics, sinkPluginId);
    LOG.info("On RunFinish Sink : {}", batchSink.getClass().getName());
    try {
      batchSink.onRunFinish(succeeded, sinkContext);
    } catch (Throwable t) {
      LOG.warn("Exception when calling onRunFinish on {}", batchSink, t);
    }
  }

  /**
   * Mapper Driver for ETL Transforms.
   */
  public static class ETLMapper extends Mapper implements ProgramLifecycle<MapReduceContext> {
    private static final Gson GSON = new Gson();
    private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();

    private List<Transform> transforms;

    private TransformExecutor<KeyValue, KeyValue> transformExecutor;
    private Metrics mapperMetrics;

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      Map<String, String> runtimeArgs = context.getRuntimeArguments();
      ETLBatchConfig etlConfig = GSON.fromJson(runtimeArgs.get(Constants.CONFIG_KEY), ETLBatchConfig.class);
      String sourcePluginId = runtimeArgs.get(Constants.Source.PLUGINID);
      String sinkPluginId = runtimeArgs.get(Constants.Sink.PLUGINID);
      List<String> transformIds = GSON.fromJson(runtimeArgs.get(Constants.Transform.PLUGINIDS), STRING_LIST_TYPE);


      List<ETLStage> stageList = etlConfig.getTransforms();
      List<Transformation> pipeline = Lists.newArrayListWithCapacity(stageList.size() + 2);
      List<StageMetrics> stageMetrics = Lists.newArrayListWithCapacity(stageList.size() + 2);
      transforms = Lists.newArrayListWithCapacity(stageList.size());

      BatchSource source = context.newPluginInstance(sourcePluginId);
      BatchSourceContext batchSourceContext = new MapReduceSourceContext(context, mapperMetrics, sourcePluginId);
      source.initialize(batchSourceContext);
      pipeline.add(source);
      stageMetrics.add(new StageMetrics(mapperMetrics, PluginID.from(sourcePluginId)));

      addTransforms(stageList, pipeline, stageMetrics, transformIds, context);

      BatchSink sink = context.newPluginInstance(sinkPluginId);
      BatchSinkContext batchSinkContext = new MapReduceSinkContext(context, mapperMetrics, sinkPluginId);
      sink.initialize(batchSinkContext);
      pipeline.add(sink);
      stageMetrics.add(new StageMetrics(mapperMetrics, PluginID.from(sinkPluginId)));

      transformExecutor = new TransformExecutor<>(pipeline, stageMetrics);
    }

    private void addTransforms(List<ETLStage> stageConfigs, List<Transformation> pipeline,
                               List<StageMetrics> stageMetrics, List<String> transformIds,
                               MapReduceContext context) throws Exception {
      Preconditions.checkArgument(stageConfigs.size() == transformIds.size());

      for (int i = 0; i < stageConfigs.size(); i++) {
        ETLStage stageConfig = stageConfigs.get(i);
        String transformId = transformIds.get(i);
        Transform transform = context.newPluginInstance(transformId);
        BatchTransformContext transformContext = new BatchTransformContext(context, mapperMetrics, transformId);
        LOG.debug("Transform Stage : {}", stageConfig.getName());
        LOG.debug("Transform Class : {}", transform.getClass().getName());
        transform.initialize(transformContext);
        pipeline.add(transform);
        transforms.add(transform);
        stageMetrics.add(new StageMetrics(mapperMetrics, PluginID.from(transformId)));
      }
    }

    @Override
    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
      try {
        KeyValue input = new KeyValue(key, value);
        for (KeyValue output : transformExecutor.runOneIteration(input)) {
          context.write(output.getKey(), output.getValue());
        }
      } catch (Exception e) {
        LOG.error("Exception thrown in BatchDriver Mapper : {}", e);
        Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      // Both BatchSource and BatchSink implements Transform, hence are inside the transformExecutor as well
      Destroyables.destroyQuietly(transformExecutor);
    }
  }
}
