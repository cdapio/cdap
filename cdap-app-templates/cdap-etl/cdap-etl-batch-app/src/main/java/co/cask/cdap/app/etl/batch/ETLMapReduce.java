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

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.app.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.batch.BatchTransformContext;
import co.cask.cdap.template.etl.batch.MapReduceSinkContext;
import co.cask.cdap.template.etl.batch.MapReduceSourceContext;
import co.cask.cdap.template.etl.common.Constants;
import co.cask.cdap.template.etl.common.Destroyables;
import co.cask.cdap.template.etl.common.Pipeline;
import co.cask.cdap.template.etl.common.PipelineRegisterer;
import co.cask.cdap.template.etl.common.PluginID;
import co.cask.cdap.template.etl.common.StageMetrics;
import co.cask.cdap.template.etl.common.TransformDetails;
import co.cask.cdap.template.etl.common.TransformExecutor;
import co.cask.cdap.template.etl.common.TransformationDetails;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * MapReduce driver for Batch ETL Adapters.
 */
public class ETLMapReduce extends AbstractMapReduce {
  public static final String NAME = ETLMapReduce.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final Gson GSON = new Gson();

  private BatchSource batchSource;
  private BatchSink batchSink;
  private Metrics mrMetrics;

  // this is only visible at configure time, not at runtime
  private final ETLBatchConfig config;

  public ETLMapReduce(ETLBatchConfig config) {
    this.config = config;
  }

  @Override
  public void configure() {
    setName(NAME);
    setDescription("MapReduce driver for Batch ETL Adapters");

    PipelineRegisterer pipelineRegisterer = new PipelineRegisterer(getConfigurer());
    //TODO : CDAP-3480 - passing null now, will implement error dataset using Fileset for ETLMapReduce
    Pipeline pipelineIds = pipelineRegisterer.registerPlugins(config, null);

    if (config.getResources() != null) {
      setMapperResources(config.getResources());
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.Source.PLUGINID, pipelineIds.getSource());
    properties.put(Constants.Sink.PLUGINID, pipelineIds.getSink());
    properties.put(Constants.Transform.PLUGINIDS, GSON.toJson(pipelineIds.getTransforms()));
    setProperties(properties);
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();

    Map<String, String> properties = context.getSpecification().getProperties();
    String sourcePluginId = properties.get(Constants.Source.PLUGINID);
    String sinkPluginId = properties.get(Constants.Sink.PLUGINID);

    batchSource = context.newInstance(sourcePluginId);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics, sourcePluginId);
    batchSource.prepareRun(sourceContext);

    batchSink = context.newInstance(sinkPluginId);
    BatchSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics, sinkPluginId);
    batchSink.prepareRun(sinkContext);

    job.setMapperClass(ETLMapper.class);
    job.setNumReduceTasks(0);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    onRunFinishSource(context, succeeded);
    onRunFinishSink(context, succeeded);
    LOG.info("Batch Run finished : succeeded = {}", succeeded);
  }

  private void onRunFinishSource(MapReduceContext context, boolean succeeded) {
    String sourcePluginId = context.getSpecification().getProperty(Constants.Source.PLUGINID);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics, sourcePluginId);
    LOG.info("On RunFinish Source : {}", batchSource.getClass().getName());
    try {
      batchSource.onRunFinish(succeeded, sourceContext);
    } catch (Throwable t) {
      LOG.warn("Exception when calling onRunFinish on {}", batchSource, t);
    }
  }

  private void onRunFinishSink(MapReduceContext context, boolean succeeded) {
    String sinkPluginId = context.getSpecification().getProperty(Constants.Sink.PLUGINID);
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
    private static final Logger LOG = LoggerFactory.getLogger(ETLMapper.class);
    private static final Gson GSON = new Gson();
    private static final Type TRANSFORMDETAILS_LIST_TYPE = new TypeToken<List<TransformDetails>>() { }.getType();

    private TransformExecutor<KeyValue, KeyValue> transformExecutor;
    private Metrics mapperMetrics;

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      context.getSpecification().getProperties();
      Map<String, String> properties = context.getSpecification().getProperties();

      String sourcePluginId = properties.get(Constants.Source.PLUGINID);
      String sinkPluginId = properties.get(Constants.Sink.PLUGINID);
      List<TransformDetails> transformIds = GSON.fromJson(properties.get(Constants.Transform.PLUGINIDS),
                                                               TRANSFORMDETAILS_LIST_TYPE);

      List<TransformationDetails> pipeline = Lists.newArrayListWithCapacity(transformIds.size() + 2);

      BatchSource source = context.newInstance(sourcePluginId);
      BatchSourceContext batchSourceContext = new MapReduceSourceContext(context, mapperMetrics, sourcePluginId);
      source.initialize(batchSourceContext);
      pipeline.add(new TransformationDetails(sourcePluginId, source,
                                             new StageMetrics(mapperMetrics, PluginID.from(sourcePluginId))));

      addTransforms(pipeline, transformIds, context);

      BatchSink sink = context.newInstance(sinkPluginId);
      BatchSinkContext batchSinkContext = new MapReduceSinkContext(context, mapperMetrics, sinkPluginId);
      sink.initialize(batchSinkContext);
      pipeline.add(new TransformationDetails(sinkPluginId, sink,
                                             new StageMetrics(mapperMetrics, PluginID.from(sinkPluginId))));

      transformExecutor = new TransformExecutor<>(pipeline);
    }

    private void addTransforms(List<TransformationDetails> pipeline,
                               List<TransformDetails> transformIds,
                               MapReduceContext context) throws Exception {

      for (int i = 0; i < transformIds.size(); i++) {
        String transformId = transformIds.get(i).getTransformId();
        Transform transform = context.newInstance(transformId);
        BatchTransformContext transformContext = new BatchTransformContext(context, mapperMetrics, transformId);
        LOG.debug("Transform Class : {}", transform.getClass().getName());
        transform.initialize(transformContext);
        pipeline.add(new TransformationDetails(transformId, transform,
                                               new StageMetrics(mapperMetrics, PluginID.from(transformId))));
      }
    }

    @Override
    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
      try {
        KeyValue input = new KeyValue(key, value);
        Iterator<KeyValue> iterator = transformExecutor.runOneIteration(input).getEmittedRecords();
        while (iterator.hasNext()) {
          KeyValue output = iterator.next();
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
