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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.app.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.batch.BatchConfigurable;
import co.cask.cdap.template.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.batch.MapReduceRuntimeContext;
import co.cask.cdap.template.etl.batch.MapReduceSinkContext;
import co.cask.cdap.template.etl.batch.MapReduceSourceContext;
import co.cask.cdap.template.etl.common.Constants;
import co.cask.cdap.template.etl.common.DefaultEmitter;
import co.cask.cdap.template.etl.common.Destroyables;
import co.cask.cdap.template.etl.common.Pipeline;
import co.cask.cdap.template.etl.common.PipelineRegisterer;
import co.cask.cdap.template.etl.common.PluginID;
import co.cask.cdap.template.etl.common.StageMetrics;
import co.cask.cdap.template.etl.common.TransformDetail;
import co.cask.cdap.template.etl.common.TransformExecutor;
import co.cask.cdap.template.etl.common.TransformInfo;
import co.cask.cdap.template.etl.common.TransformResponse;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MapReduce driver for Batch ETL Adapters.
 */
public class ETLMapReduce extends AbstractMapReduce {
  public static final String NAME = ETLMapReduce.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final String SINK_OUTPUTS_KEY = "cdap.etl.sink.outputs";
  private static final Type SINK_OUTPUTS_TYPE = new TypeToken<Map<String, Set<String>>>() { }.getType();
  private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Gson GSON = new Gson();

  private BatchConfigurable<BatchSourceContext> batchSource;
  private List<BatchConfigurable<BatchSinkContext>> batchSinks;
  // injected by CDAP
  @SuppressWarnings("unused")
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
    Pipeline pipelineIds = pipelineRegisterer.registerPlugins(config, null, DatasetProperties.EMPTY);

    if (config.getResources() != null) {
      setMapperResources(config.getResources());
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.Source.PLUGINID, pipelineIds.getSource());
    properties.put(Constants.Sink.PLUGINIDS, GSON.toJson(pipelineIds.getSinks()));
    properties.put(Constants.Transform.PLUGINIDS, GSON.toJson(pipelineIds.getTransforms()));
    setProperties(properties);
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();

    Map<String, String> properties = context.getSpecification().getProperties();
    String sourcePluginId = properties.get(Constants.Source.PLUGINID);

    batchSource = context.newInstance(sourcePluginId);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics, sourcePluginId);
    batchSource.prepareRun(sourceContext);

    Map<String, Set<String>> sinkOutputs = new HashMap<>();
    String sinkPluginIdsStr = properties.get(Constants.Sink.PLUGINIDS);
    // should never happen
    Preconditions.checkNotNull(sinkPluginIdsStr, "sink plugin ids could not be found in program properties.");

    List<String> sinkPluginIds = GSON.fromJson(sinkPluginIdsStr, STRING_LIST_TYPE);
    batchSinks = Lists.newArrayListWithCapacity(sinkPluginIds.size());
    for (String sinkPluginId : sinkPluginIds) {
      BatchConfigurable<BatchSinkContext> batchSink = context.newInstance(sinkPluginId);
      MapReduceSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics, sinkPluginId);
      batchSink.prepareRun(sinkContext);
      batchSinks.add(batchSink);
      sinkOutputs.put(sinkPluginId, sinkContext.getOutputNames());
    }
    job.getConfiguration().set(SINK_OUTPUTS_KEY, GSON.toJson(sinkOutputs));

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
    String sinkPluginIdsStr = context.getSpecification().getProperty(Constants.Sink.PLUGINIDS);
    // should never happen
    Preconditions.checkNotNull(sinkPluginIdsStr, "sink plugin ids could not be found in program properties.");

    List<String> sinkPluginIds = GSON.fromJson(sinkPluginIdsStr, STRING_LIST_TYPE);
    for (int i = 0; i < sinkPluginIds.size(); i++) {
      BatchConfigurable<BatchSinkContext> batchSink = batchSinks.get(i);
      String sinkPluginId = sinkPluginIds.get(i);
      BatchSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics, sinkPluginId);
      try {
        batchSink.onRunFinish(succeeded, sinkContext);
      } catch (Throwable t) {
        LOG.warn("Exception when calling onRunFinish on {}", batchSink, t);
      }
    }
  }

  /**
   * Mapper Driver for ETL Transforms.
   */
  public static class ETLMapper extends Mapper implements ProgramLifecycle<MapReduceTaskContext> {
    private static final Logger LOG = LoggerFactory.getLogger(ETLMapper.class);
    private static final Gson GSON = new Gson();
    private static final Type TRANSFORMDETAILS_LIST_TYPE = new TypeToken<List<TransformInfo>>() { }.getType();

    private TransformExecutor<KeyValue, Object> transformExecutor;
    // injected by CDAP
    @SuppressWarnings("unused")
    private Metrics mapperMetrics;
    private List<WrappedSink<Object, Object, Object>> sinks;

    @Override
    public void initialize(MapReduceTaskContext context) throws Exception {
      // get source, transform, sink ids from program properties
      context.getSpecification().getProperties();
      Map<String, String> properties = context.getSpecification().getProperties();

      String sourcePluginId = properties.get(Constants.Source.PLUGINID);
      // should never happen
      String transformInfosStr = properties.get(Constants.Transform.PLUGINIDS);
      Preconditions.checkNotNull(transformInfosStr, "Transform plugin ids not found in program properties.");

      List<TransformInfo> transformInfos = GSON.fromJson(transformInfosStr, TRANSFORMDETAILS_LIST_TYPE);
      List<TransformDetail> pipeline = Lists.newArrayListWithCapacity(transformInfos.size() + 2);

      BatchSource source = context.newInstance(sourcePluginId);
      BatchRuntimeContext runtimeContext = new MapReduceRuntimeContext(context, mapperMetrics, sourcePluginId);
      source.initialize(runtimeContext);
      pipeline.add(new TransformDetail(sourcePluginId, source,
        new StageMetrics(mapperMetrics, PluginID.from(sourcePluginId))));

      addTransforms(pipeline, transformInfos, context);

      // get the list of sinks, and the names of the outputs each sink writes to
      Context hadoopContext = (Context) context.getHadoopContext();
      String sinkOutputsStr = hadoopContext.getConfiguration().get(SINK_OUTPUTS_KEY);
      // should never happen, this is set in beforeSubmit
      Preconditions.checkNotNull(sinkOutputsStr, "Sink outputs not found in hadoop conf.");

      Map<String, Set<String>> sinkOutputs = GSON.fromJson(sinkOutputsStr, SINK_OUTPUTS_TYPE);
      sinks = new ArrayList<>(sinkOutputs.size());
      for (Map.Entry<String, Set<String>> sinkOutput : sinkOutputs.entrySet()) {
        String sinkPluginId = sinkOutput.getKey();
        Set<String> sinkOutputNames = sinkOutput.getValue();

        BatchSink<Object, Object, Object> sink = context.newInstance(sinkPluginId);
        runtimeContext = new MapReduceRuntimeContext(context, mapperMetrics, sinkPluginId);
        sink.initialize(runtimeContext);
        sinks.add(new WrappedSink<>(sinkPluginId, sink, sinkOutputNames, context, mapperMetrics));
      }

      transformExecutor = new TransformExecutor<>(pipeline);
    }

    private void addTransforms(List<TransformDetail> pipeline,
                               List<TransformInfo> transformInfos,
                               MapReduceTaskContext context) throws Exception {

      for (TransformInfo transformInfo : transformInfos) {
        String transformId = transformInfo.getTransformId();
        Transform transform = context.newInstance(transformId);
        BatchRuntimeContext transformContext = new MapReduceRuntimeContext(context, mapperMetrics, transformId);
        LOG.debug("Transform Class : {}", transform.getClass().getName());
        transform.initialize(transformContext);
        pipeline.add(new TransformDetail(transformId, transform,
                                               new StageMetrics(mapperMetrics, PluginID.from(transformId))));
      }
    }

    @Override
    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
      try {
        KeyValue<Object, Object> input = new KeyValue<>(key, value);
        TransformResponse<Object> recordsAndErrors = transformExecutor.runOneIteration(input);
        Iterator<Object> transformedRecords = recordsAndErrors.getEmittedRecords();
        while (transformedRecords.hasNext()) {
          Object transformedRecord = transformedRecords.next();
          for (WrappedSink<Object, Object, Object> sink : sinks) {
            sink.write(transformedRecord);
          }
        }
        transformExecutor.resetEmitters();
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

  // wrapper around sinks to help writing sink output to the correct named output
  private static class WrappedSink<IN, KEY_OUT, VAL_OUT> {
    private final BatchSink<IN, KEY_OUT, VAL_OUT> sink;
    private final DefaultEmitter<KeyValue<KEY_OUT, VAL_OUT>> emitter;
    private final Set<String> outputNames;
    private final MapReduceTaskContext context;

    private WrappedSink(String sinkPluginId,
                        BatchSink<IN, KEY_OUT, VAL_OUT> sink,
                        Set<String> outputNames,
                        MapReduceTaskContext context,
                        Metrics metrics) {
      this.sink = sink;
      this.emitter = new DefaultEmitter<>(new StageMetrics(metrics, PluginID.from(sinkPluginId)));
      this.outputNames = outputNames;
      this.context = context;
    }

    private void write(IN input) throws Exception {
      sink.transform(input, emitter);
      for (KeyValue outputRecord : emitter) {
        for (String outputName : outputNames) {
          context.write(outputName, outputRecord.getKey(), outputRecord.getValue());
        }
      }
      // TODO: write errors to error dataset
      emitter.reset();
    }
  }
}
