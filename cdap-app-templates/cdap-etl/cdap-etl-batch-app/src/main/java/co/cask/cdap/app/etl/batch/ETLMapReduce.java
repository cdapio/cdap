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
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceConfigurer;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.template.etl.api.PipelineConfigurable;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.Transformation;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.batch.BatchTransformContext;
import co.cask.cdap.template.etl.batch.MapReduceSinkContext;
import co.cask.cdap.template.etl.batch.MapReduceSourceContext;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.common.Constants;
import co.cask.cdap.template.etl.common.DefaultPipelineConfigurer;
import co.cask.cdap.template.etl.common.Destroyables;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.PipelineValidator;
import co.cask.cdap.template.etl.common.PluginID;
import co.cask.cdap.template.etl.common.StageMetrics;
import co.cask.cdap.template.etl.common.TransformExecutor;
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
import java.util.List;
import java.util.Map;

/**
 * MapReduce driver for Batch ETL Adapters.
 */
public class ETLMapReduce extends AbstractMapReduce {
  public static final String NAME = ETLMapReduce.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final Gson GSON = new Gson();
  private static final String SOURCE_ID_KEY = "sourceId";
  private static final String SINK_ID_KEY = "sinkId";
  private static final String TRANSFORM_IDS_KEY = "transformIds";
  private static final String RESOURCES_KEY = "resources";

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

    ETLStage sourceConfig = config.getSource();
    ETLStage sinkConfig = config.getSink();
    List<ETLStage> transformConfigs = config.getTransforms();
    String sourcePluginId = PluginID.from(Constants.Source.PLUGINTYPE, sourceConfig.getName(), 1).getID();
    // 2 + since we start at 1, and there is always a source.  For example, if there are 0 transforms, sink is stage 2.
    String sinkPluginId =
      PluginID.from(Constants.Sink.PLUGINTYPE, sinkConfig.getName(), 2 + transformConfigs.size()).getID();

    // Instantiate Source, Transforms, Sink stages.
    // Use the plugin name as the plugin id for source and sink stages since there can be only one source and one sink.
    PipelineConfigurable source = usePlugin(Constants.Source.PLUGINTYPE, sourceConfig.getName(),
                                            sourcePluginId, getPluginProperties(sourceConfig));
    if (source == null) {
      throw new IllegalArgumentException(String.format("No Plugin of type '%s' named '%s' was found.",
                                                       Constants.Source.PLUGINTYPE, sourceConfig.getName()));
    }

    PipelineConfigurable sink = usePlugin(Constants.Sink.PLUGINTYPE, sinkConfig.getName(),
                                          sinkPluginId, getPluginProperties(sinkConfig));
    if (sink == null) {
      throw new IllegalArgumentException(String.format("No Plugin of type '%s' named '%s' was found.",
                                                       Constants.Sink.PLUGINTYPE, sinkConfig.getName()));
    }

    // Store transform id list to be serialized and passed to the driver program
    List<String> transformIds = Lists.newArrayListWithCapacity(transformConfigs.size());
    List<Transformation> transforms = Lists.newArrayListWithCapacity(transformConfigs.size());
    for (int i = 0; i < transformConfigs.size(); i++) {
      ETLStage transformConfig = transformConfigs.get(i);

      // Generate a transformId based on transform name and the array index (since there could
      // multiple transforms - ex, N filter transforms in the same pipeline)
      // stage number starts from 1, plus source is always #1, so add 2 for stage number.
      String transformId = PluginID.from(Constants.Transform.PLUGINTYPE, transformConfig.getName(), 2 + i).getID();
      PluginProperties transformProperties = getPluginProperties(transformConfig);
      Transform transformObj = usePlugin(Constants.Transform.PLUGINTYPE, transformConfig.getName(),
                                         transformId, transformProperties);
      if (transformObj == null) {
        throw new IllegalArgumentException(String.format("No Plugin of type '%s' named '%s' was found",
                                                         Constants.Transform.PLUGINTYPE, transformConfig.getName()));
      }

      transformIds.add(transformId);
      transforms.add(transformObj);
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(SOURCE_ID_KEY, sourcePluginId);
    properties.put(SINK_ID_KEY, sinkPluginId);
    properties.put(TRANSFORM_IDS_KEY, GSON.toJson(transformIds));
    if (config.getResources() != null) {
      properties.put(RESOURCES_KEY, GSON.toJson(config.getResources()));
    }
    setProperties(properties);

    // Validate Source -> Transform -> Sink hookup
    try {
      MapReduceConfigurer mrConfigurer = getConfigurer();
      PipelineValidator.validateStages(source, sink, transforms);
      PipelineConfigurer sourceConfigurer = new DefaultPipelineConfigurer(mrConfigurer, sourcePluginId);
      PipelineConfigurer sinkConfigurer = new DefaultPipelineConfigurer(mrConfigurer, sinkPluginId);
      source.configurePipeline(sourceConfigurer);
      sink.configurePipeline(sinkConfigurer);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();

    Map<String, String> properties = context.getSpecification().getProperties();
    String sourcePluginId = properties.get(SOURCE_ID_KEY);
    String sinkPluginId = properties.get(SINK_ID_KEY);

    batchSource = context.newInstance(sourcePluginId);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics, sourcePluginId);
    batchSource.prepareRun(sourceContext);

    batchSink = context.newInstance(sinkPluginId);
    BatchSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics, sinkPluginId);
    batchSink.prepareRun(sinkContext);

    if (properties.containsKey(RESOURCES_KEY)) {
      Resources resources = GSON.fromJson(properties.get(RESOURCES_KEY), Resources.class);
      context.setMapperResources(resources);
    }
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
    String sourcePluginId = context.getSpecification().getProperty(SOURCE_ID_KEY);
    BatchSourceContext sourceContext = new MapReduceSourceContext(context, mrMetrics, sourcePluginId);
    LOG.info("On RunFinish Source : {}", batchSource.getClass().getName());
    try {
      batchSource.onRunFinish(succeeded, sourceContext);
    } catch (Throwable t) {
      LOG.warn("Exception when calling onRunFinish on {}", batchSource, t);
    }
  }

  private void onRunFinishSink(MapReduceContext context, boolean succeeded) {
    String sinkPluginId = context.getSpecification().getProperty(SINK_ID_KEY);
    BatchSinkContext sinkContext = new MapReduceSinkContext(context, mrMetrics, sinkPluginId);
    LOG.info("On RunFinish Sink : {}", batchSink.getClass().getName());
    try {
      batchSink.onRunFinish(succeeded, sinkContext);
    } catch (Throwable t) {
      LOG.warn("Exception when calling onRunFinish on {}", batchSink, t);
    }
  }

  private PluginProperties getPluginProperties(ETLStage config) {
    PluginProperties.Builder builder = PluginProperties.builder();
    if (config.getProperties() != null) {
      builder.addAll(config.getProperties());
    }
    return builder.build();
  }

  /**
   * Mapper Driver for ETL Transforms.
   */
  public static class ETLMapper extends Mapper implements ProgramLifecycle<MapReduceContext> {
    private static final Logger LOG = LoggerFactory.getLogger(ETLMapper.class);
    private static final Gson GSON = new Gson();
    private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();

    private List<Transform> transforms;

    private TransformExecutor<KeyValue, KeyValue> transformExecutor;
    private Metrics mapperMetrics;

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      context.getSpecification().getProperties();
      Map<String, String> properties = context.getSpecification().getProperties();

      String sourcePluginId = properties.get(SOURCE_ID_KEY);
      String sinkPluginId = properties.get(SINK_ID_KEY);
      List<String> transformIds = GSON.fromJson(properties.get(TRANSFORM_IDS_KEY), STRING_LIST_TYPE);
      transforms = Lists.newArrayListWithCapacity(transformIds.size());

      List<Transformation> pipeline = Lists.newArrayListWithCapacity(transformIds.size() + 2);
      List<StageMetrics> stageMetrics = Lists.newArrayListWithCapacity(pipeline.size());

      BatchSource source = context.newInstance(sourcePluginId);
      BatchSourceContext batchSourceContext = new MapReduceSourceContext(context, mapperMetrics, sourcePluginId);
      source.initialize(batchSourceContext);
      pipeline.add(source);
      stageMetrics.add(new StageMetrics(mapperMetrics, PluginID.from(sourcePluginId)));

      addTransforms(pipeline, stageMetrics, transformIds, context);

      BatchSink sink = context.newInstance(sinkPluginId);
      BatchSinkContext batchSinkContext = new MapReduceSinkContext(context, mapperMetrics, sinkPluginId);
      sink.initialize(batchSinkContext);
      pipeline.add(sink);
      stageMetrics.add(new StageMetrics(mapperMetrics, PluginID.from(sinkPluginId)));

      transformExecutor = new TransformExecutor<>(pipeline, stageMetrics);
    }

    private void addTransforms(List<Transformation> pipeline,
                               List<StageMetrics> stageMetrics,
                               List<String> transformIds,
                               MapReduceContext context) throws Exception {

      for (int i = 0; i < transformIds.size(); i++) {
        String transformId = transformIds.get(i);
        Transform transform = context.newInstance(transformId);
        BatchTransformContext transformContext = new BatchTransformContext(context, mapperMetrics, transformId);
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
