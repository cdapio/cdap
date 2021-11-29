/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.mapreduce;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.SetMultimap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.mapreduce.MapReduceTaskContext;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.batch.BatchPhaseSpec;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.FieldOperationTypeAdapter;
import io.cdap.cdap.etl.common.LocationAwareMDCWrapperLogger;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.SetMultimapCodec;
import io.cdap.cdap.etl.common.submit.CompositeFinisher;
import io.cdap.cdap.etl.common.submit.Finisher;
import io.cdap.cdap.etl.exec.StageFailureException;
import io.cdap.cdap.etl.log.LogStageInjector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * MapReduce Driver for ETL Batch Applications.
 */
public class ETLMapReduce extends AbstractMapReduce {
  public static final String NAME = ETLMapReduce.class.getSimpleName();
  static final String MAP_KEY_CLASS = "cdap.etl.map.key.class";
  static final String MAP_VAL_CLASS = "cdap.etl.map.val.class";
  static final String RUNTIME_ARGS_KEY = "cdap.etl.runtime.args";
  static final String INPUT_ALIAS_KEY = "cdap.etl.source.alias.key";
  static final String SINK_OUTPUTS_KEY = "cdap.etl.sink.outputs";
  static final Type RUNTIME_ARGS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  static final Type INPUT_ALIAS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  static final Type SINK_OUTPUTS_TYPE = new TypeToken<Map<String, SinkOutput>>() { }.getType();
  private static final Type CONNECTOR_DATASETS_TYPE = new TypeToken<HashSet<String>>() { }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(ETLMapReduce.class);
  private static final Logger PIPELINE_LOG = new LocationAwareMDCWrapperLogger(LOG, Constants.EVENT_TYPE_TAG,
                                                                              Constants.PIPELINE_LIFECYCLE_TAG_VALUE);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .registerTypeAdapter(FieldOperation.class, new FieldOperationTypeAdapter())
    .create();

  private Finisher finisher;

  // injected by CDAP
  @SuppressWarnings("unused")
  private Metrics mrMetrics;

  // this is only visible at configure time, not at runtime
  private final BatchPhaseSpec phaseSpec;
  private final RuntimeConfigurer runtimeConfigurer;
  private final String deployedNamespace;

  private final Set<String> connectorDatasets;

  public ETLMapReduce(BatchPhaseSpec phaseSpec, Set<String> connectorDatasets,
                      @Nullable RuntimeConfigurer runtimeConfigurer, String deployedNamespace) {
    this.phaseSpec = phaseSpec;
    this.connectorDatasets = connectorDatasets;
    this.runtimeConfigurer = runtimeConfigurer;
    this.deployedNamespace = deployedNamespace;
  }

  @Override
  public void configure() {
    setName(phaseSpec.getPhaseName());
    setDescription("MapReduce phase executor. " + phaseSpec.getDescription());

    // register the plugins at program level so that the program can be failed by the platform early in case of
    // plugin requirements not being meet
    phaseSpec.getPhase().registerPlugins(getConfigurer(), runtimeConfigurer, deployedNamespace);

    // Set resources for mapper, reducer and driver
    setMapperResources(phaseSpec.getResources());
    setReducerResources(phaseSpec.getResources());
    setDriverResources(phaseSpec.getDriverResources());

    Set<String> sources = phaseSpec.getPhase().getSources();
    // Planner should make sure this never happens
    if (sources.isEmpty()) {
      throw new IllegalArgumentException(String.format(
        "Pipeline phase '%s' must contain at least one source but it has no sources.", phaseSpec.getPhaseName()));
    }
    if (phaseSpec.getPhase().getSinks().isEmpty()) {
      throw new IllegalArgumentException(String.format(
        "Pipeline phase '%s' must contain at least one sink but does not have any.", phaseSpec.getPhaseName()));
    }
    Set<StageSpec> reducers = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE,
                                                                   BatchJoiner.PLUGIN_TYPE);
    if (reducers.size() > 1) {
      throw new IllegalArgumentException(String.format(
        "Pipeline phase '%s' cannot contain more than one reducer but it has reducers '%s'.",
        phaseSpec.getPhaseName(), Joiner.on(',').join(reducers)));
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec));
    properties.put(Constants.CONNECTOR_DATASETS, GSON.toJson(connectorDatasets));
    setProperties(properties);
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    Map<String, String> properties = context.getSpecification().getProperties();
    if (Boolean.valueOf(properties.get(Constants.STAGE_LOGGING_ENABLED))) {
      LogStageInjector.start();
    }
    PipelineRuntime pipelineRuntime = new PipelineRuntime(context, mrMetrics);

    Job job = context.getHadoopJob();
    Configuration hConf = job.getConfiguration();

    BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);

    for (Map.Entry<String, String> pipelineProperty : phaseSpec.getPipelineProperties().entrySet()) {
      hConf.set(pipelineProperty.getKey(), pipelineProperty.getValue());
    }

    // should never happen if planner is correct
    Set<StageSpec> reducers = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE,
                                                                   BatchJoiner.PLUGIN_TYPE);
    if (reducers.size() > 1) {
      Iterator<StageSpec> reducerIter = reducers.iterator();
      StringBuilder reducersStr = new StringBuilder(reducerIter.next().getName());
      while (reducerIter.hasNext()) {
        reducersStr.append(",");
        reducersStr.append(reducerIter.next().getName());
      }
      throw new IllegalStateException("Found multiple reducers ( " + reducersStr + " ) in the same pipeline phase. " +
                                        "This means there was a bug in planning the pipeline when it was deployed. ");
    }

    job.setMapperClass(ETLMapper.class);
    if (reducers.isEmpty()) {
      job.setNumReduceTasks(0);
    } else {
      job.setReducerClass(ETLReducer.class);
    }

    // instantiate plugins and call their prepare methods
    Set<String> connectorDatasets = GSON.fromJson(properties.get(Constants.CONNECTOR_DATASETS),
                                                  CONNECTOR_DATASETS_TYPE);
    MacroEvaluator evaluator = new DefaultMacroEvaluator(pipelineRuntime.getArguments(),
                                                         context.getLogicalStartTime(),
                                                         context, context, context.getNamespace());
    MapReducePreparer preparer = new MapReducePreparer(context, mrMetrics, evaluator,
                                                       pipelineRuntime, connectorDatasets);
    List<Finisher> finishers = preparer.prepare(phaseSpec, job);
    finisher = new CompositeFinisher(finishers);
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void destroy() {
    boolean isSuccessful = getContext().getState().getStatus() == ProgramStatus.COMPLETED;
    if (finisher != null) {
      // this can be null if the initialize() method failed.
      finisher.onFinish(isSuccessful);
    }
    LOG.info("Batch Run finished : status = {}", getContext().getState());
  }

  /**
   * Mapper Driver for ETL Transforms.
   */
  public static class ETLMapper extends Mapper implements ProgramLifecycle<MapReduceTaskContext<Object, Object>> {

    private TransformRunner<Object, Object> transformRunner;
    // injected by CDAP
    @SuppressWarnings("unused")
    private Metrics mapperMetrics;

    @Override
    public void initialize(MapReduceTaskContext<Object, Object> context) throws Exception {
      // get source, transform, sink ids from program properties
      Map<String, String> properties = context.getSpecification().getProperties();
      if (Boolean.valueOf(properties.get(Constants.STAGE_LOGGING_ENABLED))) {
        LogStageInjector.start();
      }
      transformRunner = new TransformRunner<>(context, mapperMetrics);
    }

    @Override
    public void map(Object key, Object value, Mapper.Context context) {
      try {
        transformRunner.transform(key, value);
      } catch (StageFailureException e) {
        PIPELINE_LOG.error("{}", e.getMessage(), e.getCause());
        throw Throwables.propagate(e.getCause());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      transformRunner.destroy();
    }
  }

  /**
   * Reducer for a phase of an ETL pipeline.
   */
  public static class ETLReducer extends Reducer implements ProgramLifecycle<MapReduceTaskContext<Object, Object>> {

    // injected by CDAP
    @SuppressWarnings("unused")
    private Metrics reducerMetrics;
    private TransformRunner<Object, Iterator> transformRunner;

    @Override
    public void initialize(MapReduceTaskContext<Object, Object> context) throws Exception {
      // get source, transform, sink ids from program properties
      Map<String, String> properties = context.getSpecification().getProperties();
      if (Boolean.valueOf(properties.get(Constants.STAGE_LOGGING_ENABLED))) {
        LogStageInjector.start();
      }
      transformRunner = new TransformRunner<>(context, reducerMetrics);
    }

    @Override
    protected void reduce(Object key, Iterable values, Context context) {
      try {
        transformRunner.transform(key, values.iterator());
      } catch (StageFailureException e) {
        PIPELINE_LOG.error("{}", e.getMessage(), e.getCause());
        throw Throwables.propagate(e.getCause());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      transformRunner.destroy();
    }
  }
}
