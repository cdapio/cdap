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

package co.cask.cdap.templates.etl.realtime;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSink;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultEmitter;
import co.cask.cdap.templates.etl.common.DefaultTransformContext;
import co.cask.cdap.templates.etl.common.TransformExecutor;
import co.cask.cdap.templates.etl.realtime.config.ETLRealtimeConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Worker driver for Realtime ETL Adapters.
 */
public class ETLWorker extends AbstractWorker {
  private static final Logger LOG = LoggerFactory.getLogger(ETLWorker.class);
  private static final Gson GSON = new Gson();
  private static final Type SPEC_LIST_TYPE = new TypeToken<List<StageSpecification>>() { }.getType();

  private String adapterName;
  private RealtimeSource source;
  private RealtimeSink sink;
  private List<Transform> transforms;
  private DatasetContext datasetContext;
  private TransformExecutor transformExecutor;
  private DefaultEmitter defaultEmitter;

  private volatile boolean running;

  @Override
  public void configure() {
    setName(ETLWorker.class.getSimpleName());
    setDescription("Worker Driver for Realtime ETL Adapters");
  }

  @Override
  public void initialize(final WorkerContext context) throws Exception {
    super.initialize(context);
    Map<String, String> runtimeArgs = context.getRuntimeArguments();

    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.ADAPTER_NAME));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.CONFIG_KEY));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Source.SPECIFICATION));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Sink.SPECIFICATION));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Transform.SPECIFICATIONS));

    adapterName = runtimeArgs.get(Constants.ADAPTER_NAME);
    transforms = Lists.newArrayList();
    final ETLRealtimeConfig config = GSON.fromJson(runtimeArgs.get(Constants.CONFIG_KEY), ETLRealtimeConfig.class);


    getContext().execute(new TxRunnable() {
      @Override
      public void run(DatasetContext dsContext) throws Exception {
        datasetContext = dsContext;
      }
    });

    initializeSource(context, config.getSource());
    initializeTransforms(context, config.getTransforms());

    // Execute within a transaction since Dataset operations could be performed in the Sink.
    getContext().execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetCtx) throws Exception {
        initializeSink(context, config.getSink());
      }
    });

    transformExecutor = new TransformExecutor(transforms);
    defaultEmitter = new DefaultEmitter();
  }

  private void initializeSource(WorkerContext context, ETLStage stage) throws Exception {
    StageSpecification spec = GSON.fromJson(context.getRuntimeArguments().get(Constants.Source.SPECIFICATION),
                                            StageSpecification.class);
    source = (RealtimeSource) Class.forName(spec.getClassName()).newInstance();
    WorkerSourceContext sourceContext = new WorkerSourceContext(context, stage, spec);
    LOG.info("Source Stage : {}", stage.getName());
    LOG.info("Source Class : {}", stage.getClass().getName());
    LOG.info("Specifications of Source : {}", spec);
    source.initialize(sourceContext);
  }

  private void initializeSink(WorkerContext context, ETLStage stage) throws Exception {
    StageSpecification spec = GSON.fromJson(context.getRuntimeArguments().get(Constants.Sink.SPECIFICATION),
                                            StageSpecification.class);
    sink = (RealtimeSink) Class.forName(spec.getClassName()).newInstance();
    WorkerSinkContext sinkContext = new WorkerSinkContext(context, stage, spec, datasetContext);
    LOG.info("Sink Stage : {}", stage.getName());
    LOG.info("Sink Class : {}", stage.getClass().getName());
    LOG.info("Specifications of Sink : {}", spec);
    sink.initialize(sinkContext);
  }

  private void initializeTransforms(WorkerContext context, List<ETLStage> stages) throws Exception {
    List<StageSpecification> specs = GSON.fromJson(context.getRuntimeArguments().get(
      Constants.Transform.SPECIFICATIONS), SPEC_LIST_TYPE);
    for (int i = 0; i < specs.size(); i++) {
      StageSpecification spec = specs.get(i);
      ETLStage stage = stages.get(i);
      try {
        Transform transform = (Transform) Class.forName(spec.getClassName()).newInstance();
        DefaultTransformContext transformContext = new DefaultTransformContext(spec, stage.getProperties());
        LOG.info("Transform Stage : {}", stage.getName());
        LOG.info("Transform Class : {}", stage.getClass().getName());
        LOG.info("Specifications of Transform : {}", spec);
        transform.initialize(transformContext);
        transforms.add(transform);
      } catch (ClassNotFoundException e) {
        LOG.error("Unable to load Transform : {}", spec.getClassName(), e);
        Throwables.propagate(e);
      } catch (InstantiationException e) {
        LOG.error("Unable to instantiate Transform : {}", spec.getClassName(), e);
        Throwables.propagate(e);
      } catch (IllegalAccessException e) {
        LOG.error("Error while creating instance of Transform : {}", spec.getClassName(), e);
        Throwables.propagate(e);
      }
    }
  }

  @Override
  public void run() {
    running = true;
    while (running) {
      final SourceState sourceState = new SourceState();

      // Fetch SourceState from State Table.
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          KeyValueTable stateTable = context.getDataset(ETLRealtimeTemplate.STATE_TABLE);
          byte[] stateBytes = stateTable.read(adapterName);
          if (stateBytes != null) {
            SourceState state = GSON.fromJson(Bytes.toString(stateBytes), SourceState.class);
            sourceState.setState(state.getState());
          }
        }
      });

      final SourceState nextState = source.poll(defaultEmitter, sourceState);
      for (Object sourceData : defaultEmitter) {
        try {
          final Iterable<Object> dataToSink = transformExecutor.runOneIteration(sourceData);
          getContext().execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {
              sink.write(dataToSink);

              //Persist sourceState
              KeyValueTable stateTable = context.getDataset(ETLRealtimeTemplate.STATE_TABLE);
              if (nextState != null) {
                stateTable.write(adapterName, GSON.toJson(nextState));
              }
            }
          });
        } catch (Exception e) {
          // Log a warning and continue.
          LOG.warn("Adapter {} : Exception thrown while processing data {}", adapterName, sourceData, e);
        }
      }
      defaultEmitter.reset();
    }
  }

  @Override
  public void stop() {
    running = false;
    source.destroy();
    for (Transform transform : transforms) {
      transform.destroy();
    }
    sink.destroy();
  }
}
