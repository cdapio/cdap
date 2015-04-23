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
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.templates.etl.api.TransformStage;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.api.realtime.RealtimeContext;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSink;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultEmitter;
import co.cask.cdap.templates.etl.common.StageMetrics;
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
  private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Gson GSON = new Gson();
  private static final String SEPARATOR = ":";

  private String adapterName;
  private RealtimeSource source;
  private RealtimeSink sink;
  private List<TransformStage> transforms;
  private List<Metrics> transformMetrics;
  private TransformExecutor transformExecutor;
  private DefaultEmitter sourceEmitter;
  private String stateStoreKey;
  private byte[] stateStoreKeyBytes;
  private Metrics metrics;

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
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Source.PLUGINID));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Sink.PLUGINID));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Transform.PLUGINIDS));
    Preconditions.checkArgument(runtimeArgs.containsKey(Constants.Realtime.UNIQUE_ID));

    adapterName = runtimeArgs.get(Constants.ADAPTER_NAME);
    stateStoreKey = String.format("%s%s%s", adapterName, SEPARATOR, runtimeArgs.get(Constants.Realtime.UNIQUE_ID));
    stateStoreKeyBytes = Bytes.toBytes(stateStoreKey);
    transforms = Lists.newArrayList();
    final ETLRealtimeConfig config = GSON.fromJson(runtimeArgs.get(Constants.CONFIG_KEY), ETLRealtimeConfig.class);

    // Cleanup the rows in statetable for runs with same adapter name but other runids.
    getContext().execute(new TxRunnable() {
      @Override
      public void run(DatasetContext dsContext) throws Exception {
        KeyValueTable stateTable = dsContext.getDataset(ETLRealtimeTemplate.STATE_TABLE);
        byte[] startKey = Bytes.toBytes(String.format("%s%s", adapterName, SEPARATOR));
        // Scan the table for adaptername: prefixes and remove rows which doesn't match the unique id of this adapter.
        CloseableIterator<KeyValue<byte[], byte[]>> rows = stateTable.scan(startKey, Bytes.stopKeyForPrefix(startKey));
        try {
          while (rows.hasNext()) {
            KeyValue<byte[], byte[]> row = rows.next();
            if (Bytes.compareTo(stateStoreKeyBytes, row.getKey()) != 0) {
              stateTable.delete(row.getKey());
            }
          }
        } finally {
          rows.close();
        }
      }
    });

    initializeSource(context, config.getSource());
    initializeTransforms(context, config.getTransforms());
    initializeSink(context, config.getSink());

    transformExecutor = new TransformExecutor(transforms, transformMetrics);
  }

  private void initializeSource(WorkerContext context, ETLStage stage) throws Exception {
    String sourcePluginId = context.getRuntimeArguments().get(Constants.Source.PLUGINID);
    source = context.newPluginInstance(sourcePluginId);
    RealtimeContext sourceContext = new WorkerRealtimeContext(context, metrics, sourcePluginId);
    LOG.info("Source Stage : {}", stage.getName());
    LOG.info("Source Class : {}", source.getClass().getName());
    source.initialize(sourceContext);
    sourceEmitter = new DefaultEmitter(new StageMetrics(metrics, StageMetrics.Type.SOURCE, stage.getName()));
  }

  @SuppressWarnings("unchecked")
  private void initializeSink(WorkerContext context, ETLStage stage) throws Exception {
    String sinkPluginId = context.getRuntimeArguments().get(Constants.Sink.PLUGINID);
    sink = context.newPluginInstance(sinkPluginId);
    RealtimeContext sinkContext = new WorkerRealtimeContext(context, metrics, sinkPluginId);
    LOG.info("Sink Stage : {}", stage.getName());
    LOG.info("Sink Class : {}", sink.getClass().getName());
    sink.initialize(sinkContext);
    sink = new TrackedRealtimeSink(sink, metrics, stage.getName());
  }

  private void initializeTransforms(WorkerContext context, List<ETLStage> stages) {
    List<String> transformIds = GSON.fromJson(context.getRuntimeArguments().get(Constants.Transform.PLUGINIDS),
                                              STRING_LIST_TYPE);
    Preconditions.checkArgument(transformIds != null);
    Preconditions.checkArgument(stages.size() == transformIds.size());
    transformMetrics = Lists.newArrayListWithCapacity(stages.size());
    for (int i = 0; i < stages.size(); i++) {
      ETLStage stage = stages.get(i);
      String transformId = transformIds.get(i);
      try {
        TransformStage transform = context.newPluginInstance(transformId);
        RealtimeStageContext transformContext = new RealtimeStageContext(context, metrics, transformId);
        LOG.info("Transform Stage : {}", stage.getName());
        LOG.info("Transform Class : {}", transform.getClass().getName());
        transform.initialize(transformContext);
        transforms.add(transform);
        transformMetrics.add(new StageMetrics(metrics, StageMetrics.Type.TRANSFORM, stage.getName()));
      } catch (InstantiationException e) {
        LOG.error("Unable to instantiate Transform : {}", stage.getName(), e);
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
          byte[] stateBytes = stateTable.read(stateStoreKeyBytes);
          if (stateBytes != null) {
            SourceState state = GSON.fromJson(Bytes.toString(stateBytes), SourceState.class);
            sourceState.setState(state.getState());
          }
        }
      });

      final SourceState nextState = source.poll(sourceEmitter, sourceState);
      for (Object sourceData : sourceEmitter) {
        try {
          final Iterable<Object> dataToSink = transformExecutor.runOneIteration(sourceData);

          getContext().execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {
              DefaultDataWriter defaultDataWriter = new DefaultDataWriter(getContext(), context);
              sink.write(dataToSink, defaultDataWriter);

              //Persist sourceState
              KeyValueTable stateTable = context.getDataset(ETLRealtimeTemplate.STATE_TABLE);
              if (nextState != null) {
                stateTable.write(stateStoreKey, GSON.toJson(nextState));
              }
            }
          });
        } catch (Exception e) {
          // Log a warning and continue.
          LOG.warn("Adapter {} : Exception thrown while processing data {}", adapterName, sourceData, e);
        }
      }
      sourceEmitter.reset();
    }
  }

  @Override
  public void stop() {
    running = false;
    source.destroy();
    for (TransformStage transform : transforms) {
      transform.destroy();
    }
    sink.destroy();
  }
}
