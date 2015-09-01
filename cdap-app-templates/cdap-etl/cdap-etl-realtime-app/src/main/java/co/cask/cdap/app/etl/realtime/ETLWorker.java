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

package co.cask.cdap.app.etl.realtime;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.app.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.Transformation;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.api.realtime.RealtimeSink;
import co.cask.cdap.template.etl.api.realtime.RealtimeSource;
import co.cask.cdap.template.etl.api.realtime.SourceState;
import co.cask.cdap.template.etl.common.Constants;
import co.cask.cdap.template.etl.common.DefaultEmitter;
import co.cask.cdap.template.etl.common.Destroyables;
import co.cask.cdap.template.etl.common.Pipeline;
import co.cask.cdap.template.etl.common.PipelineRegisterer;
import co.cask.cdap.template.etl.common.PluginID;
import co.cask.cdap.template.etl.common.StageMetrics;
import co.cask.cdap.template.etl.common.TransformExecutor;
import co.cask.cdap.template.etl.realtime.RealtimeTransformContext;
import co.cask.cdap.template.etl.realtime.TrackedRealtimeSink;
import co.cask.cdap.template.etl.realtime.WorkerRealtimeContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Worker driver for Realtime ETL Adapters.
 */
public class ETLWorker extends AbstractWorker {
  public static final String NAME = ETLWorker.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(ETLWorker.class);
  private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Gson GSON = new Gson();
  private static final String SEPARATOR = ":";

  // only visible at configure time
  private final ETLRealtimeConfig config;

  private RealtimeSource source;
  private RealtimeSink sink;
  private List<Metrics> transformMetrics;
  private TransformExecutor transformExecutor;
  private DefaultEmitter sourceEmitter;
  private String stateStoreKey;
  private byte[] stateStoreKeyBytes;
  private Metrics metrics;

  private volatile boolean stopped;

  public ETLWorker(ETLRealtimeConfig config) {
    this.config = config;
  }

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Worker Driver for Realtime ETL Adapters");

    PipelineRegisterer registerer = new PipelineRegisterer(getConfigurer());
    Pipeline pluginIDs = registerer.registerPlugins(config);

    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.Source.PLUGINID, pluginIDs.getSource());
    properties.put(Constants.Sink.PLUGINID, pluginIDs.getSink());
    properties.put(Constants.Transform.PLUGINIDS, GSON.toJson(pluginIDs.getTransforms()));
    // Generate unique id for this app creation.
    properties.put(Constants.Realtime.UNIQUE_ID, String.valueOf(System.currentTimeMillis()));
    setProperties(properties);
  }

  @Override
  public void initialize(final WorkerContext context) throws Exception {
    super.initialize(context);
    Map<String, String> properties = context.getSpecification().getProperties();

    Preconditions.checkArgument(properties.containsKey(Constants.Source.PLUGINID));
    Preconditions.checkArgument(properties.containsKey(Constants.Sink.PLUGINID));
    Preconditions.checkArgument(properties.containsKey(Constants.Transform.PLUGINIDS));
    Preconditions.checkArgument(properties.containsKey(Constants.Realtime.UNIQUE_ID));

    String uniqueId = properties.get(Constants.Realtime.UNIQUE_ID);

    // Each worker instance should have its own unique state.
    final String appName = context.getApplicationSpecification().getName();
    stateStoreKey = String.format("%s%s%s%s%s", appName, SEPARATOR, uniqueId, SEPARATOR, context.getInstanceId());
    stateStoreKeyBytes = Bytes.toBytes(stateStoreKey);

    // Cleanup the rows in statetable for runs with same adapter name but other runids.
    getContext().execute(new TxRunnable() {
      @Override
      public void run(DatasetContext dsContext) throws Exception {
        KeyValueTable stateTable = dsContext.getDataset(ETLRealtimeApplication.STATE_TABLE);
        byte[] startKey = Bytes.toBytes(String.format("%s%s", appName, SEPARATOR));
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

    initializeSource(context);
    List<Transformation> transforms = initializeTransforms(context);
    initializeSink(context);

    transformExecutor = new TransformExecutor(transforms, transformMetrics);
  }

  private void initializeSource(WorkerContext context) throws Exception {
    String sourcePluginId = context.getSpecification().getProperty(Constants.Source.PLUGINID);
    source = context.newInstance(sourcePluginId);
    RealtimeContext sourceContext = new WorkerRealtimeContext(context, metrics, sourcePluginId);
    LOG.debug("Source Class : {}", source.getClass().getName());
    source.initialize(sourceContext);
    sourceEmitter = new DefaultEmitter(new StageMetrics(metrics, PluginID.from(sourcePluginId)));
  }

  @SuppressWarnings("unchecked")
  private void initializeSink(WorkerContext context) throws Exception {
    String sinkPluginId = context.getSpecification().getProperty(Constants.Sink.PLUGINID);
    sink = context.newInstance(sinkPluginId);
    RealtimeContext sinkContext = new WorkerRealtimeContext(context, metrics, sinkPluginId);
    LOG.debug("Sink Class : {}", sink.getClass().getName());
    sink.initialize(sinkContext);
    sink = new TrackedRealtimeSink(sink, metrics, PluginID.from(sinkPluginId));
  }

  private List<Transformation> initializeTransforms(WorkerContext context) throws Exception {
    List<String> transformIds = GSON.fromJson(context.getSpecification().getProperty(Constants.Transform.PLUGINIDS),
                                              STRING_LIST_TYPE);
    List<Transformation> transforms = Lists.newArrayList();

    Preconditions.checkArgument(transformIds != null);
    transformMetrics = Lists.newArrayListWithCapacity(transformIds.size());
    for (int i = 0; i < transformIds.size(); i++) {
      String transformId = transformIds.get(i);
      try {
        Transform transform = context.newInstance(transformId);
        RealtimeTransformContext transformContext = new RealtimeTransformContext(context, metrics, transformId);
        LOG.debug("Transform Class : {}", transform.getClass().getName());
        transform.initialize(transformContext);
        transforms.add(transform);
        transformMetrics.add(new StageMetrics(metrics, PluginID.from(transformId)));
      } catch (InstantiationException e) {
        LOG.error("Unable to instantiate Transform", e);
        Throwables.propagate(e);
      }
    }
    return transforms;
  }

  @Override
  public void run() {
    final SourceState currentState = new SourceState();
    final SourceState nextState = new SourceState();
    final List<Object> dataToSink = Lists.newArrayList();

    // Fetch SourceState from State Table.
    // Only required at the beginning since we persist the state if there is a change.
    getContext().execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        KeyValueTable stateTable = context.getDataset(ETLRealtimeApplication.STATE_TABLE);
        byte[] stateBytes = stateTable.read(stateStoreKeyBytes);
        if (stateBytes != null) {
          SourceState state = GSON.fromJson(Bytes.toString(stateBytes), SourceState.class);
          currentState.setState(state);
        }
      }
    });

    while (!stopped) {
      // Invoke poll method of the source to fetch data
      try {
        SourceState newState = source.poll(sourceEmitter, new SourceState(currentState));
        if (newState != null) {
          nextState.setState(newState);
        }
      } catch (Exception e) {
        // Continue since the source threw an exception. No point in processing records and state is not changed.
        LOG.warn("Exception thrown during polling of Source for data", e);
        sourceEmitter.reset();
        continue;
      }

      // For each object emitted by the source, invoke the transformExecutor and collect all the data
      // to be persisted in the sink.
      for (Object sourceData : sourceEmitter) {
        try {
          for (Object object : transformExecutor.runOneIteration(sourceData)) {
            dataToSink.add(object);
          }
        } catch (Exception e) {
          LOG.warn("Exception thrown while processing data {}", sourceData, e);
        }
      }
      sourceEmitter.reset();

      // Start a Transaction if there is data to persist or if the Source state has changed.
      try {
        if ((!dataToSink.isEmpty()) || (!nextState.equals(currentState))) {
          getContext().execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {

              // Invoke the sink's write method if there is any object to be written.
              if (!dataToSink.isEmpty()) {
                DefaultDataWriter defaultDataWriter = new DefaultDataWriter(getContext(), context);
                sink.write(dataToSink, defaultDataWriter);
              }

              // Persist nextState if it is different from currentState
              if (!nextState.equals(currentState)) {
                KeyValueTable stateTable = context.getDataset(ETLRealtimeApplication.STATE_TABLE);
                stateTable.write(stateStoreKey, GSON.toJson(nextState));
              }
            }
          });

          // Update the in-memory copy of the state only if the transaction succeeded.
          currentState.setState(nextState);
        }
      } catch (Exception e) {
        LOG.warn("Exception thrown during persisting of data", e);
      } finally {
        // Clear the persisted sink data (in case transaction failure occurred, we will poll the source with old state)
        dataToSink.clear();
      }
    }
  }

  @Override
  public void stop() {
    stopped = true;
  }

  @Override
  public void destroy() {
    Destroyables.destroyQuietly(source);
    Destroyables.destroyQuietly(transformExecutor);
    Destroyables.destroyQuietly(sink);
  }
}
