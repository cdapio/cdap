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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.api.realtime.SourceState;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultEmitter;
import co.cask.cdap.etl.common.Destroyables;
import co.cask.cdap.etl.common.Pipeline;
import co.cask.cdap.etl.common.PipelineRegisterer;
import co.cask.cdap.etl.common.PluginID;
import co.cask.cdap.etl.common.SinkInfo;
import co.cask.cdap.etl.common.StageMetrics;
import co.cask.cdap.etl.common.StructuredRecordStringConverter;
import co.cask.cdap.etl.common.TransformDetail;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.TransformInfo;
import co.cask.cdap.etl.common.TransformResponse;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Worker driver for Realtime ETL Adapters.
 */
public class ETLWorker extends AbstractWorker {
  public static final String NAME = ETLWorker.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(ETLWorker.class);
  private static final Type TRANSFORMDETAILS_LIST_TYPE = new TypeToken<List<TransformInfo>>() { }.getType();
  private static final Type SINK_INFO_TYPE = new TypeToken<List<SinkInfo>>() { }.getType();
  private static final Gson GSON = new Gson();
  private static final String SEPARATOR = ":";
  private static final Schema ERROR_SCHEMA = Schema.recordOf(
    "error",
    Schema.Field.of(Constants.ErrorDataset.ERRCODE, Schema.of(Schema.Type.INT)),

    Schema.Field.of(Constants.ErrorDataset.ERRMSG, Schema.unionOf(Schema.of(Schema.Type.STRING),
                                                                  Schema.of(Schema.Type.NULL))),
    Schema.Field.of(Constants.ErrorDataset.INVALIDENTRY, Schema.of(Schema.Type.STRING)));

  // only visible at configure time
  private final ETLRealtimeConfig config;

  private RealtimeSource source;
  private List<RealtimeSink> sinks;
  private List<Metrics> transformMetrics;
  private TransformExecutor transformExecutor;
  private DefaultEmitter sourceEmitter;
  private String stateStoreKey;
  private byte[] stateStoreKeyBytes;
  private String appName;
  private Metrics metrics;
  private List<TransformDetail> transformationDetailList;
  private Map<String, String> tranformIdToDatasetName;
  private volatile boolean stopped;

  public ETLWorker(ETLRealtimeConfig config) {
    this.config = config;
  }

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Worker Driver for Realtime ETL Adapters");
    int instances = config.getInstances() != null ? config.getInstances() : 1;
    if (instances < 1) {
      throw new IllegalArgumentException("instances must be greater than 0.");
    }
    setInstances(instances);
    if (config.getResources() != null) {
      setResources(config.getResources());
    }

    PipelineRegisterer registerer = new PipelineRegisterer(getConfigurer(), "realtime");
    // using table dataset type for error dataset
    Pipeline pluginIDs = registerer.registerPlugins(config, Table.class, DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA, ERROR_SCHEMA.toString())
      .build(), false);

    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.Source.PLUGINID, pluginIDs.getSource());
    properties.put(Constants.Sink.PLUGINIDS, GSON.toJson(pluginIDs.getSinks()));
    properties.put(Constants.Transform.PLUGINIDS, GSON.toJson(pluginIDs.getTransforms()));
    // Generate unique id for this app creation.
    properties.put(Constants.Realtime.UNIQUE_ID, String.valueOf(System.currentTimeMillis()));
    setProperties(properties);
  }

  @Override
  public void initialize(final WorkerContext context) throws Exception {
    super.initialize(context);
    Map<String, String> properties = context.getSpecification().getProperties();
    appName = context.getApplicationSpecification().getName();
    Preconditions.checkArgument(properties.containsKey(Constants.Source.PLUGINID));
    Preconditions.checkArgument(properties.containsKey(Constants.Sink.PLUGINIDS));
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
    transformationDetailList = initializeTransforms(context);
    initializeSinks(context);

    transformExecutor = new TransformExecutor(transformationDetailList);
  }

  private void initializeSource(WorkerContext context) throws Exception {
    String sourcePluginId = context.getSpecification().getProperty(Constants.Source.PLUGINID);
    source = context.newPluginInstance(sourcePluginId);
    RealtimeContext sourceContext = new WorkerRealtimeContext(context, metrics, sourcePluginId);
    LOG.debug("Source Class : {}", source.getClass().getName());
    source.initialize(sourceContext);
    sourceEmitter = new DefaultEmitter(new StageMetrics(metrics, PluginID.from(sourcePluginId)));
  }

  @SuppressWarnings("unchecked")
  private void initializeSinks(WorkerContext context) throws Exception {
    List<SinkInfo> sinkInfos = GSON.fromJson(context.getSpecification().getProperty(Constants.Sink.PLUGINIDS),
                                                 SINK_INFO_TYPE);
    sinks = Lists.newArrayListWithCapacity(sinkInfos.size());

    for (SinkInfo sinkInfo : sinkInfos) {
      RealtimeSink sink = context.newPluginInstance(sinkInfo.getSinkId());
      RealtimeContext sinkContext = new WorkerRealtimeContext(context, metrics, sinkInfo.getSinkId());
      LOG.debug("Sink Class : {}", sink.getClass().getName());
      sink.initialize(sinkContext);
      sink = new TrackedRealtimeSink(sink, metrics, PluginID.from(sinkInfo.getSinkId()));
      sinks.add(sink);
    }
  }

  private List<TransformDetail> initializeTransforms(WorkerContext context) throws Exception {
    List<TransformInfo> transformInfos =
      GSON.fromJson(context.getSpecification().getProperty(Constants.Transform.PLUGINIDS), TRANSFORMDETAILS_LIST_TYPE);
    Preconditions.checkArgument(transformInfos != null);
    List<TransformDetail> transformDetailList = new ArrayList<>(transformInfos.size());
    tranformIdToDatasetName = new HashMap<>(transformInfos.size());

    for (int i = 0; i < transformInfos.size(); i++) {
      String transformId = transformInfos.get(i).getTransformId();
      try {
        Transform transform = context.newPluginInstance(transformId);
        RealtimeTransformContext transformContext = new RealtimeTransformContext(context, metrics, transformId);
        LOG.debug("Transform Class : {}", transform.getClass().getName());
        transform.initialize(transformContext);
        StageMetrics stageMetrics = new StageMetrics(metrics, PluginID.from(transformId));
        transformDetailList.add(new TransformDetail(transformId, transform, stageMetrics));
        if (transformInfos.get(i).getErrorDatasetName() != null) {
          tranformIdToDatasetName.put(transformId, transformInfos.get(i).getErrorDatasetName());
        }
      } catch (InstantiationException e) {
        LOG.error("Unable to instantiate Transform", e);
        Throwables.propagate(e);
      }
    }
    return transformDetailList;
  }

  @Override
  public void run() {
    final SourceState currentState = new SourceState();
    final SourceState nextState = new SourceState();
    final List<Object> dataToSink = Lists.newArrayList();
    final Map<String, List<InvalidEntry>> transformIdToErrorRecords = intializeTransformIdToErrorsList();
    Set<String> transformErrorsWithoutDataset = Sets.newHashSet();
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
          TransformResponse transformResponse = transformExecutor.runOneIteration(sourceData);
          while (transformResponse.getEmittedRecords().hasNext()) {
            dataToSink.add(transformResponse.getEmittedRecords().next());
          }

          Map<String, Collection<InvalidEntry>> entryMap = transformResponse.getMapTransformIdToErrorEmitter();

          for (Map.Entry<String, Collection<InvalidEntry>> entry : entryMap.entrySet()) {
            String transformId = entry.getKey();
            if (!tranformIdToDatasetName.containsKey(transformId)) {
              if (!transformErrorsWithoutDataset.contains(transformId)) {
                LOG.warn("Error records were emitted in transform {}, " +
                           "but error dataset is not configured for this transform", transformId);
              }
              continue;
            }
            transformIdToErrorRecords.get(transformId).addAll(entry.getValue());
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
                for (RealtimeSink sink : sinks) {
                  sink.write(dataToSink, defaultDataWriter);
                }
              }

              for (Map.Entry<String, List<InvalidEntry>> errorRecordEntry : transformIdToErrorRecords.entrySet()) {
                String transformId = errorRecordEntry.getKey();
                final String datasetName = tranformIdToDatasetName.get(transformId);
                Table errorTable = context.getDataset(datasetName);
                long timeInMillis = System.currentTimeMillis();
                byte[] currentTime = Bytes.toBytes(timeInMillis);
                String transformIdentifier = appName + SEPARATOR + transformId;
                for (InvalidEntry invalidEntry : errorRecordEntry.getValue()) {
                  // using random uuid as we want to write each record uniquely,
                  // but we are not concerned about the uuid while scanning later.
                  byte[] rowKey = Bytes.concat(currentTime,
                                               Bytes.toBytes(transformIdentifier), Bytes.toBytes(UUID.randomUUID()));
                  Put errorPut = constructErrorPut(rowKey, invalidEntry, timeInMillis);
                  errorTable.write(rowKey, errorPut);
                }
              }

              // Persist nextState if it is different from currentState
              if (!nextState.equals(currentState)) {
                KeyValueTable stateTable = context.getDataset(ETLRealtimeApplication.STATE_TABLE);
                stateTable.write(stateStoreKey, GSON.toJson(nextState));
              }

              // after running one iteration and succesfully writing to sinks and error datasets, reset the emitters.
              transformExecutor.resetEmitters();
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
        for (List<InvalidEntry> invalidEntryList : transformIdToErrorRecords.values()) {
          invalidEntryList.clear();
        }
      }
    }
  }

  private Map<String, List<InvalidEntry>> intializeTransformIdToErrorsList() {
    Map<String, List<InvalidEntry>> transformIdToErrorListMap = new HashMap<>();
    for (String transformId : tranformIdToDatasetName.keySet()) {
      transformIdToErrorListMap.put(transformId, new ArrayList<InvalidEntry>());
    }
    return transformIdToErrorListMap;
  }

  private Put constructErrorPut(byte[] rowKey, InvalidEntry entry, long timeInMillis) throws IOException {
    Put errorPut = new Put(rowKey);
    errorPut.add(Constants.ErrorDataset.ERRCODE, entry.getErrorCode());
    errorPut.add(Constants.ErrorDataset.TIMESTAMP, timeInMillis);
    if (entry.getInvalidRecord() instanceof StructuredRecord) {
      StructuredRecord record = (StructuredRecord) entry.getInvalidRecord();
      errorPut.add(Constants.ErrorDataset.INVALIDENTRY,
                   StructuredRecordStringConverter.toJsonString(record));
    } else {
      errorPut.add(Constants.ErrorDataset.INVALIDENTRY,
                   String.format("Error Entry is of type %s, only records of type " +
                                   "co.cask.cdap.api.data.format.StructuredRecord " +
                                   "is supported currently", entry.getInvalidRecord().getClass().getName()));
    }

    return errorPut;
  }

  @Override
  public void stop() {
    stopped = true;
  }

  @Override
  public void destroy() {
    Destroyables.destroyQuietly(source);
    Destroyables.destroyQuietly(transformExecutor);
    for (RealtimeSink sink : sinks) {
      Destroyables.destroyQuietly(sink);
    }
  }
}
