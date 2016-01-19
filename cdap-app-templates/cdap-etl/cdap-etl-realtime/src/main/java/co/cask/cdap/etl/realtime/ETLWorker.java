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
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.api.realtime.SourceState;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultEmitter;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.Destroyables;
import co.cask.cdap.etl.common.LoggedTransform;
import co.cask.cdap.etl.common.NoopMetrics;
import co.cask.cdap.etl.common.Pipeline;
import co.cask.cdap.etl.common.PipelineRegisterer;
import co.cask.cdap.etl.common.SinkInfo;
import co.cask.cdap.etl.common.TransformDetail;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.TransformInfo;
import co.cask.cdap.etl.common.TransformResponse;
import co.cask.cdap.etl.common.TxLookupProvider;
import co.cask.cdap.etl.log.LogStageInjector;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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

  @SuppressWarnings("unused")
  private Metrics metrics;

  private RealtimeSource<Object> source;
  private String sourceStageName;
  private Map<String, RealtimeSink> sinks;
  private TransformExecutor transformExecutor;
  private DefaultEmitter sourceEmitter;
  private String stateStoreKey;
  private byte[] stateStoreKeyBytes;
  private String appName;
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
    Pipeline pipeline = registerer.registerPlugins(config, Table.class, DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA, ERROR_SCHEMA.toString())
      .build(), false);

    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(pipeline));
    // Generate unique id for this app creation.
    properties.put(Constants.Realtime.UNIQUE_ID, String.valueOf(System.currentTimeMillis()));
    properties.put(Constants.STAGE_LOGGING_ENABLED, String.valueOf(config.isStageLoggingEnabled()));
    setProperties(properties);
  }

  @Override
  public void initialize(final WorkerContext context) throws Exception {
    if (Boolean.valueOf(context.getSpecification().getProperty(Constants.STAGE_LOGGING_ENABLED))) {
      LogStageInjector.start();
    }
    super.initialize(context);

    Map<String, String> properties = context.getSpecification().getProperties();
    appName = context.getApplicationSpecification().getName();
    Preconditions.checkArgument(properties.containsKey(Constants.PIPELINEID));
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

    Map<String, List<String>> connectionsMap =
      GSON.fromJson(properties.get(Constants.PIPELINEID), Pipeline.class).getConnections();
    Map<String, TransformDetail> transformationMap = new HashMap<>();
    DefaultEmitter defaultEmitter = new DefaultEmitter(metrics);

    initializeSource(context);

    initializeTransforms(context, transformationMap, connectionsMap);
    initializeSinks(context, transformationMap);
    List<String> startStages = new ArrayList<>();
    startStages.addAll(connectionsMap.get(sourceStageName));
    transformExecutor = new TransformExecutor(transformationMap, startStages);
  }

  private void initializeSource(WorkerContext context) throws Exception {
    String sourcePluginId =
      GSON.fromJson(context.getSpecification().getProperty(Constants.PIPELINEID), Pipeline.class).getSource();
    source = context.newPluginInstance(sourcePluginId);
    source = new LoggedRealtimeSource<>(sourcePluginId, source);
    WorkerRealtimeContext sourceContext = new WorkerRealtimeContext(
      context, metrics, new TxLookupProvider(context), sourcePluginId);
    sourceStageName = sourcePluginId;
    LOG.debug("Source Class : {}", source.getClass().getName());
    source.initialize(sourceContext);
    sourceEmitter = new DefaultEmitter(sourceContext.getMetrics());
  }

  @SuppressWarnings("unchecked")
  private void initializeSinks(WorkerContext context,
                               Map<String, TransformDetail> transformationMap) throws Exception {
    List<SinkInfo> sinkInfos = GSON.fromJson(context.getSpecification().getProperty(Constants.PIPELINEID),
                                             Pipeline.class).getSinks();
    sinks = new HashMap<>(sinkInfos.size());
    for (SinkInfo sinkInfo : sinkInfos) {
      String sinkName = sinkInfo.getSinkId();
      RealtimeSink sink = context.newPluginInstance(sinkName);
      sink = new LoggedRealtimeSink(sinkName, sink);
      WorkerRealtimeContext sinkContext = new WorkerRealtimeContext(
        context, metrics, new TxLookupProvider(context), sinkName);
      LOG.debug("Sink Class : {}", sink.getClass().getName());
      sink.initialize(sinkContext);
      sink = new TrackedRealtimeSink(sink, sinkContext.getMetrics());

      Transformation identityTransformation = new Transformation() {
        @Override
        public void transform(Object input, Emitter emitter) throws Exception {
          emitter.emit(input);
        }
      };

      // we use identity transformation to simplify executing transformation in pipeline (similar to ETLMapreduce),
      // since we want to emit metrics during write to sink and not during this transformation, we use NoOpMetrics.
      transformationMap.put(sinkInfo.getSinkId(),
                            new TransformDetail(identityTransformation,
                                                new NoopMetrics(),
                                                new ArrayList<String>()));
      sinks.put(sinkInfo.getSinkId(), sink);
    }
  }

  private void initializeTransforms(WorkerContext context,
                                    Map<String, TransformDetail> transformDetailMap,
                                    Map<String, List<String>> connectionsMap) throws Exception {
    List<TransformInfo> transformInfos =
      GSON.fromJson(context.getSpecification().getProperty(Constants.PIPELINEID), Pipeline.class).getTransforms();
    Preconditions.checkArgument(transformInfos != null);
    tranformIdToDatasetName = new HashMap<>(transformInfos.size());

    for (TransformInfo transformInfo : transformInfos) {
      String transformId = transformInfo.getTransformId();
      try {
        Transform<?, ?> transform = context.newPluginInstance(transformId);
        transform = new LoggedTransform<>(transformId, transform);
        WorkerRealtimeContext transformContext = new WorkerRealtimeContext(
          context, metrics, new TxLookupProvider(context), transformId);
        LOG.debug("Transform Class : {}", transform.getClass().getName());
        transform.initialize(transformContext);
        transformDetailMap.put(transformId,
                               new TransformDetail(transform, new DefaultStageMetrics(metrics, transformId),
                                                   connectionsMap.get(transformId)));
        if (transformInfo.getErrorDatasetName() != null) {
          tranformIdToDatasetName.put(transformId, transformInfo.getErrorDatasetName());
        }
      } catch (InstantiationException e) {
        LOG.error("Unable to instantiate Transform", e);
        Throwables.propagate(e);
      }
    }
  }

  @Override
  public void run() {
    final SourceState currentState = new SourceState();
    final SourceState nextState = new SourceState();
    final Map<String, List<Object>> dataToSink = new HashMap<>();
    boolean hasData = false;
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
      for (Object sourceData : sourceEmitter.getEntries()) {
        try {
          TransformResponse transformResponse = transformExecutor.runOneIteration(sourceData);

          for (Map.Entry<String, Collection<Object>> transformedValues :
            transformResponse.getSinksResults().entrySet()) {
            dataToSink.put(transformedValues.getKey(), new ArrayList<>());
            Iterator emitterIterator = transformedValues.getValue().iterator();
            while (emitterIterator.hasNext()) {
              if (!hasData) {
                hasData = true;
              }
              dataToSink.get(transformedValues.getKey()).add(emitterIterator.next());
            }
          }

          for (Map.Entry<String, Collection<InvalidEntry<Object>>> transformErrorsEntry :
            transformResponse.getMapTransformIdToErrorEmitter().entrySet()) {

            if (!transformErrorsWithoutDataset.contains(transformErrorsEntry.getKey())) {

              if (!tranformIdToDatasetName.containsKey(transformErrorsEntry.getKey())
                && !transformErrorsEntry.getValue().isEmpty()) {
                transformErrorsWithoutDataset.add(transformErrorsEntry.getKey());
                LOG.warn("Error records were emitted in transform {}, " +
                           "but error dataset is not configured for this transform", transformErrorsEntry.getKey());
              }
              if (tranformIdToDatasetName.containsKey(transformErrorsEntry.getKey())
                && !transformErrorsEntry.getValue().isEmpty()) {
                // add the errors
                if (!hasData && transformErrorsEntry.getValue().size() > 0) {
                  hasData = true;
                }
                transformIdToErrorRecords.get(transformErrorsEntry.getKey()).addAll(transformErrorsEntry.getValue());
              }
            }
          }
        } catch (Exception e) {
          LOG.warn("Exception thrown while processing data {}", sourceData, e);
        }
      }
      sourceEmitter.reset();

      // Start a Transaction if there is data to persist or if the Source state has changed.
      try {
        if (hasData || (!nextState.equals(currentState))) {
          getContext().execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {

              // Invoke the sink's write method if there is any object to be written.
              if (!dataToSink.isEmpty()) {
                DefaultDataWriter defaultDataWriter = new DefaultDataWriter(getContext(), context);
                for (Map.Entry<String, List<Object>> sinkEntry : dataToSink.entrySet()) {
                  sinks.get(sinkEntry.getKey()).write(sinkEntry.getValue(), defaultDataWriter);
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
              transformExecutor.resetEmitter();
            }
          });

          // Update the in-memory copy of the state only if the transaction succeeded.
          currentState.setState(nextState);
        }
      } catch (Exception e) {
        LOG.warn("Exception thrown during persisting of data", e);
      } finally {
        // Clear the persisted sink data (in case transaction failure occurred, we will poll the source with old state)
        hasData = false;
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
    for (RealtimeSink sink : sinks.values()) {
      Destroyables.destroyQuietly(sink);
    }
  }
}
