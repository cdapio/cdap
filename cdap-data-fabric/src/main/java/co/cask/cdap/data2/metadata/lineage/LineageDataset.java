/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Dataset to store/retrieve Dataset accesses of a Program.
 */
public class LineageDataset extends AbstractDataset {

  // Storage format for row keys
  // ---------------------------
  //
  // Dataset access from program:
  // -------------------------------------------------------------------------------
  // | d | <id.dataset> | <inverted-start-time> | p | <id.run>     | <access-type> |
  // -------------------------------------------------------------------------------
  // | p | <id.run>     | <inverted-start-time> | p | <id.dataset> | <access-type> |
  // -------------------------------------------------------------------------------
  //
  // Stream access from program:
  // -------------------------------------------------------------------------------
  // | s | <id.stream>  | <inverted-start-time> | p | <id.run>     | <access-type> |
  // -------------------------------------------------------------------------------
  // | p | <id.run>     | <inverted-start-time> | s | <id.stream>  | <access-type> |
  // -------------------------------------------------------------------------------
  //
  // TMS message ID storage
  // --------------------
  // | t | <topic.name> |
  // --------------------

  public static final DatasetId LINEAGE_DATASET_ID = NamespaceId.SYSTEM.dataset("lineage");

  private static final Logger LOG = LoggerFactory.getLogger(LineageDataset.class);

  // Column used to store access time
  private static final byte[] ACCESS_TIME_COLS_BYTE = {'t'};
  private static final byte[] MESSAGE_ID_COLS_BYTE = {'m'};

  private static final char DATASET_MARKER = 'd';
  private static final char PROGRAM_MARKER = 'p';
  private static final char FLOWLET_MARKER = 'f';
  private static final char STREAM_MARKER = 's';
  private static final char TOPIC_MARKER = 't';
  private static final char NONE_MARKER = '0';

  private Table accessRegistryTable;

  /**
   * Adds datasets and types to the given {@link DatasetFramework}. Used by the upgrade tool to upgrade Lineage Dataset.
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(LineageDataset.class.getName(), LINEAGE_DATASET_ID, DatasetProperties.EMPTY);
  }

  /**
   * Gets an instance of {@link LineageDataset}. The dataset instance will be created if it is not yet exist.
   *
   * @param datasetContext the {@link DatasetContext} for getting the dataset instance.
   * @param datasetFramework the {@link DatasetFramework} for creating the dataset instance if missing
   * @return an instance of {@link LineageDataset}
   */
  public static LineageDataset getLineageDataset(DatasetContext datasetContext, DatasetFramework datasetFramework) {
    return getLineageDataset(datasetContext, datasetFramework, LINEAGE_DATASET_ID);
  }

  /**
   * Gets an instance of {@link LineageDataset}. The dataset instance will be created if it is not yet exist.
   *
   * @param datasetContext the {@link DatasetContext} for getting the dataset instance.
   * @param datasetFramework the {@link DatasetFramework} for creating the dataset instance if missing
   * @param datasetId the {@link DatasetId} of the {@link LineageDataset}
   * @return an instance of {@link LineageDataset}
   */
  @VisibleForTesting
  public static LineageDataset getLineageDataset(DatasetContext datasetContext,
                                                 DatasetFramework datasetFramework,
                                                 DatasetId datasetId) {
    try {
      return DatasetsUtil.getOrCreateDataset(datasetContext, datasetFramework, datasetId,
                                             LineageDataset.class.getName(), DatasetProperties.EMPTY);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public LineageDataset(String instanceName, Table accessRegistryTable) {
    super(instanceName, accessRegistryTable);
    this.accessRegistryTable = accessRegistryTable;
  }

  /**
   * Add a program-dataset access.
   *
   * @param run program run information
   * @param datasetInstance dataset accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   */
  public void addAccess(ProgramRunId run, DatasetId datasetInstance,
                        AccessType accessType, long accessTimeMillis) {
    addAccess(run, datasetInstance, accessType, accessTimeMillis, null);
  }
  /**
   * Add a program-dataset access.
   *
   * @param run program run information
   * @param datasetInstance dataset accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   * @param component program component such as flowlet id, etc.
   */
  public void addAccess(ProgramRunId run, DatasetId datasetInstance,
                        AccessType accessType, long accessTimeMillis, @Nullable NamespacedEntityId component) {
    LOG.trace("Recording access run={}, dataset={}, accessType={}, accessTime={}, component={}",
              run, datasetInstance, accessType, accessTimeMillis, component);
    accessRegistryTable.put(getDatasetKey(datasetInstance, run, accessType, component),
                            ACCESS_TIME_COLS_BYTE, Bytes.toBytes(accessTimeMillis));
    accessRegistryTable.put(getProgramKey(run, datasetInstance, accessType, component),
                            ACCESS_TIME_COLS_BYTE, Bytes.toBytes(accessTimeMillis));
  }

  /**
   * Add a program-stream access.
   *
   * @param run program run information
   * @param stream stream accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   */
  public void addAccess(ProgramRunId run, StreamId stream,
                        AccessType accessType, long accessTimeMillis) {
    addAccess(run, stream, accessType, accessTimeMillis, null);
  }

  /**
   * Add a program-stream access.
   *
   * @param run program run information
   * @param stream stream accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   * @param component program component such as flowlet id, etc.
   */
  public void addAccess(ProgramRunId run, StreamId stream,
                        AccessType accessType, long accessTimeMillis, @Nullable NamespacedEntityId component) {
    LOG.trace("Recording access run={}, stream={}, accessType={}, accessTime={}, component={}",
              run, stream, accessType, accessTimeMillis, component);
    accessRegistryTable.put(getStreamKey(stream, run, accessType, component),
                            ACCESS_TIME_COLS_BYTE, Bytes.toBytes(accessTimeMillis));
    accessRegistryTable.put(getProgramKey(run, stream, accessType, component),
                            ACCESS_TIME_COLS_BYTE, Bytes.toBytes(accessTimeMillis));
  }

  /**
   * @return a set of entities (program and data it accesses) associated with a program run.
   */
  public Set<NamespacedEntityId> getEntitiesForRun(ProgramRunId run) {
    ImmutableSet.Builder<NamespacedEntityId> recordBuilder = ImmutableSet.builder();
    byte[] startKey = getRunScanStartKey(run);
    try (Scanner scanner = accessRegistryTable.scan(startKey, Bytes.stopKeyForPrefix(startKey))) {
      Row row;
      while ((row = scanner.next()) != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Got row key = {}", Bytes.toString(row.getRow()));
        }
        RowKey rowKey = parseRow(row);
        if (run.getEntityName().equals(rowKey.getRunId().getId())) {
          recordBuilder.add(rowKey.getProgram());
          recordBuilder.add(rowKey.getData());
        }
      }
    }
    return recordBuilder.build();
  }

  /**
   * Fetch program-dataset access information for a dataset for a given period.
   *
   * @param datasetInstance dataset for which to fetch access information
   * @param start start time period
   * @param end end time period
   * @param filter filter to be applied on result set
   * @return program-dataset access information
   */
  public Set<Relation> getRelations(DatasetId datasetInstance, long start, long end,
                                    Predicate<Relation> filter) {
    return scanRelations(getDatasetScanStartKey(datasetInstance, end),
                         getDatasetScanEndKey(datasetInstance, start),
                         filter);
  }

  /**
   * Fetch program-stream access information for a dataset for a given period.
   *
   * @param stream stream for which to fetch access information
   * @param start start time period
   * @param end end time period
   * @param filter filter to be applied on result set
   * @return program-dataset access information
   */
  public Set<Relation> getRelations(StreamId stream, long start, long end, Predicate<Relation> filter) {
    return scanRelations(getStreamScanStartKey(stream, end),
                         getStreamScanEndKey(stream, start),
                         filter);
  }

  /**
   * Fetch program-dataset access information for a program for a given period.
   *
   * @param program program for which to fetch access information
   * @param start start time period
   * @param end end time period
   * @param filter filter to be applied on result set
   * @return program-dataset access information
   */
  public Set<Relation> getRelations(ProgramId program, long start, long end, Predicate<Relation> filter) {
    return scanRelations(getProgramScanStartKey(program, end),
                         getProgramScanEndKey(program, start),
                         filter);
  }

  /**
   * Loads the message id for the given topic.
   *
   * @param topicId the topic id to lookup for
   * @return the message id persisted earlier by {@link #storeMessageId(TopicId, String)} or {@code null}
   *         if there is no message id associated with the given topic
   */
  @Nullable
  public String loadMessageId(TopicId topicId) {
    return Bytes.toString(accessRegistryTable.get(getTopicKey(topicId), MESSAGE_ID_COLS_BYTE));
  }

  /**
   * Stores the given message id for the given topic.
   *
   * @param topicId the topic id to associate with the message id
   * @param messageId
   */
  public void storeMessageId(TopicId topicId, String messageId) {
    accessRegistryTable.put(getTopicKey(topicId), MESSAGE_ID_COLS_BYTE, Bytes.toBytes(messageId));
  }

  /**
   * @return a set of access times (for program and data it accesses) associated with a program run.
   */
  @VisibleForTesting
  public List<Long> getAccessTimesForRun(ProgramRunId run) {
    ImmutableList.Builder<Long> recordBuilder = ImmutableList.builder();
    byte[] startKey = getRunScanStartKey(run);
    try (Scanner scanner = accessRegistryTable.scan(startKey, Bytes.stopKeyForPrefix(startKey))) {
      Row row;
      while ((row = scanner.next()) != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Got row key = {}", Bytes.toString(row.getRow()));
        }
        RowKey rowKey = parseRow(row);
        if (run.getEntityName().equals(rowKey.getRunId().getId())) {
          recordBuilder.add(Bytes.toLong(row.get(ACCESS_TIME_COLS_BYTE)));
        }
      }
    }
    return recordBuilder.build();
  }

  private Set<Relation> scanRelations(byte[] startKey, byte[] endKey, Predicate<Relation> filter) {
    ImmutableSet.Builder<Relation> relationsBuilder = ImmutableSet.builder();
    try (Scanner scanner = accessRegistryTable.scan(startKey, endKey)) {
      Row row;
      while ((row = scanner.next()) != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Got row key = {}", Bytes.toString(row.getRow()));
        }
        Relation relation = toRelation(row);
        if (filter.test(relation)) {
          relationsBuilder.add(relation);
        }
      }
    }
    return relationsBuilder.build();
  }

  private byte[] getDatasetKey(DatasetId datasetInstance, ProgramRunId run,
                               AccessType accessType, @Nullable NamespacedEntityId component) {
    MDSKey.Builder builder = new MDSKey.Builder();
    addDataset(builder, datasetInstance);
    addDataKey(builder, run, accessType, component);
    return builder.build().getKey();
  }

  private byte[] getStreamKey(StreamId stream, ProgramRunId run,
                              AccessType accessType, @Nullable NamespacedEntityId component) {
    MDSKey.Builder builder = new MDSKey.Builder();
    addStream(builder, stream);
    addDataKey(builder, run, accessType, component);
    return builder.build().getKey();
  }

  private byte[] getTopicKey(TopicId topicId) {
    return new MDSKey.Builder()
      .add(TOPIC_MARKER)
      .add(topicId.getNamespace())
      .add(topicId.getTopic()).build().getKey();
  }

  private void addDataKey(MDSKey.Builder builder, ProgramRunId run,
                          AccessType accessType, @Nullable NamespacedEntityId component) {
    long invertedStartTime = getInvertedStartTime(run);
    builder.add(invertedStartTime);
    addProgram(builder, run.getParent());
    builder.add(run.getEntityName());
    builder.add(accessType.getType());
    addComponent(builder, component);
  }

  private byte[] getProgramKey(ProgramRunId run, DatasetId datasetInstance,
                               AccessType accessType, @Nullable NamespacedEntityId component) {
    long invertedStartTime = getInvertedStartTime(run);
    MDSKey.Builder builder = new MDSKey.Builder();
    addProgram(builder, run.getParent());
    builder.add(invertedStartTime);
    addDataset(builder, datasetInstance);
    builder.add(run.getEntityName());
    builder.add(accessType.getType());
    addComponent(builder, component);

    return builder.build().getKey();
  }

  private byte[] getProgramKey(ProgramRunId run, StreamId stream,
                               AccessType accessType, @Nullable NamespacedEntityId component) {
    long invertedStartTime = getInvertedStartTime(run);
    MDSKey.Builder builder = new MDSKey.Builder();
    addProgram(builder, run.getParent());
    builder.add(invertedStartTime);
    addStream(builder, stream);
    builder.add(run.getEntityName());
    builder.add(accessType.getType());
    addComponent(builder, component);

    return builder.build().getKey();
  }

  private RowKey parseRow(Row row) {
    ProgramId program;
    NamespacedEntityId data;
    RunId runId;
    MDSKey.Splitter splitter = new MDSKey(row.getRow()).split();
    char marker = (char) splitter.getInt();
    LOG.trace("Got marker {}", marker);
    switch (marker) {
      case PROGRAM_MARKER:
        program = (ProgramId) toEntityId(splitter, marker);
        splitter.skipLong(); // inverted start time
        marker = (char) splitter.getInt();
        data = toEntityId(splitter, marker);  // data
        runId = RunIds.fromString(splitter.getString());
        return new RowKey(program, data, runId);

      case DATASET_MARKER:
      case STREAM_MARKER:
        data = toEntityId(splitter, marker);
        splitter.skipLong(); // inverted start time
        marker = (char) splitter.getInt();
        program = (ProgramId) toEntityId(splitter, marker);  // program
        runId = RunIds.fromString(splitter.getString());
        return new RowKey(program, data, runId);

      default:
        throw new IllegalStateException("Invalid row with marker " +  marker);
    }
  }

  private byte[] getDatasetScanKey(DatasetId datasetInstance, long time) {
    long invertedStartTime = invertTime(time);
    MDSKey.Builder builder = new MDSKey.Builder();
    addDataset(builder, datasetInstance);
    builder.add(invertedStartTime);

    return builder.build().getKey();
  }

  private byte[] getDatasetScanStartKey(DatasetId datasetInstance, long end) {
    // time is inverted, hence we need to have end time in start key.
    // Since end time is exclusive, add 1 to make it inclusive.
    return getDatasetScanKey(datasetInstance, end + 1);
  }

  private byte[] getDatasetScanEndKey(DatasetId datasetInstance, long start) {
    // time is inverted, hence we need to have start time in end key.
    // Since start time is inclusive, subtract 1 to make it exclusive.
    return getDatasetScanKey(datasetInstance, start - 1);
  }

  private byte[] getStreamScanKey(StreamId stream, long time) {
    long invertedStartTime = invertTime(time);
    MDSKey.Builder builder = new MDSKey.Builder();
    addStream(builder, stream);
    builder.add(invertedStartTime);

    return builder.build().getKey();
  }

  private byte[] getStreamScanStartKey(StreamId stream, long end) {
    // time is inverted, hence we need to have end time in start key.
    // Since end time is exclusive, add 1 to make it inclusive.
    return getStreamScanKey(stream, end + 1);
  }

  private byte[] getStreamScanEndKey(StreamId stream, long start) {
    // time is inverted, hence we need to have start time in end key.
    // Since start time is inclusive, subtract 1 to make it exclusive.
    return getStreamScanKey(stream, start - 1);
  }

  private byte[] getProgramScanKey(ProgramId program, long time) {
    long invertedStartTime = invertTime(time);
    MDSKey.Builder builder = new MDSKey.Builder();
    addProgram(builder, program);
    builder.add(invertedStartTime);

    return builder.build().getKey();
  }

  private byte[] getProgramScanStartKey(ProgramId program, long end) {
    // time is inverted, hence we need to have end time in start key.
    // Since end time is exclusive, add 1 to make it inclusive.
    return getProgramScanKey(program, end + 1);
  }

  private byte[] getProgramScanEndKey(ProgramId program, long start) {
    // time is inverted, hence we need to have start time in end key.
    // Since start time is inclusive, subtract 1 to make it exclusive.
    return getProgramScanKey(program, start - 1);
  }

  private byte[] getRunScanStartKey(ProgramRunId run) {
    MDSKey.Builder builder = new MDSKey.Builder();
    addProgram(builder, run.getParent());
    builder.add(getInvertedStartTime(run));
    return builder.build().getKey();
  }

  private void addDataset(MDSKey.Builder keyBuilder, DatasetId datasetInstance) {
    keyBuilder.add(DATASET_MARKER)
      .add(datasetInstance.getNamespace())
      .add(datasetInstance.getEntityName());
  }

  private void addStream(MDSKey.Builder keyBuilder, StreamId stream) {
    keyBuilder.add(STREAM_MARKER)
      .add(stream.getNamespace())
      .add(stream.getEntityName());
  }

  private void addProgram(MDSKey.Builder keyBuilder, ProgramId program) {
    keyBuilder.add(PROGRAM_MARKER)
      .add(program.getNamespace())
      .add(program.getParent().getEntityName())
      .add(program.getType().getCategoryName())
      .add(program.getEntityName());
  }

  private void addComponent(MDSKey.Builder keyBuilder, EntityId component) {
    if (component instanceof FlowletId) {
      keyBuilder.add(FLOWLET_MARKER)
        .add(component.getEntityName());
    } else {
      keyBuilder.add(NONE_MARKER);
    }
  }

  private NamespacedEntityId toEntityId(MDSKey.Splitter splitter, char marker) {
    switch (marker) {
      case DATASET_MARKER:
        return new DatasetId(splitter.getString(), splitter.getString());

      case STREAM_MARKER:
        return new StreamId(splitter.getString(), splitter.getString());

      case PROGRAM_MARKER:
        return new ProgramId(splitter.getString(), splitter.getString(),
                               ProgramType.valueOfCategoryName(splitter.getString()),
                               splitter.getString());

      default: throw new IllegalStateException("Invalid row with marker " +  marker);
    }
  }

  @Nullable
  private NamespacedEntityId toComponent(MDSKey.Splitter splitter, ProgramId program) {
    char marker = (char) splitter.getInt();
    switch (marker) {
      case NONE_MARKER:
        return null;

      case FLOWLET_MARKER :
        return program.flowlet(splitter.getString());

      default:
        throw new IllegalStateException("Invalid row with component marker " + marker);
    }
  }

  private long invertTime(long time) {
    return Long.MAX_VALUE - time;
  }

  private long getInvertedStartTime(ProgramRunId run) {
    return invertTime(RunIds.getTime(RunIds.fromString(run.getEntityName()), TimeUnit.MILLISECONDS));
  }

  private Relation toRelation(Row row) {
    Map<Character, EntityId> rowInfo = new HashMap<>(4);

    MDSKey.Splitter splitter = new MDSKey(row.getRow()).split();
    char marker = (char) splitter.getInt();
    LOG.trace("Got marker {}", marker);
    EntityId id1 = toEntityId(splitter, marker);
    LOG.trace("Got id1 {}", id1);
    rowInfo.put(marker, id1);

    splitter.skipLong(); // inverted time - not required for relation

    marker = (char) splitter.getInt();
    LOG.trace("Got marker {}", marker);
    EntityId id2 = toEntityId(splitter, marker);
    LOG.trace("Got id2 {}", id1);
    rowInfo.put(marker, id2);

    RunId runId = RunIds.fromString(splitter.getString());
    LOG.trace("Got runId {}", runId);
    AccessType accessType = AccessType.fromType((char) splitter.getInt());
    LOG.trace("Got access type {}", accessType);

    DatasetId datasetInstance = (DatasetId) rowInfo.get(DATASET_MARKER);
    LOG.trace("Got datasetInstance {}", datasetInstance);
    StreamId stream = (StreamId) rowInfo.get(STREAM_MARKER);
    LOG.trace("Got stream {}", stream);

    ProgramId program = (ProgramId) rowInfo.get(PROGRAM_MARKER);
    LOG.trace("Got program {}", program);
    NamespacedEntityId component = toComponent(splitter, program);
    LOG.trace("Got component {}", component);

    if (stream == null) {
      return new Relation(datasetInstance,
                          program,
                          accessType,
                          runId,
                          component == null ?
                            ImmutableSet.<NamespacedEntityId>of() :
                            ImmutableSet.of((NamespacedEntityId) component));
    }

    return new Relation(stream,
                        program,
                        accessType,
                        runId,
                        component == null ?
                          ImmutableSet.<NamespacedEntityId>of() :
                          ImmutableSet.of((NamespacedEntityId) component));
  }

  private static final class RowKey {
    private final ProgramId program;
    private final NamespacedEntityId data;
    private final RunId runId;

    RowKey(ProgramId program, NamespacedEntityId data, RunId runId) {
      this.program = program;
      this.data = data;
      this.runId = runId;
    }

    public ProgramId getProgram() {
      return program;
    }

    public NamespacedEntityId getData() {
      return data;
    }

    public RunId getRunId() {
      return runId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RowKey)) {
        return false;
      }
      RowKey rowKey = (RowKey) o;
      return Objects.equals(program, rowKey.program) &&
        Objects.equals(data, rowKey.data) &&
        Objects.equals(runId, rowKey.runId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(program, data, runId);
    }

    @Override
    public String toString() {
      return "RowKey{" +
        "program=" + program +
        ", data=" + data +
        ", runId=" + runId +
        '}';
    }
  }
}
