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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

  private static final Logger LOG = LoggerFactory.getLogger(LineageDataset.class);
  // Column used to store access time
  private static final byte[] ACCESS_TIME_COLS_BYTE = {'t'};

  private static final char DATASET_MARKER = 'd';
  private static final char PROGRAM_MARKER = 'p';
  private static final char FLOWLET_MARKER = 'f';
  private static final char STREAM_MARKER = 's';
  private static final char NONE_MARKER = '0';

  private Table accessRegistryTable;

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
  public void addAccess(Id.Run run, Id.DatasetInstance datasetInstance,
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
  public void addAccess(Id.Run run, Id.DatasetInstance datasetInstance,
                        AccessType accessType, long accessTimeMillis, @Nullable Id.NamespacedId component) {
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
  public void addAccess(Id.Run run, Id.Stream stream,
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
  public void addAccess(Id.Run run, Id.Stream stream,
                        AccessType accessType, long accessTimeMillis, @Nullable Id.NamespacedId component) {
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
  public Set<Id.NamespacedId> getEntitiesForRun(Id.Run run) {
    ImmutableSet.Builder<Id.NamespacedId> recordBuilder = ImmutableSet.builder();
    byte[] startKey = getRunScanStartKey(run);
    try (Scanner scanner = accessRegistryTable.scan(startKey, Bytes.stopKeyForPrefix(startKey))) {
      Row row;
      while ((row = scanner.next()) != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Got row key = {}", Bytes.toString(row.getRow()));
        }
        RowKey rowKey = parseRow(row);
        if (run.getId().equals(rowKey.getRunId().getId())) {
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
  public Set<Relation> getRelations(Id.DatasetInstance datasetInstance, long start, long end,
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
  public Set<Relation> getRelations(Id.Stream stream, long start, long end, Predicate<Relation> filter) {
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
  public Set<Relation> getRelations(Id.Program program, long start, long end, Predicate<Relation> filter) {
    return scanRelations(getProgramScanStartKey(program, end),
                         getProgramScanEndKey(program, start),
                         filter);
  }

  /**
   * @return a set of access times (for program and data it accesses) associated with a program run.
   */
  @VisibleForTesting
  public List<Long> getAccessTimesForRun(Id.Run run) {
    ImmutableList.Builder<Long> recordBuilder = ImmutableList.builder();
    byte[] startKey = getRunScanStartKey(run);
    try (Scanner scanner = accessRegistryTable.scan(startKey, Bytes.stopKeyForPrefix(startKey))) {
      Row row;
      while ((row = scanner.next()) != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Got row key = {}", Bytes.toString(row.getRow()));
        }
        RowKey rowKey = parseRow(row);
        if (run.getId().equals(rowKey.getRunId().getId())) {
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
        if (filter.apply(relation)) {
          relationsBuilder.add(relation);
        }
      }
    }
    return relationsBuilder.build();
  }

  private byte[] getDatasetKey(Id.DatasetInstance datasetInstance, Id.Run run,
                               AccessType accessType, @Nullable Id.NamespacedId component) {
    MDSKey.Builder builder = new MDSKey.Builder();
    addDataset(builder, datasetInstance);
    addDataKey(builder, run, accessType, component);
    return builder.build().getKey();
  }

  private byte[] getStreamKey(Id.Stream stream, Id.Run run,
                              AccessType accessType, @Nullable Id.NamespacedId component) {
    MDSKey.Builder builder = new MDSKey.Builder();
    addStream(builder, stream);
    addDataKey(builder, run, accessType, component);
    return builder.build().getKey();
  }

  private void addDataKey(MDSKey.Builder builder, Id.Run run,
                          AccessType accessType, @Nullable Id.NamespacedId component) {
    long invertedStartTime = getInvertedStartTime(run);
    builder.add(invertedStartTime);
    addProgram(builder, run.getProgram());
    builder.add(run.getId());
    builder.add(accessType.getType());
    addComponent(builder, component);
  }

  private byte[] getProgramKey(Id.Run run, Id.DatasetInstance datasetInstance,
                               AccessType accessType, @Nullable Id.NamespacedId component) {
    long invertedStartTime = getInvertedStartTime(run);
    MDSKey.Builder builder = new MDSKey.Builder();
    addProgram(builder, run.getProgram());
    builder.add(invertedStartTime);
    addDataset(builder, datasetInstance);
    builder.add(run.getId());
    builder.add(accessType.getType());
    addComponent(builder, component);

    return builder.build().getKey();
  }

  private byte[] getProgramKey(Id.Run run, Id.Stream stream,
                               AccessType accessType, @Nullable Id.NamespacedId component) {
    long invertedStartTime = getInvertedStartTime(run);
    MDSKey.Builder builder = new MDSKey.Builder();
    addProgram(builder, run.getProgram());
    builder.add(invertedStartTime);
    addStream(builder, stream);
    builder.add(run.getId());
    builder.add(accessType.getType());
    addComponent(builder, component);

    return builder.build().getKey();
  }

  private RowKey parseRow(Row row) {
    Id.Program program;
    Id.NamespacedId data;
    RunId runId;
    MDSKey.Splitter splitter = new MDSKey(row.getRow()).split();
    char marker = (char) splitter.getInt();
    LOG.trace("Got marker {}", marker);
    switch (marker) {
      case PROGRAM_MARKER:
        program = (Id.Program) toId(splitter, marker);
        splitter.skipLong(); // inverted start time
        marker = (char) splitter.getInt();
        data = toId(splitter, marker);  // data
        runId = RunIds.fromString(splitter.getString());
        return new RowKey(program, data, runId);

      case DATASET_MARKER:
      case STREAM_MARKER:
        data = toId(splitter, marker);
        splitter.skipLong(); // inverted start time
        marker = (char) splitter.getInt();
        program = (Id.Program) toId(splitter, marker);  // program
        runId = RunIds.fromString(splitter.getString());
        return new RowKey(program, data, runId);

      default:
        throw new IllegalStateException("Invalid row with marker " +  marker);
    }
  }

  private byte[] getDatasetScanKey(Id.DatasetInstance datasetInstance, long time) {
    long invertedStartTime = invertTime(time);
    MDSKey.Builder builder = new MDSKey.Builder();
    addDataset(builder, datasetInstance);
    builder.add(invertedStartTime);

    return builder.build().getKey();
  }

  private byte[] getDatasetScanStartKey(Id.DatasetInstance datasetInstance, long end) {
    // time is inverted, hence we need to have end time in start key.
    // Since end time is exclusive, add 1 to make it inclusive.
    return getDatasetScanKey(datasetInstance, end + 1);
  }

  private byte[] getDatasetScanEndKey(Id.DatasetInstance datasetInstance, long start) {
    // time is inverted, hence we need to have start time in end key.
    // Since start time is inclusive, subtract 1 to make it exclusive.
    return getDatasetScanKey(datasetInstance, start - 1);
  }

  private byte[] getStreamScanKey(Id.Stream stream, long time) {
    long invertedStartTime = invertTime(time);
    MDSKey.Builder builder = new MDSKey.Builder();
    addStream(builder, stream);
    builder.add(invertedStartTime);

    return builder.build().getKey();
  }

  private byte[] getStreamScanStartKey(Id.Stream stream, long end) {
    // time is inverted, hence we need to have end time in start key.
    // Since end time is exclusive, add 1 to make it inclusive.
    return getStreamScanKey(stream, end + 1);
  }

  private byte[] getStreamScanEndKey(Id.Stream stream, long start) {
    // time is inverted, hence we need to have start time in end key.
    // Since start time is inclusive, subtract 1 to make it exclusive.
    return getStreamScanKey(stream, start - 1);
  }

  private byte[] getProgramScanKey(Id.Program program, long time) {
    long invertedStartTime = invertTime(time);
    MDSKey.Builder builder = new MDSKey.Builder();
    addProgram(builder, program);
    builder.add(invertedStartTime);

    return builder.build().getKey();
  }

  private byte[] getProgramScanStartKey(Id.Program program, long end) {
    // time is inverted, hence we need to have end time in start key.
    // Since end time is exclusive, add 1 to make it inclusive.
    return getProgramScanKey(program, end + 1);
  }

  private byte[] getProgramScanEndKey(Id.Program program, long start) {
    // time is inverted, hence we need to have start time in end key.
    // Since start time is inclusive, subtract 1 to make it exclusive.
    return getProgramScanKey(program, start - 1);
  }

  private byte[] getRunScanStartKey(Id.Run run) {
    MDSKey.Builder builder = new MDSKey.Builder();
    addProgram(builder, run.getProgram());
    builder.add(getInvertedStartTime(run));
    return builder.build().getKey();
  }

  private void addDataset(MDSKey.Builder keyBuilder, Id.DatasetInstance datasetInstance) {
    keyBuilder.add(DATASET_MARKER)
      .add(datasetInstance.getNamespaceId())
      .add(datasetInstance.getId());
  }

  private void addStream(MDSKey.Builder keyBuilder, Id.Stream stream) {
    keyBuilder.add(STREAM_MARKER)
      .add(stream.getNamespaceId())
      .add(stream.getId());
  }

  private void addProgram(MDSKey.Builder keyBuilder, Id.Program program) {
    keyBuilder.add(PROGRAM_MARKER)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().getCategoryName())
      .add(program.getId());
  }

  private void addComponent(MDSKey.Builder keyBuilder, Id component) {
    if (component instanceof Id.Flow.Flowlet) {
      keyBuilder.add(FLOWLET_MARKER)
        .add(component.getId());
    } else {
      keyBuilder.add(NONE_MARKER);
    }
  }

  private Id.NamespacedId toId(MDSKey.Splitter splitter, char marker) {
    switch (marker) {
      case DATASET_MARKER:
        return Id.DatasetInstance.from(splitter.getString(), splitter.getString());

      case STREAM_MARKER:
        return Id.Stream.from(splitter.getString(), splitter.getString());

      case PROGRAM_MARKER:
        return Id.Program.from(splitter.getString(), splitter.getString(),
                               ProgramType.valueOfCategoryName(splitter.getString()),
                               splitter.getString());

      default: throw new IllegalStateException("Invalid row with marker " +  marker);
    }
  }

  private Id.NamespacedId toComponent(MDSKey.Splitter splitter, Id.Program program) {
    char marker = (char) splitter.getInt();
    switch (marker) {
      case NONE_MARKER:
        return null;

      case FLOWLET_MARKER :
        return Id.Flow.Flowlet.from(program.getApplication(), program.getId(), splitter.getString());

      default:
        throw new IllegalStateException("Invalid row with component marker " + marker);
    }
  }

  private long invertTime(long time) {
    return Long.MAX_VALUE - time;
  }

  private long getInvertedStartTime(Id.Run run) {
    return invertTime(RunIds.getTime(RunIds.fromString(run.getId()), TimeUnit.MILLISECONDS));
  }

  private Relation toRelation(Row row) {
    Map<Character, Id> rowInfo = new HashMap<>(4);

    MDSKey.Splitter splitter = new MDSKey(row.getRow()).split();
    char marker = (char) splitter.getInt();
    LOG.trace("Got marker {}", marker);
    Id id1 = toId(splitter, marker);
    LOG.trace("Got id1 {}", id1);
    rowInfo.put(marker, id1);

    splitter.skipLong(); // inverted time - not required for relation

    marker = (char) splitter.getInt();
    LOG.trace("Got marker {}", marker);
    Id id2 = toId(splitter, marker);
    LOG.trace("Got id2 {}", id1);
    rowInfo.put(marker, id2);

    RunId runId = RunIds.fromString(splitter.getString());
    LOG.trace("Got runId {}", runId);
    AccessType accessType = AccessType.fromType((char) splitter.getInt());
    LOG.trace("Got access type {}", accessType);

    Id.DatasetInstance datasetInstance = (Id.DatasetInstance) rowInfo.get(DATASET_MARKER);
    LOG.trace("Got datasetInstance {}", datasetInstance);
    Id.Stream stream = (Id.Stream) rowInfo.get(STREAM_MARKER);
    LOG.trace("Got stream {}", stream);

    Id.Program program = (Id.Program) rowInfo.get(PROGRAM_MARKER);
    LOG.trace("Got program {}", program);
    Id.NamespacedId component = toComponent(splitter, program);
    LOG.trace("Got component {}", component);

    if (stream == null) {
      return new Relation(datasetInstance,
                          program,
                          accessType,
                          runId,
                          component == null ? ImmutableSet.<Id.NamespacedId>of() : ImmutableSet.of(component));
    }

    return new Relation(stream,
                        program,
                        accessType,
                        runId,
                        component == null ? ImmutableSet.<Id.NamespacedId>of() : ImmutableSet.of(component));
  }

  private static final class RowKey {
    private final Id.Program program;
    private final Id.NamespacedId data;
    private final RunId runId;

    RowKey(Id.Program program, Id.NamespacedId data, RunId runId) {
      this.program = program;
      this.data = data;
      this.runId = runId;
    }

    public Id.Program getProgram() {
      return program;
    }

    public Id.NamespacedId getData() {
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
