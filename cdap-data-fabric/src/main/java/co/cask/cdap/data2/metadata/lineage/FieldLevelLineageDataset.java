/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetFieldId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FieldEntityId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamFieldId;
import com.google.common.annotations.VisibleForTesting;
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

/**
 * Dataset to store/retrieve field-level accesses of a Program.
 */
public class FieldLevelLineageDataset extends AbstractDataset {

  // Storage format for dataset field nodes
  // --------------------------------------
  //
  // Dataset field access from program:
  // -----------------------------------------------------------------------------
  // | d | <id.field> | <inverted-start-time> | p | <id.run>     | <access-type> |
  // -----------------------------------------------------------------------------
  // | p | <id.run>   | <inverted-start-time> | d | <id.field>   | <access-type> |
  // -----------------------------------------------------------------------------
  //
  //
  // Storage format for stream field nodes
  // -------------------------------------
  //
  // Stream field access from program:
  // ----------------------------------------------------------------------------
  // | s | <id.field> | <inverted-start-time> | p | <id.run>    | <access-type> |
  // ----------------------------------------------------------------------------
  // | p | <id.run>   | <inverted-start-time> | s | <id.field>  | <access-type> |
  // ----------------------------------------------------------------------------

  private static final Logger LOG = LoggerFactory.getLogger(FieldLevelLineageDataset.class);

  private static final byte[] ACCESS_TIME_COLS_BYTE = {'t'};

  private static final char DATASET_MARKER = 'd';
  private static final char STREAM_MARKER = 's';
  private static final char PROGRAM_MARKER = 'p';

  private Table accessRegistryTable;

  public FieldLevelLineageDataset(String instanceName, Table accessRegistryTable) {
    super(instanceName, accessRegistryTable);
    this.accessRegistryTable = accessRegistryTable;
  }

  /**
   * Add a program-dataset access.
   *
   * @param run              program run information
   * @param field            field accessed by the program
   * @param accessType       access type
   * @param accessTimeMillis time of access
   */
  public void addAccess(ProgramRunId run, FieldEntityId field, AccessType accessType, long accessTimeMillis) {
    LOG.trace("Recording access run={}, field={}, accessType={}, accessType={}",
              run, field, accessType, accessTimeMillis);
    accessRegistryTable.put(getFieldKey(field, run, accessType), ACCESS_TIME_COLS_BYTE,
                            Bytes.toBytes(accessTimeMillis));
    accessRegistryTable.put(getProgramKey(run, field, accessType), ACCESS_TIME_COLS_BYTE,
                            Bytes.toBytes(accessTimeMillis));
  }

  /**
   * @return a set of entities (program and fields it accesses) associated with a program run.
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
   * @return a set of access times (for program and fields it accesses) associated with a program run.
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

  private Set<FieldRelation> scanRelations(byte[] startKey, byte[] endKey, Predicate<FieldRelation> filter) {
    ImmutableSet.Builder<FieldRelation> relationsBuilder = ImmutableSet.builder();
    try (Scanner scanner = accessRegistryTable.scan(startKey, endKey)) {
      Row row;
      while ((row = scanner.next()) != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Got row key = {}", Bytes.toString(row.getRow()));
        }
        FieldRelation relation = toRelation(row);
        if (filter.apply(relation)) {
          relationsBuilder.add(relation);
        }
      }
    }
    return relationsBuilder.build();
  }

  private byte[] getFieldKey(FieldEntityId field, ProgramRunId run, AccessType accessType) {
    MDSKey.Builder builder = new MDSKey.Builder();
    addField(builder, field);
    long invertedStartTime = getInvertedStartTime(run);
    builder.add(invertedStartTime);
    addProgram(builder, run.getParent());
    builder.add(run.getEntityName());
    builder.add(accessType.getType());

    return builder.build().getKey();
  }

  private byte[] getProgramKey(ProgramRunId run, FieldEntityId field, AccessType accessType) {
    long invertedStartTime = getInvertedStartTime(run);
    MDSKey.Builder builder = new MDSKey.Builder();
    addProgram(builder, run.getParent());
    builder.add(invertedStartTime);
    addField(builder, field);
    builder.add(run.getEntityName());
    builder.add(accessType.getType());

    return builder.build().getKey();

  }

  private RowKey parseRow(Row row) {
    ProgramId program;
    FieldEntityId data;
    RunId runId;
    MDSKey.Splitter splitter = new MDSKey(row.getRow()).split();
    char marker = (char) splitter.getInt();
    LOG.trace("Got marker {}", marker);
    switch (marker) {
      case PROGRAM_MARKER:
        program = toProgramId(splitter);
        splitter.skipLong(); // inverted start time
        marker = (char) splitter.getInt();
        data = toFieldEntityId(splitter, marker);  // data
        runId = RunIds.fromString(splitter.getString());
        return new RowKey(program, data, runId);

      case DATASET_MARKER:
      case STREAM_MARKER:
        data = toFieldEntityId(splitter, marker);
        splitter.skipLong();
        splitter.getInt();
        program = toProgramId(splitter);
        runId = RunIds.fromString(splitter.getString());
        return new RowKey(program, data, runId);

      default:
        throw new IllegalStateException("Invalid row with marker " + marker);
    }
  }

  private byte[] getFieldScanKey(FieldEntityId field, long time) {
    long invertedStartTime = invertTime(time);
    MDSKey.Builder builder = new MDSKey.Builder();
    addField(builder, field);
    builder.add(invertedStartTime);

    return builder.build().getKey();
  }

  private byte[] getFieldScanStartKey(FieldEntityId field, long end) {
    // time is inverted, hence we need to have end time in start key.
    // Since end time is exclusive, add 1 to make it inclusive.
    return getFieldScanKey(field, end + 1);
  }

  private byte[] getFieldScanEndKey(FieldEntityId field, long start) {
    // time is inverted, hence we need to have start time in end key.
    // Since start time is inclusive, subtract 1 to make it exclusive.
    return getFieldScanKey(field, start - 1);
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

  private void addField(MDSKey.Builder keyBuilder, FieldEntityId field) {
    if (field instanceof DatasetFieldId) {
      keyBuilder.add(DATASET_MARKER)
        .add(field.getNamespace())
        .add(((DatasetFieldId) field).getDataset())
        .add(field.getEntityName());
    } else if (field instanceof StreamFieldId) {
      keyBuilder.add(STREAM_MARKER)
        .add(field.getNamespace())
        .add(((StreamFieldId) field).getStream())
        .add(field.getEntityName());
    } else {
      throw new IllegalArgumentException("Invalid instance of FieldEntityId");
    }
  }

  private void addProgram(MDSKey.Builder keyBuilder, ProgramId program) {
    keyBuilder.add(PROGRAM_MARKER)
      .add(program.getNamespace())
      .add(program.getParent().getEntityName())
      .add(program.getType().getCategoryName())
      .add(program.getEntityName());
  }

  private FieldEntityId toFieldEntityId(MDSKey.Splitter splitter, char marker) {
    switch (marker) {
      case DATASET_MARKER:
        return new DatasetFieldId(splitter.getString(), splitter.getString(), splitter.getString());

      case STREAM_MARKER:
        return new StreamFieldId(splitter.getString(), splitter.getString(), splitter.getString());

      default:
        throw new IllegalStateException("Invalid row with marker " + marker);
    }
  }

  private ProgramId toProgramId(MDSKey.Splitter splitter) {
    return new ProgramId(splitter.getString(), splitter.getString(),
                         ProgramType.valueOfCategoryName(splitter.getString()), splitter.getString());
  }

  private long invertTime(long time) {
    return Long.MAX_VALUE - time;
  }

  private long getInvertedStartTime(ProgramRunId run) {
    return invertTime(RunIds.getTime(RunIds.fromString(run.getEntityName()), TimeUnit.MILLISECONDS));
  }

  private void oneSplit(MDSKey.Splitter splitter, Map<Character, EntityId> rowInfo, int num) {
    char marker = (char) splitter.getInt();
    LOG.trace("Got marker {}", marker);
    NamespacedEntityId id = (marker == PROGRAM_MARKER) ? toProgramId(splitter) : toFieldEntityId(splitter, marker);
    LOG.trace("Got id{} {}", num, id);
    rowInfo.put(marker, id);
  }

  private FieldRelation toRelation(Row row) {
    Map<Character, EntityId> rowInfo = new HashMap<>(4);
    char marker;

    MDSKey.Splitter splitter = new MDSKey(row.getRow()).split();

    oneSplit(splitter, rowInfo, 1);

    splitter.skipLong(); // inverted time - not required for relation

    oneSplit(splitter, rowInfo, 2);

    RunId runId = RunIds.fromString(splitter.getString());
    LOG.trace("Got runId {}", runId);
    AccessType accessType = AccessType.fromType((char) splitter.getInt());
    LOG.trace("Got access type {}", accessType);
    FieldEntityId field = (FieldEntityId) ((rowInfo.containsKey(STREAM_MARKER)) ?
      rowInfo.get(STREAM_MARKER) : rowInfo.get(DATASET_MARKER));
    LOG.trace("Got field {}", field);
    ProgramId program = (ProgramId) rowInfo.get(PROGRAM_MARKER);
    LOG.trace("Got program {}", program);

    return new FieldRelation(field, program, accessType, runId);
  }

  private static final class RowKey {
    private final ProgramId program;
    private final FieldEntityId data;
    private final RunId runId;

    RowKey(ProgramId program, FieldEntityId data, RunId runId) {
      this.program = program;
      this.data = data;
      this.runId = runId;
    }

    public ProgramId getProgram() {
      return program;
    }

    public FieldEntityId getData() {
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
