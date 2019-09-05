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

package io.cdap.cdap.data2.metadata.lineage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Dataset to store/retrieve Dataset accesses of a Program.
 */
public class LineageTable {

  private static final Logger LOG = LoggerFactory.getLogger(LineageTable.class);

  private final StructuredTableContext structuredTableContext;
  private StructuredTable datasetTable;
  private StructuredTable programTable;

  /**
   * Gets an instance of {@link LineageTable}.
   *
   * @param context the {@link StructuredTableContext} for getting the lineage table.
   * @return an instance of {@link LineageTable}
   */
  @VisibleForTesting
  public static LineageTable create(StructuredTableContext context) {
    return new LineageTable(context);
  }

  private LineageTable(StructuredTableContext structuredTableContext) {
    this.structuredTableContext = structuredTableContext;
  }

  private StructuredTable getDatasetTable() {
    if (datasetTable == null) {
      try {
        datasetTable = structuredTableContext.getTable(StoreDefinition.LineageStore.DATASET_LINEAGE_TABLE);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return datasetTable;
  }

  private StructuredTable getProgramTable() {
    if (programTable == null) {
      try {
        programTable = structuredTableContext.getTable(StoreDefinition.LineageStore.PROGRAM_LINEAGE_TABLE);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return programTable;
  }

  @VisibleForTesting
  public void deleteAll() throws IOException {
    getDatasetTable().deleteAll(Range.all());
    getProgramTable().deleteAll(Range.all());
  }

  /**
   * Add a program-dataset access.
   *
   * @param run program run information
   * @param datasetInstance dataset accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   */
  public void addAccess(ProgramRunId run, DatasetId datasetInstance, AccessType accessType, long accessTimeMillis)
  throws IOException {
    LOG.trace("Recording access run={}, dataset={}, accessType={}, accessTime={}",
              run, datasetInstance, accessType, accessTimeMillis);
    List<Field<?>> datasetFields = getDatasetKey(datasetInstance, run, accessType);
    addAccessTime(datasetFields, accessTimeMillis);
    getDatasetTable().upsert(datasetFields);
    List<Field<?>> programFields = getProgramKey(run, datasetInstance, accessType);
    addAccessTime(programFields, accessTimeMillis);
    getProgramTable().upsert(programFields);
  }

  /**
   * @return a set of entities (program and data it accesses) associated with a program run.
   */
  public Set<NamespacedEntityId> getEntitiesForRun(ProgramRunId run) throws IOException {
    ImmutableSet.Builder<NamespacedEntityId> builder = ImmutableSet.builder();
    List<Field<?>> prefix = getRunScanStartKey(run);
    try (
      CloseableIterator<StructuredRow> iterator = getProgramTable().scan(Range.singleton(prefix), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        if (run.getRun().equals(row.getString(StoreDefinition.LineageStore.RUN_FIELD))) {
          builder.add(getProgramFromRow(row));
          builder.add(getDatasetFromRow(row));
        }
      }
    }
    return builder.build();
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
  public Set<Relation> getRelations(DatasetId datasetInstance, long start, long end, Predicate<Relation> filter)
    throws IOException {
    return scanRelations(getDatasetTable(),
                         getDatasetScanStartKey(datasetInstance, end),
                         getDatasetScanEndKey(datasetInstance, start),
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
  public Set<Relation> getRelations(ProgramId program, long start, long end, Predicate<Relation> filter)
    throws IOException {
    return scanRelations(getProgramTable(),
                         getProgramScanStartKey(program, end),
                         getProgramScanEndKey(program, start),
                         filter);
  }

  /**
   * @return a set of access times (for program and data it accesses) associated with a program run.
   */
  @VisibleForTesting
  public List<Long> getAccessTimesForRun(ProgramRunId run) throws IOException {
    ImmutableList.Builder<Long> builder = ImmutableList.builder();
    List<Field<?>> prefix = getRunScanStartKey(run);
    try (
      CloseableIterator<StructuredRow> iterator = getProgramTable().scan(Range.singleton(prefix), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        if (run.getRun().equals(row.getString(StoreDefinition.LineageStore.RUN_FIELD))) {
          builder.add(row.getLong(StoreDefinition.LineageStore.ACCESS_TIME_FIELD));
        }
      }
    }
    return builder.build();
  }

  private Set<Relation> scanRelations(StructuredTable table, List<Field<?>> startKey, List<Field<?>> endKey,
                                      Predicate<Relation> filter) throws IOException {
    ImmutableSet.Builder<Relation> builder = ImmutableSet.builder();
    try (CloseableIterator<StructuredRow> iterator =
      table.scan(Range.create(startKey, Range.Bound.INCLUSIVE, endKey, Range.Bound.INCLUSIVE), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        Relation relation = toRelation(row);
        if (filter.test(relation)) {
          builder.add(relation);
        }
      }
    }
    return builder.build();
  }

  private List<Field<?>> getDatasetKey(DatasetId datasetInstance, ProgramRunId run, AccessType accessType) {
    List<Field<?>> fields = new ArrayList<>();
    addDataset(fields, datasetInstance);
    addDataKey(fields, run, accessType);
    return fields;
  }

  private void addDataKey(List<Field<?>> fields, ProgramRunId run, AccessType accessType) {
    long invertedStartTime = getInvertedStartTime(run);
    fields.add(Fields.longField(StoreDefinition.LineageStore.START_TIME_FIELD, invertedStartTime));
    addProgram(fields, run.getParent());
    fields.add(Fields.stringField(StoreDefinition.LineageStore.RUN_FIELD, run.getEntityName()));
    fields.add(Fields.stringField(StoreDefinition.LineageStore.ACCESS_TYPE_FIELD,
                                  Character.toString(accessType.getType())));
  }

  private List<Field<?>> getProgramKey(ProgramRunId run, DatasetId datasetInstance, AccessType accessType) {
    long invertedStartTime = getInvertedStartTime(run);
    List<Field<?>> fields = new ArrayList<>();
    addProgram(fields, run.getParent());
    fields.add(Fields.longField(StoreDefinition.LineageStore.START_TIME_FIELD, invertedStartTime));
    addDataset(fields, datasetInstance);
    fields.add(Fields.stringField(StoreDefinition.LineageStore.RUN_FIELD, run.getEntityName()));
    fields.add(Fields.stringField(StoreDefinition.LineageStore.ACCESS_TYPE_FIELD,
                                  Character.toString(accessType.getType())));

    return fields;
  }

  private void addAccessTime(List<Field<?>> fields, long accessTime) {
    fields.add(Fields.longField(StoreDefinition.LineageStore.ACCESS_TIME_FIELD, accessTime));
  }

  private List<Field<?>> getDatasetScanKey(DatasetId datasetInstance, long time) {
    long invertedStartTime = invertTime(time);
    List<Field<?>> fields = new ArrayList<>();
    addDataset(fields, datasetInstance);
    fields.add(Fields.longField(StoreDefinition.LineageStore.START_TIME_FIELD, invertedStartTime));
    return fields;
  }

  private List<Field<?>> getDatasetScanStartKey(DatasetId datasetInstance, long end) {
    // time is inverted, hence we need to have end time in start key.
    // Since end time is exclusive, add 1 to make it inclusive.
    return getDatasetScanKey(datasetInstance, end == Long.MAX_VALUE ? end : end + 1);
  }

  private List<Field<?>> getDatasetScanEndKey(DatasetId datasetInstance, long start) {
    // time is inverted, hence we need to have start time in end key.
    // Since start time is inclusive, subtract 1 to make it exclusive.
    return getDatasetScanKey(datasetInstance, start == 0 ? start : start - 1);
  }

  private List<Field<?>> getProgramScanKey(ProgramId program, long time) {
    long invertedStartTime = invertTime(time);
    List<Field<?>> fields = new ArrayList<>();
    addProgram(fields, program);
    fields.add(Fields.longField(StoreDefinition.LineageStore.START_TIME_FIELD, invertedStartTime));

    return fields;
  }

  private List<Field<?>> getProgramScanStartKey(ProgramId program, long end) {
    // time is inverted, hence we need to have end time in start key.
    // Since end time is exclusive, add 1 to make it inclusive (except when end is max long, which will overflow if +1)
    return getProgramScanKey(program, end == Long.MAX_VALUE ? end : end + 1);
  }

  private List<Field<?>> getProgramScanEndKey(ProgramId program, long start) {
    // time is inverted, hence we need to have start time in end key.
    // Since start time is inclusive, subtract 1 to make it exclusive.
    return getProgramScanKey(program, start == 0 ? start : start - 1);
  }

  private List<Field<?>> getRunScanStartKey(ProgramRunId run) {
    List<Field<?>> fields = new ArrayList<>();
    addProgram(fields, run.getParent());
    fields.add(Fields.longField(StoreDefinition.LineageStore.START_TIME_FIELD, getInvertedStartTime(run)));
    return fields;
  }

  private void addDataset(List<Field<?>> fields, DatasetId datasetInstance) {
      fields.add(Fields.stringField(StoreDefinition.LineageStore.NAMESPACE_FIELD, datasetInstance.getNamespace()));
      fields.add(Fields.stringField(StoreDefinition.LineageStore.DATASET_FIELD, datasetInstance.getEntityName()));
  }

  private void addProgram(List<Field<?>> fields, ProgramId program) {
      fields.add(Fields.stringField(StoreDefinition.LineageStore.PROGRAM_NAMESPACE_FIELD, program.getNamespace()));
      fields.add(Fields.stringField(StoreDefinition.LineageStore.PROGRAM_APPLICATION_FIELD,
                                    program.getParent().getEntityName()));
      fields.add(Fields.stringField(StoreDefinition.LineageStore.PROGRAM_TYPE_FIELD,
                                    program.getType().getCategoryName()));
      fields.add(Fields.stringField(StoreDefinition.LineageStore.PROGRAM_FIELD, program.getEntityName()));
  }

  private ProgramId getProgramFromRow(StructuredRow row) {
    return new ProgramId(row.getString(StoreDefinition.LineageStore.PROGRAM_NAMESPACE_FIELD),
                         row.getString(StoreDefinition.LineageStore.PROGRAM_APPLICATION_FIELD),
                         ProgramType.valueOfCategoryName(
                           row.getString(StoreDefinition.LineageStore.PROGRAM_TYPE_FIELD)),
                         row.getString(StoreDefinition.LineageStore.PROGRAM_FIELD));
  }

  private DatasetId getDatasetFromRow(StructuredRow row) {
    return new DatasetId(row.getString(StoreDefinition.LineageStore.NAMESPACE_FIELD),
                         row.getString(StoreDefinition.LineageStore.DATASET_FIELD));
  }

  private long invertTime(long time) {
    return Long.MAX_VALUE - time;
  }

  private long getInvertedStartTime(ProgramRunId run) {
    return invertTime(RunIds.getTime(RunIds.fromString(run.getRun()), TimeUnit.MILLISECONDS));
  }

  private Relation toRelation(StructuredRow row) {
    RunId runId = RunIds.fromString(row.getString(StoreDefinition.LineageStore.RUN_FIELD));
    LOG.trace("Got runId {}", runId);
    AccessType accessType =
      AccessType.fromType(row.getString(StoreDefinition.LineageStore.ACCESS_TYPE_FIELD).charAt(0));
    LOG.trace("Got access type {}", accessType);

    DatasetId datasetInstance = getDatasetFromRow(row);
    LOG.trace("Got datasetInstance {}", datasetInstance);

    ProgramId program = getProgramFromRow(row);
    LOG.trace("Got program {}", program);

    return new Relation(datasetInstance, program, accessType, runId);
  }
}
