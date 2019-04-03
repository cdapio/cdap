/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.lineage.field;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.proto.codec.OperationTypeAdapter;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.metadata.lineage.ProgramRunOperations;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Dataset to store/retrieve field level lineage information.
 */
public class FieldLineageTable {

  private static final Logger LOG = LoggerFactory.getLogger(FieldLineageTable.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
    .create();

  private static final String INCOMING_DIRECTION_MARKER = "i";
  private static final String OUTGOING_DIRECTION_MARKER = "o";

  private static final Type SET_FIELD_TYPE = new TypeToken<HashSet<String>>() { }.getType();
  private static final Type SET_ENDPOINT_FIELD_TYPE = new TypeToken<HashSet<EndPointField>>() { }.getType();
  private static final Type SET_OPERATION_TYPE = new TypeToken<HashSet<Operation>>() { }.getType();

  private final StructuredTableContext structuredTableContext;
  private StructuredTable endpointChecksumTable;
  private StructuredTable operationsTable;
  private StructuredTable destinationFieldsTable;
  private StructuredTable summaryFieldsTable;

  private FieldLineageTable(StructuredTableContext structuredTableContext) {
    this.structuredTableContext = structuredTableContext;
  }

  /**
   * Gets an instance of {@link FieldLineageTable}.
   *
   * @param structuredTableContext the {@link StructuredTableContext} for getting the table.
   * @return an instance of {@link FieldLineageTable}
   */
  @VisibleForTesting
  public static FieldLineageTable create(StructuredTableContext structuredTableContext) {
    return new FieldLineageTable(structuredTableContext);
  }

  private StructuredTable getEndpointChecksumTable() {
    if (endpointChecksumTable == null) {
      endpointChecksumTable =
        structuredTableContext.getTable(StoreDefinition.FieldLineageStore.ENDPOINT_CHECKSUM_TABLE);
    }
    return endpointChecksumTable;
  }

  private StructuredTable getOperationsTable() {
    if (operationsTable == null) {
      operationsTable =
        structuredTableContext.getTable(StoreDefinition.FieldLineageStore.OPERATIONS_TABLE);
    }
    return operationsTable;
  }

  private StructuredTable getDestinationFieldsTable() {
    if (destinationFieldsTable == null) {
      destinationFieldsTable =
        structuredTableContext.getTable(StoreDefinition.FieldLineageStore.DESTINATION_FIELDS_TABLE);
    }
    return destinationFieldsTable;
  }

  private StructuredTable getSummaryFieldsTable() {
    if (summaryFieldsTable == null) {
      summaryFieldsTable =
        structuredTableContext.getTable(StoreDefinition.FieldLineageStore.SUMMARY_FIELDS_TABLE);
    }
    return summaryFieldsTable;
  }

  /**
   * Store the field lineage information.
   *
   * @param info the field lineage information
   */
  public void addFieldLineageInfo(ProgramRunId programRunId, FieldLineageInfo info) throws IOException {
    long checksum = info.getChecksum();
    if (readOperations(checksum) == null) {
      writeOperation(checksum, info.getOperations());

      Map<EndPoint, Set<String>> destinationFields = info.getDestinationFields();
      for (Map.Entry<EndPoint, Set<String>> entry : destinationFields.entrySet()) {
        addDestinationEntry(checksum, entry.getKey(), GSON.toJson(entry.getValue()));
      }

      addSummary(checksum, INCOMING_DIRECTION_MARKER, info.getIncomingSummary());
      addSummary(checksum, OUTGOING_DIRECTION_MARKER, info.getOutgoingSummary());
    }

    addFieldLineageInfoReferenceRecords(programRunId, info);
  }

  @VisibleForTesting
  public void deleteAll() throws IOException {
    getEndpointChecksumTable().deleteAll(Range.all());
    getDestinationFieldsTable().deleteAll(Range.all());
    getOperationsTable().deleteAll(Range.all());
    getSummaryFieldsTable().deleteAll(Range.all());
  }

  @Nullable
  private Set<Operation> readOperations(long checksum) throws IOException {
    List<Field<?>> fields = getOperationsKey(checksum);
    Optional<StructuredRow> row = getOperationsTable().read(fields);
    if (!row.isPresent()) {
      return null;
    }
    return GSON.fromJson(row.get().getString(StoreDefinition.FieldLineageStore.OPERATIONS_FIELD), SET_OPERATION_TYPE);
  }

  private void writeOperation(long checksum, Set<Operation> operations) throws IOException {
    List<Field<?>> fields = getOperationsKey(checksum);
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.OPERATIONS_FIELD, GSON.toJson(operations)));

    getOperationsTable().upsert(fields);
  }

  private void addSummary(long checksum, String direction, Map<EndPointField, Set<EndPointField>> summary)
    throws IOException {
    for (Map.Entry<EndPointField, Set<EndPointField>> entry : summary.entrySet()) {
      addSummaryEntry(checksum, direction, entry.getKey(), GSON.toJson(entry.getValue()));
    }
  }

  /**
   * Add records referring to the common operation record having the given checksum.
   * Operations represent transformations from source endpoints to the destination endpoints.
   * From source perspective the operations are added as lineage in outgoing direction, while from
   * destination perspective they are added as lineage in incoming direction.
   *
   * @param programRunId program run for which lineage is to be added
   * @param info the FieldLineageInfo created by program run
   */
  private void addFieldLineageInfoReferenceRecords(ProgramRunId programRunId, FieldLineageInfo info)
    throws IOException {
    // For all the destinations, operations represents incoming lineage
    for (EndPoint destination : info.getDestinations()) {
      addOperationReferenceRecord(INCOMING_DIRECTION_MARKER, destination, programRunId, info.getChecksum());
    }

    // For all the sources, operations represents the outgoing lineage
    for (EndPoint source : info.getSources()) {
      addOperationReferenceRecord(OUTGOING_DIRECTION_MARKER, source, programRunId, info.getChecksum());
    }
  }

  private void addOperationReferenceRecord(String direction, EndPoint endPoint, ProgramRunId programRunId,
                                           long checksum) throws IOException {
    List<Field<?>> fields = getOperationReferenceRowKey(direction, endPoint, programRunId);
    fields.add(Fields.longField(StoreDefinition.FieldLineageStore.CHECKSUM_FIELD, checksum));
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.PROGRAM_RUN_FIELD, GSON.toJson(programRunId)));
    getEndpointChecksumTable().upsert(fields);
  }

  private void addSummaryEntry(long checksum, String direction, EndPointField endPointField, String data)
    throws IOException {
    List<Field<?>> fields = getSummaryKey(checksum, direction, endPointField);
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.DESTINATION_DATA_FIELD, data));
    getSummaryFieldsTable().upsert(fields);
  }

  private void addDestinationEntry(long checksum, EndPoint endPoint, String data) throws IOException {
    List<Field<?>> fields = getDestinationKeys(checksum, endPoint);
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.DESTINATION_DATA_FIELD, data));
    getDestinationFieldsTable().upsert(fields);
  }

  /**
   * Get the set of fields read and/or written to the EndPoint by field lineage {@link ReadOperation} and/or
   * {@link WriteOperation}, over the given time range.
   *
   * @param endPoint the EndPoint for which the fields need to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return set of fields read and/or written to a given EndPoint
   */
  public Set<String> getFields(EndPoint endPoint, long start, long end) throws IOException {
    // A EndPoint can either have incoming lineage information (when it acts as a sink) or outgoing lineage
    // information (when it act as a source). During the given time period it is possible that the EndPoint has been
    // used a source only or sink only or both. So we combine all the field information which we have for the
    // EndPoint.
    Set<String> fields = getDestinationFields(endPoint, start, end);
    fields.addAll(getSourceFields(endPoint, start, end));
    return fields;
  }

  private Set<String> getDestinationFields(EndPoint endPoint, long start, long end) throws IOException {
    Set<Long> checksums = getChecksumsWithProgramRunsInRange(INCOMING_DIRECTION_MARKER, endPoint, start, end).keySet();
    Set<String> result = new HashSet<>();
    for (long checksum : checksums) {
      List<Field<?>> keys = getDestinationKeys(checksum, endPoint);
      Optional<StructuredRow> row = getDestinationFieldsTable().read(keys);
      if (!row.isPresent()) {
        continue;
      }
      String value = row.get().getString(StoreDefinition.FieldLineageStore.DESTINATION_DATA_FIELD);
      Set<String> fields = null;
      try {
        fields = GSON.fromJson(value, SET_FIELD_TYPE);
      } catch (JsonSyntaxException e) {
        LOG.warn(String.format("Failed to parse json from checksum %d'.", checksum));
      }
      if (fields != null) {
        result.addAll(fields);
      }
    }
    return result;
  }

  private Set<String> getSourceFields(EndPoint endPoint, long start, long end) throws IOException {
    Set<Long> checksums = getChecksumsWithProgramRunsInRange(OUTGOING_DIRECTION_MARKER, endPoint, start, end).keySet();
    Set<String> fields = new HashSet<>();
    for (long checksum : checksums) {
      List<Field<?>> prefix = getSummaryPrefix(checksum, OUTGOING_DIRECTION_MARKER, endPoint);
      try (CloseableIterator<StructuredRow> iterator =
        getSummaryFieldsTable().scan(Range.singleton(prefix), Integer.MAX_VALUE)) {
        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();
          fields.add(row.getString(StoreDefinition.FieldLineageStore.ENDPOINT_FIELD));
        }
      }
    }
    return fields;
  }

  /**
   * Get the incoming summary for the specified EndPointField over a given time range.
   * Incoming summary consists of set of EndPointFields which participated in the computation
   * of the given EndPointField.
   *
   * @param endPointField the EndPointField for which incoming summary to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the set of EndPointFields
   */
  public Set<EndPointField> getIncomingSummary(EndPointField endPointField, long start, long end) throws IOException {
    return getSummary(INCOMING_DIRECTION_MARKER, endPointField, start, end);
  }

  /**
   * Get the outgoing summary for the specified EndPointField in a given time range.
   * Outgoing summary consists of set of EndPointFields which were computed from the
   * specified EndPointField.
   *
   * @param endPointField the EndPointField for which outgoing summary to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the set of EndPointFields
   */
  public Set<EndPointField> getOutgoingSummary(EndPointField endPointField, long start, long end) throws IOException {
    return getSummary(OUTGOING_DIRECTION_MARKER, endPointField, start, end);
  }

  private Set<EndPointField> getSummary(String direction, EndPointField endPointField, long start, long end)
    throws IOException {
    Set<Long> checksums = getChecksumsWithProgramRunsInRange(direction, endPointField.getEndPoint(),
                                                             start, end).keySet();
    Set<EndPointField> result = new HashSet<>();

    for (long checksum : checksums) {
      List<Field<?>> keys = getSummaryKey(checksum, direction, endPointField);
      Optional<StructuredRow> row = getSummaryFieldsTable().read(keys);
      if (!row.isPresent()) {
        continue;
      }
      String value = row.get().getString(StoreDefinition.FieldLineageStore.DESTINATION_DATA_FIELD);
      Set<EndPointField> endPointFields;
      try {
        endPointFields = GSON.fromJson(value, SET_ENDPOINT_FIELD_TYPE);
      } catch (JsonSyntaxException e) {
        LOG.warn(String.format("Failed to parse json from checksum %d.", checksum));
        continue;
      }
      if (endPointFields != null) {
        result.addAll(endPointFields);
      }
    }

    return result;
  }

  /**
   * Get the set of operations which were responsible for computing the fields
   * of the specified EndPoint over a given time range. Along with the operations, program
   * runs are also returned which performed these operations.
   *
   * @param endPoint the EndPoint for which incoming operations are to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the operations and program run information
   */
  public Set<ProgramRunOperations> getIncomingOperations(EndPoint endPoint, long start, long end) throws IOException {
    return getOperations(INCOMING_DIRECTION_MARKER, endPoint, start, end);
  }

  /**
   * Get the set of operations which were performed on the specified EndPoint to compute the
   * fields of the downstream EndPoints. Along with the operations, program runs are also returned
   * which performed these operations.
   *
   * @param endPoint the EndPoint for which outgoing operations are to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the operations and program run information
   */
  public Set<ProgramRunOperations> getOutgoingOperations(EndPoint endPoint, long start, long end) throws IOException {
    return getOperations(OUTGOING_DIRECTION_MARKER, endPoint, start, end);
  }

  private Set<ProgramRunOperations> getOperations(String direction, EndPoint endPoint, long start, long end)
    throws IOException {
    Map<Long, Set<ProgramRunId>> checksumsWithProgramRunsInRange =
      getChecksumsWithProgramRunsInRange(direction, endPoint, start, end);

    Set<ProgramRunOperations> result = new LinkedHashSet<>();

    for (Map.Entry<Long, Set<ProgramRunId>> entry : checksumsWithProgramRunsInRange.entrySet()) {
      long checksum = entry.getKey();
      List<Field<?>> keys = getOperationsKey(checksum);
      Optional<StructuredRow> row = getOperationsTable().read(keys);
      if (!row.isPresent()) {
        continue;
      }
      String value = row.get().getString(StoreDefinition.FieldLineageStore.OPERATIONS_FIELD);
      Set<Operation> operations;
      try {
        operations = GSON.fromJson(value, SET_OPERATION_TYPE);
      } catch (JsonSyntaxException e) {
        LOG.warn(String.format("Failed to parse json from checksum %d'. Ignoring operations.", checksum));
        continue;
      }

      if (operations != null) {
        result.add(new ProgramRunOperations(entry.getValue(), operations));
      }
    }

    return result;
  }

  private Map<Long, Set<ProgramRunId>> getChecksumsWithProgramRunsInRange(String direction, EndPoint endPoint,
                                                                          long start, long end) throws IOException {
    // time is inverted, hence we need to pass end-time for getting start key
    List<Field<?>> scanStartKey = getScanKey(direction, endPoint, end);
    // time is inverted, hence we need to pass start-time for getting end key
    List<Field<?>> scanEndKey = getScanKey(direction, endPoint, start);
    Map<Long, Set<ProgramRunId>> result = new LinkedHashMap<>();
    try (CloseableIterator<StructuredRow> iterator =
      getEndpointChecksumTable().scan(
        Range.create(scanStartKey, Range.Bound.INCLUSIVE, scanEndKey, Range.Bound.INCLUSIVE), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        long checksum = row.getLong(StoreDefinition.FieldLineageStore.CHECKSUM_FIELD);
        ProgramRunId programRunId =
          GSON.fromJson(row.getString(StoreDefinition.FieldLineageStore.PROGRAM_RUN_FIELD), ProgramRunId.class);
        Set<ProgramRunId> programRuns = result.computeIfAbsent(checksum, k -> new HashSet<>());
        programRuns.add(programRunId);
      }
    }
    return result;
  }

  private List<Field<?>> getScanKey(String direction, EndPoint endPoint, long time) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.DIRECTION_FIELD, direction));
    addEndPoint(fields, endPoint);
    // add 1 to the inverted time
    // Once the time is inverted, original end-time becomes the start time for the scan, however we should
    // not return the record corresponding to this start time (inverted end-time) since its exclusive, so add 1.
    // Similarly once the time is inverted, original start-time becomes the end time of the scan. Since we want to
    // return the records corresponding to this end time (inverted start-time), we add 1.
    long invertedTime = invertTime(time);
    invertedTime = invertedTime == Long.MAX_VALUE ? invertedTime : invertedTime + 1;
    fields.add(Fields.longField(StoreDefinition.FieldLineageStore.START_TIME_FIELD, invertedTime));
    return fields;
  }

  private List<Field<?>> getSummaryPrefix(long checksum, String direction, EndPoint endPoint) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.longField(StoreDefinition.FieldLineageStore.CHECKSUM_FIELD, checksum));
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.DIRECTION_FIELD, direction));
    addEndPoint(fields, endPoint);
    return fields;
  }

  private List<Field<?>> getOperationReferenceRowKey(String direction, EndPoint endPoint, ProgramRunId programRunId) {
    long invertedStartTime = getInvertedStartTime(programRunId);
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.DIRECTION_FIELD, direction));
    addEndPoint(fields, endPoint);
    fields.add(Fields.longField(StoreDefinition.FieldLineageStore.START_TIME_FIELD, invertedStartTime));
    return fields;
  }

  private List<Field<?>> getDestinationKeys(long checksum, EndPoint endPoint) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.longField(StoreDefinition.FieldLineageStore.CHECKSUM_FIELD, checksum));
    addEndPoint(fields, endPoint);
    return fields;
  }

  private long invertTime(long time) {
    return Long.MAX_VALUE - time;
  }

  private long getInvertedStartTime(ProgramRunId run) {
    return invertTime(RunIds.getTime(RunIds.fromString(run.getEntityName()), TimeUnit.MILLISECONDS));
  }

  private void addEndPoint(List<Field<?>> fields, EndPoint endPoint) {
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.ENDPOINT_NAMESPACE_FIELD, endPoint.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.ENDPOINT_NAME_FIELD, endPoint.getName()));
  }

  private List<Field<?>> getSummaryKey(long checksum, String direction, EndPointField endPointField) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.longField(StoreDefinition.FieldLineageStore.CHECKSUM_FIELD, checksum));
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.DIRECTION_FIELD, direction));
    addEndPoint(fields, endPointField.getEndPoint());
    fields.add(Fields.stringField(StoreDefinition.FieldLineageStore.ENDPOINT_FIELD, endPointField.getField()));
    return fields;
  }

  private List<Field<?>> getOperationsKey(long checksum) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.longField(StoreDefinition.FieldLineageStore.CHECKSUM_FIELD, checksum));
    return fields;
  }
}
