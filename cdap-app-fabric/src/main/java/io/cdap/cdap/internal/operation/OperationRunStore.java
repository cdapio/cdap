/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationMeta;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.cdap.store.StoreDefinition.OperationRunsStore;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Store for operation runs.
 */
public class OperationRunStore {

  private static final Gson GSON = new GsonBuilder().create();
  private static final String SMALLEST_POSSIBLE_STRING = "";

  private final StructuredTableContext context;
  private final Clock clock;

  public OperationRunStore(StructuredTableContext context) {
    this(context, Clock.systemUTC());
  }

  @VisibleForTesting
  OperationRunStore(StructuredTableContext context, Clock clock) {
    this.context = context;
    this.clock = clock;
  }

  /**
   * Create a new operation. If an operation with same id exist throw exception.
   *
   * @param runId {@link OperationRunId} for the run
   * @param detail the run details of the operation
   * @throws OperationRunAlreadyExistsException when a run with same id exist in namespace
   */
  public OperationRunDetail createOperationRun(OperationRunId runId, OperationRunDetail detail)
      throws OperationRunAlreadyExistsException, IOException {
    Optional<StructuredRow> row = getOperationRunInternal(runId);
    if (row.isPresent()) {
      OperationRunStatus status = OperationRunStatus.valueOf(
          row.get().getString(StoreDefinition.OperationRunsStore.STATUS_FIELD));
      throw new OperationRunAlreadyExistsException(runId.getRun(), status);
    }
    writeOperationRun(runId, detail);
    return detail;
  }

  /**
   * Update the resources of an operation run.
   *
   * @param runId {@link OperationRunId} for the run
   * @param resources updated resources for the run
   * @param sourceId the message id which is responsible for the update
   * @throws OperationRunNotFoundException run with id does not exist in namespace
   */
  public void updateOperationResources(OperationRunId runId, Set<OperationResource> resources,
      @Nullable byte[] sourceId) throws OperationRunNotFoundException, IOException {
    OperationRunDetail currentDetail = getRunDetail(runId);
    OperationRun currentRun = currentDetail.getRun();
    OperationRun updatedRun = OperationRun.builder(currentRun).setMetadata(
        OperationMeta.builder(currentRun.getMetadata()).setResources(resources).build()).build();
    OperationRunDetail updatedDetail = OperationRunDetail.builder(currentDetail)
        .setRun(updatedRun).setSourceId(sourceId).build();

    Collection<Field<?>> fields = getCommonUpdateFields(runId);
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.DETAILS_FIELD,
            GSON.toJson(updatedDetail)));

    StructuredTable operationRunsTable = getOperationRunsTable(context);
    operationRunsTable.update(fields);
  }

  /**
   * Update the status of an operation run.
   *
   * @param runId {@link OperationRunId} for the run
   * @param status new metadata of the run
   * @param sourceId the message id which is responsible for the update
   * @throws OperationRunNotFoundException run with id does not exist in namespace
   */
  public void updateOperationStatus(OperationRunId runId, OperationRunStatus status,
      @Nullable byte[] sourceId) throws OperationRunNotFoundException, IOException {
    OperationRunDetail currentDetail = getRunDetail(runId);
    OperationRun currentRun = currentDetail.getRun();
    OperationRun updatedRun = OperationRun.builder(currentRun)
        .setStatus(status).build();
    OperationRunDetail updatedDetail = OperationRunDetail.builder(currentDetail)
        .setRun(updatedRun).setSourceId(sourceId).build();

    Collection<Field<?>> fields = getCommonUpdateFields(runId);
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.DETAILS_FIELD,
            GSON.toJson(updatedDetail)));
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.STATUS_FIELD,
            GSON.toJson(status)));

    StructuredTable operationRunsTable = getOperationRunsTable(context);
    operationRunsTable.update(fields);
  }

  /**
   * Mark an operation run as failed.
   *
   * @param runId {@link OperationRunId} for the run
   * @param error error related to the failure
   * @param sourceId the message id which is responsible for the update
   * @throws OperationRunNotFoundException run with id does not exist in namespace
   */
  public void failOperationRun(OperationRunId runId, OperationError error, Instant endTime,
      @Nullable byte[] sourceId) throws OperationRunNotFoundException, IOException {
    OperationRunDetail currentDetail = getRunDetail(runId);
    OperationRun currentRun = currentDetail.getRun();
    OperationMeta updatedMeta = OperationMeta.builder(currentDetail.getRun().getMetadata())
        .setEndTime(endTime).build();
    OperationRun updatedRun = OperationRun.builder(currentRun)
        .setStatus(OperationRunStatus.FAILED).setError(error).setMetadata(updatedMeta).build();
    OperationRunDetail updatedDetail = OperationRunDetail.builder(currentDetail)
        .setRun(updatedRun).setSourceId(sourceId).build();

    Collection<Field<?>> fields = getCommonUpdateFields(runId);
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.DETAILS_FIELD,
            GSON.toJson(updatedDetail)));
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.STATUS_FIELD,
            GSON.toJson(OperationRunStatus.FAILED)));

    StructuredTable operationRunsTable = getOperationRunsTable(context);
    operationRunsTable.update(fields);
  }

  /**
   * Get an operation run by id.
   *
   * @param runId {@link OperationRunId} for the run
   * @throws OperationRunNotFoundException run with id does not exist in namespace
   */
  public OperationRunDetail getOperation(OperationRunId runId)
      throws OperationRunNotFoundException, IOException {
    return getOperationRunInternal(runId).map(this::rowToRunDetail)
        .orElseThrow(() -> new OperationRunNotFoundException(runId.getNamespace(), runId.getRun()));
  }


  /**
   * Scans operations. Allows to optionally set filters and implement pagination. For pagination set
   * {@link ScanOperationRunsRequest#getScanAfter()} to the last application id of the previous
   * page.
   *
   * @param request parameters defining filters
   * @param consumer {@link Consumer} to process each scanned run
   * @throws IOException if failed to scan the storage
   * @throws OperationRunNotFoundException if the
   *     {@link ScanOperationRunsRequest#getScanAfter()} operation run does not exist
   * @see ScanOperationRunsRequest#builder(ScanOperationRunsRequest) to create a next page / batch
   *     request
   */
  public String scanOperations(ScanOperationRunsRequest request,
      Consumer<OperationRunDetail> consumer) throws IOException, OperationRunNotFoundException {
    Range.Bound startBound = Range.Bound.INCLUSIVE;
    final Range.Bound endBound = Range.Bound.INCLUSIVE;
    Collection<Field<?>> startFields = new ArrayList<>();

    startFields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD,
            request.getNamespace())
    );
    if (request.getFilter().getOperationType() != null) {
      startFields.add(Fields.stringField(StoreDefinition.OperationRunsStore.TYPE_FIELD,
          request.getFilter().getOperationType().name())
      );
    }
    if (request.getFilter().getStatus() != null) {
      startFields.add(Fields.stringField(StoreDefinition.OperationRunsStore.STATUS_FIELD,
          request.getFilter().getStatus().name()));
    }

    Collection<Field<?>> endFields = startFields;

    if (request.getScanAfter() != null) {
      startBound = Range.Bound.EXCLUSIVE;
      startFields = getRangeFields(
          new OperationRunId(request.getNamespace(), request.getScanAfter()));
    }

    Range range = Range.create(endFields, endBound, startFields, startBound);
    StructuredTable table = getOperationRunsTable(context);
    String lastKey = null;

    try (CloseableIterator<StructuredRow> iterator = table.scan(range, request.getLimit(),
        StoreDefinition.OperationRunsStore.START_TIME_FIELD, SortOrder.DESC)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        lastKey = row.getString(StoreDefinition.OperationRunsStore.ID_FIELD);
        OperationRunDetail detail = rowToRunDetail(row);
        consumer.accept(detail);
      }
    }

    return lastKey;
  }

  /**
   * Returns runs with a given status for all namespaces.
   */
  public void scanOperationByStatus(OperationRunStatus status,
      Consumer<OperationRunDetail> consumer) throws IOException {
    try (CloseableIterator<StructuredRow> iterator = getOperationRunsTable(context).scan(
        Fields.stringField(OperationRunsStore.STATUS_FIELD, status.name())
    )) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        OperationRunDetail detail = rowToRunDetail(row);
        consumer.accept(detail);
      }
    }
  }

  private List<Field<?>> getRangeFields(OperationRunId runId)
      throws IOException, OperationRunNotFoundException {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD,
            runId.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.OperationRunsStore.ID_FIELD, runId.getRun()));

    Long startTime = getOperationRunInternal(runId)
        .map(r -> r.getLong(StoreDefinition.OperationRunsStore.START_TIME_FIELD))
        .orElseThrow(() -> new OperationRunNotFoundException(runId.getNamespace(), runId.getRun()));

    fields.add(
        Fields.longField(StoreDefinition.OperationRunsStore.START_TIME_FIELD, startTime));
    return fields;
  }

  private OperationRunDetail getRunDetail(OperationRunId runId)
      throws IOException, OperationRunNotFoundException {
    return getOperationRunInternal(runId)
        .map(this::rowToRunDetail)
        .orElseThrow(() -> new OperationRunNotFoundException(runId.getNamespace(), runId.getRun()));
  }

  private OperationRunDetail rowToRunDetail(StructuredRow row) {
    OperationRunDetail detail = GSON.fromJson(
        row.getString(StoreDefinition.OperationRunsStore.DETAILS_FIELD),
        OperationRunDetail.class
    );
    // RunId is not serialized hence we need to populate it from row
    String namespace = row.getString(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD);
    String id = row.getString(StoreDefinition.OperationRunsStore.ID_FIELD);
    return OperationRunDetail.builder(detail).setRunId(new OperationRunId(namespace, id)).build();
  }

  private Collection<Field<?>> getCommonUpdateFields(OperationRunId runId) {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD,
            runId.getNamespace()));
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.ID_FIELD, runId.getRun()));
    fields.add(
        Fields.longField(StoreDefinition.OperationRunsStore.UPDATE_TIME_FIELD, clock.millis()));
    return fields;
  }

  private Optional<StructuredRow> getOperationRunInternal(OperationRunId runId)
      throws IOException {
    StructuredTable operationRunsTable = getOperationRunsTable(context);
    Collection<Field<?>> key = ImmutableList.of(
        Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD,
            runId.getNamespace()),
        Fields.stringField(StoreDefinition.OperationRunsStore.ID_FIELD, runId.getRun())
    );
    return operationRunsTable.read(key);
  }

  private void writeOperationRun(OperationRunId runId, OperationRunDetail detail)
      throws IOException {
    Collection<Field<?>> fields = ImmutableList.of(
        Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD,
            runId.getNamespace()),
        Fields.stringField(StoreDefinition.OperationRunsStore.ID_FIELD, runId.getRun()),
        Fields.stringField(StoreDefinition.OperationRunsStore.TYPE_FIELD,
            detail.getRun().getType().name()),
        Fields.stringField(StoreDefinition.OperationRunsStore.STATUS_FIELD,
            detail.getRun().getStatus().name()),
        Fields.longField(StoreDefinition.OperationRunsStore.START_TIME_FIELD,
            detail.getRun().getMetadata().getCreateTime().toEpochMilli()),
        Fields.longField(StoreDefinition.OperationRunsStore.UPDATE_TIME_FIELD,
            System.currentTimeMillis()),
        Fields.stringField(StoreDefinition.OperationRunsStore.DETAILS_FIELD, GSON.toJson(detail))
    );
    StructuredTable operationRunsTable = getOperationRunsTable(context);
    operationRunsTable.upsert(fields);
  }

  private StructuredTable getOperationRunsTable(StructuredTableContext context) {
    return context.getTable(StoreDefinition.OperationRunsStore.OPERATION_RUNS);
  }

  @VisibleForTesting
  // USE ONLY IN TESTS: WILL DELETE ALL OPERATION RUNS
  void clearData() throws IOException {
    StructuredTable table = getOperationRunsTable(context);
    table.deleteAll(
        Range.from(ImmutableList.of(
                Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD,
                    SMALLEST_POSSIBLE_STRING)),
            Range.Bound.INCLUSIVE));
  }
}
