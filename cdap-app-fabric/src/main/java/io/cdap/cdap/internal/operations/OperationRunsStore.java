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

package io.cdap.cdap.internal.operations;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.app.store.OperationRunDetail;
import io.cdap.cdap.proto.operationrun.OperationError;
import io.cdap.cdap.proto.operationrun.OperationMeta;
import io.cdap.cdap.proto.operationrun.OperationRun;
import io.cdap.cdap.proto.operationrun.OperationRunStatus;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Store for operation runs.
 */
public class OperationRunsStore {

  private static final Gson GSON = new GsonBuilder().create();
  private final StructuredTableContext context;

  private static final String SMALLEST_POSSIBLE_STRING = "";

  @Inject
  public OperationRunsStore(StructuredTableContext context) {
    this.context = context;
  }

  /**
   * Create a new operation. If a operation with same id exist throw exception.
   *
   * @param namespace namespace of the operation
   * @param id id of the operation
   * @param detail the run details of the operation
   * @throws OperationRunAlreadyExistsException when a run with same id exist in namespace
   */
  public void createOperationRun(String namespace, String id, OperationRunDetail<?> detail)
      throws OperationRunAlreadyExistsException, IOException {
    Optional<StructuredRow> row = getOperationRunInternal(namespace, id);
    if (row.isPresent()) {
      OperationRunStatus status = OperationRunStatus.valueOf(
          row.get().getString(StoreDefinition.OperationRunsStore.STATUS_FIELD));
      throw new OperationRunAlreadyExistsException(id, status);
    }
    writeOperationRun(namespace, id, detail);
  }

  /**
   * Update the metadata of an operation run.
   *
   * @param namespace namespace of the operation
   * @param id id of the run
   * @param metadata new metdata of the run
   * @param sourceId the message id which is responsible for the update
   * @throws OperationRunNotFoundException run with id does not exist in namespace
   */
  public void updateOperationMeta(String namespace, String id, OperationMeta metadata,
      @Nullable byte[] sourceId) throws OperationRunNotFoundException, IOException {
    OperationRunDetail<?> currentDetail = getCurrentRunDetail(namespace, id);
    OperationRun currentRun = currentDetail.getRun();
    OperationRun updatedRun = OperationRun.builder(currentRun)
        .setMetadata(metadata).build();
    OperationRunDetail<?> updatedDetail = OperationRunDetail.builder(currentDetail)
        .setRun(updatedRun).setSourceId(sourceId).build();

    Collection<Field<?>> fields = getCommonUpdateFields(namespace, id);
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.DETAILS_FIELD,
            GSON.toJson(updatedDetail)));

    StructuredTable operationRunsTable = getOperationRunsTable(context);
    operationRunsTable.update(fields);
  }

  /**
   * Update the status of an operation run.
   *
   * @param namespace namespace of the operation
   * @param id id of the run
   * @param status new metadata of the run
   * @param sourceId the message id which is responsible for the update
   * @throws OperationRunNotFoundException run with id does not exist in namespace
   */
  public void updateOperationStatus(String namespace, String id, OperationRunStatus status,
      @Nullable byte[] sourceId) throws OperationRunNotFoundException, IOException {
    OperationRunDetail<?> currentDetail = getCurrentRunDetail(namespace, id);
    OperationRun currentRun = currentDetail.getRun();
    OperationRun updatedRun = OperationRun.builder(currentRun)
        .setStatus(status).build();
    OperationRunDetail<?> updatedDetail = OperationRunDetail.builder(currentDetail)
        .setRun(updatedRun).setSourceId(sourceId).build();

    Collection<Field<?>> fields = getCommonUpdateFields(namespace, id);
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
   * @param namespace namespace of the operation
   * @param id id of the run
   * @param error error related to the failure
   * @param sourceId the message id which is responsible for the update
   * @throws OperationRunNotFoundException run with id does not exist in namespace
   */
  public void failOperationRun(String namespace, String id, OperationError error,
      @Nullable byte[] sourceId) throws OperationRunNotFoundException, IOException {
    OperationRunDetail<?> currentDetail = getCurrentRunDetail(namespace, id);
    OperationRun currentRun = currentDetail.getRun();
    OperationRun updatedRun = OperationRun.builder(currentRun)
        .setStatus(OperationRunStatus.FAILED).setError(error).build();
    OperationRunDetail<?> updatedDetail = OperationRunDetail.builder(currentDetail)
        .setRun(updatedRun).setSourceId(sourceId).build();

    Collection<Field<?>> fields = getCommonUpdateFields(namespace, id);
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
   * @param namespace namespace of the operation
   * @param id id of the run
   * @throws OperationRunNotFoundException run with id does not exist in namespace
   */
  public OperationRunDetail<?> getOperation(String namespace, String id)
      throws NotFoundException, IOException {
    Optional<StructuredRow> row = getOperationRunInternal(namespace, id);
    if (!row.isPresent()) {
      throw new OperationRunNotFoundException(namespace, id);
    }
    return GSON.fromJson(
        row.get().getString(StoreDefinition.OperationRunsStore.DETAILS_FIELD),
        OperationRunDetail.class
    );
  }

  /**
   * Scan operations in a namespace.
   *
   * @param request scan request including filters and limit
   * @param txBatchSize batch size of transaction
   */
  public boolean scanRuns(ScanOperationRunsRequest request, int txBatchSize,
      BiConsumer<String, OperationRunDetail<?>> consumer)
      throws OperationRunNotFoundException, IOException {

    AtomicReference<ScanOperationRunsRequest> requestRef = new AtomicReference<>(request);
    AtomicReference<String> lastKey = new AtomicReference<>();
    AtomicInteger currentLimit = new AtomicInteger(request.getLimit());

    while (currentLimit.get() > 0) {
      AtomicInteger count = new AtomicInteger();

      scanRunsInternal(requestRef.get(), entry -> {
        lastKey.set(entry.getKey());
        currentLimit.decrementAndGet();
        consumer.accept(entry.getKey(), entry.getValue());
        return count.incrementAndGet() < txBatchSize && currentLimit.get() > 0;
      });

      if (lastKey.get() == null) {
        break;
      }

      ScanOperationRunsRequest nextBatchRequest = ScanOperationRunsRequest
          .builder(requestRef.get())
          .setScanFrom(lastKey.get())
          .setLimit(currentLimit.get())
          .build();
      requestRef.set(nextBatchRequest);
      lastKey.set(null);
    }
    return currentLimit.get() == 0;
  }


  private void scanRunsInternal(ScanOperationRunsRequest request,
      Function<Map.Entry<String, OperationRunDetail<?>>, Boolean> func)
      throws IOException, OperationRunNotFoundException {
    Range.Bound startBound = Range.Bound.INCLUSIVE;
    Range.Bound endBound = Range.Bound.INCLUSIVE;
    Collection<Field<?>> startFields = new ArrayList<>();

    startFields.add(Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD,
        request.getNamespace()));

    Collection<Field<?>> endFields = startFields;

    if (request.getScanFrom() != null) {
      startBound = Range.Bound.EXCLUSIVE;
      startFields = getRangeFields(request.getNamespace(), request.getScanFrom());
    }
    if (request.getScanTo() != null) {
      endBound = Range.Bound.EXCLUSIVE;
      endFields = getRangeFields(request.getNamespace(), request.getScanTo());
    }

    Range range = Range.create(endFields, endBound, startFields, startBound);
    Collection<Field<?>> filterIndexes = getFilterIndexes(request.getFilter());
    int limit = request.getLimit();

    StructuredTable table = getOperationRunsTable(context);
    try (CloseableIterator<StructuredRow> iterator = table.scan(range, limit,
        filterIndexes, StoreDefinition.OperationRunsStore.START_TIME_FIELD, SortOrder.DESC)) {
      boolean keepScanning = true;
      while (iterator.hasNext() && keepScanning && limit > 0) {
        StructuredRow row = iterator.next();
        Map.Entry<String, OperationRunDetail<?>> scanEntry =
            new SimpleImmutableEntry<String, OperationRunDetail<?>>(
                row.getString(StoreDefinition.OperationRunsStore.ID_FIELD),
                GSON.fromJson(row.getString(StoreDefinition.OperationRunsStore.DETAILS_FIELD),
                    OperationRunDetail.class)
            );
        keepScanning = func.apply(scanEntry);
        limit--;
      }
    }
  }

  private Collection<Field<?>> getFilterIndexes(OperationRunFilter filter) {
    Collection<Field<?>> filterIndexes = new ArrayList<>();
    if (filter == null) {
      return filterIndexes;
    }
    if (filter.getOperationType() != null) {
      filterIndexes.add(Fields.stringField(StoreDefinition.OperationRunsStore.TYPE_FIELD,
          filter.getOperationType()));
    }
    if (filter.getStatus() != null) {
      filterIndexes.add(Fields.stringField(StoreDefinition.OperationRunsStore.STATUS_FIELD,
          filter.getStatus().toString()));
    }
    return filterIndexes;
  }

  private List<Field<?>> getRangeFields(String namespace, String runId)
      throws IOException, OperationRunNotFoundException {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD, namespace));
    fields.add(Fields.stringField(StoreDefinition.OperationRunsStore.ID_FIELD, runId));
    Long startTime = getStartTime(namespace, runId);
    fields.add(
        Fields.longField(StoreDefinition.OperationRunsStore.START_TIME_FIELD, startTime));
    return fields;
  }

  private OperationRunDetail<?> getCurrentRunDetail(String namespace, String id)
      throws IOException, OperationRunNotFoundException {
    Optional<StructuredRow> currentRow = getOperationRunInternal(namespace, id);
    if (!currentRow.isPresent()) {
      throw new OperationRunNotFoundException(namespace, id);
    }
    return GSON.fromJson(
        currentRow.get().getString(StoreDefinition.OperationRunsStore.DETAILS_FIELD),
        OperationRunDetail.class
    );
  }

  private long getStartTime(String namespace, String id)
      throws IOException, OperationRunNotFoundException {
    Optional<StructuredRow> currentRow = getOperationRunInternal(namespace, id);
    if (!currentRow.isPresent()) {
      throw new OperationRunNotFoundException(namespace, id);
    }
    return currentRow.get().getLong(StoreDefinition.OperationRunsStore.START_TIME_FIELD);
  }

  private Collection<Field<?>> getCommonUpdateFields(String namespace, String id) {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD, namespace));
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.ID_FIELD, id));
    fields.add(
        Fields.longField(StoreDefinition.OperationRunsStore.UPDATE_TIME_FIELD,
            System.currentTimeMillis()));
    return fields;
  }

  private Optional<StructuredRow> getOperationRunInternal(String namespace, String operationId)
      throws IOException {
    StructuredTable operationRunsTable = getOperationRunsTable(context);
    Collection<Field<?>> key = ImmutableList.of(
        Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD, namespace),
        Fields.stringField(StoreDefinition.OperationRunsStore.ID_FIELD, operationId)
    );
    return operationRunsTable.read(key);
  }

  private void writeOperationRun(String namespace, String id, OperationRunDetail<?> detail)
      throws IOException {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.ID_FIELD, id));
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD, namespace));
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.STATUS_FIELD,
            detail.getRun().getStatus().toString()));
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.TYPE_FIELD,
            detail.getRun().getType()));
    fields.add(
        Fields.longField(StoreDefinition.OperationRunsStore.START_TIME_FIELD,
            detail.getRun().getMetadata().getCreateTime().toEpochMilli()));
    fields.add(
        Fields.longField(StoreDefinition.OperationRunsStore.UPDATE_TIME_FIELD,
            System.currentTimeMillis()));
    fields.add(
        Fields.stringField(StoreDefinition.OperationRunsStore.DETAILS_FIELD,
            GSON.toJson(detail)));
    StructuredTable operationRunsTable = getOperationRunsTable(context);
    operationRunsTable.upsert(fields);
  }

  private StructuredTable getOperationRunsTable(StructuredTableContext context) {
    return context.getTable(StoreDefinition.OperationRunsStore.OPERATION_RUNS);
  }

  @VisibleForTesting
  // USE ONLY IN TESTS: WILL DELETE ALL OPERATION RUNS
  public void deleteAllTables() throws IOException {
    deleteTable(getOperationRunsTable(context), StoreDefinition.AppMetadataStore.NAMESPACE_FIELD);
  }

  private void deleteTable(StructuredTable table, String firstKey) throws IOException {
    table.deleteAll(
        Range.from(ImmutableList.of(Fields.stringField(firstKey, SMALLEST_POSSIBLE_STRING)),
            Range.Bound.INCLUSIVE));
  }

}
