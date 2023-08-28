/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.service.operation.OperationError;
import io.cdap.cdap.api.service.operation.OperationMeta;
import io.cdap.cdap.api.service.operation.OperationRun;
import io.cdap.cdap.api.service.operation.OperationStatus;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;

/**
 * Store for tethering data.
 */
public class OperationRunsStore {

  private static final Gson GSON = new GsonBuilder().create();
  public static final Type LIST_ERROR_TYPE = new TypeToken<List<OperationError>>() {
  }.getType();
  private final TransactionRunner transactionRunner;

  @Inject
  public OperationRunsStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }


  public void createOperation(NamespaceId namespaceId, String operationId, String operationType)
    throws OperationRunAlreadyExistsException, IOException {
    TransactionRunners.run(transactionRunner, context -> {
      Optional<StructuredRow> row = getOperationRunInternal(context, operationId);
      if (row.isPresent()) {
        OperationRun existingRun = rowToOperationRun(row.get());
        throw new OperationRunAlreadyExistsException(existingRun.getOperationId(), existingRun.getStatus());
      }
      OperationRun newRun = new OperationRun(operationId, namespaceId, operationType, System.currentTimeMillis());
      writeOperationRun(context, newRun);
    }, IOException.class, OperationRunAlreadyExistsException.class);
  }

  public void updateOperationMeta(NamespaceId namespaceId, String operationId, OperationMeta metadata) throws NotFoundException, IOException {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.OPERATION_ID_FIELD, operationId));
    fields.add(
      Fields.longField(StoreDefinition.OperationRunsStore.UPDATED_AT_FIELD, System.currentTimeMillis()));
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.METADATA_FIELD, GSON.toJson(metadata)));
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable operationRunsTable = context.getTable(StoreDefinition.OperationRunsStore.OPERATION_RUNS);
      operationRunsTable.update(fields);
    }, IOException.class, NotFoundException.class);
  }

  public void updateOperationStatus(NamespaceId namespaceId, String operationId, OperationStatus status) throws NotFoundException, IOException {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.OPERATION_ID_FIELD, operationId));
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.STATUS_FIELD, status.toString()));
    fields.add(
      Fields.longField(StoreDefinition.OperationRunsStore.UPDATED_AT_FIELD, System.currentTimeMillis()));
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable operationRunsTable = context.getTable(StoreDefinition.OperationRunsStore.OPERATION_RUNS);
      operationRunsTable.update(fields);
    }, IOException.class, NotFoundException.class);
  }

  public void failOperation(NamespaceId namespaceId, String operationId, List<OperationError> error) throws NotFoundException, IOException {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.OPERATION_ID_FIELD, operationId));
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.STATUS_FIELD, OperationStatus.FAILED.toString()));
    fields.add(
      Fields.longField(StoreDefinition.OperationRunsStore.UPDATED_AT_FIELD, System.currentTimeMillis()));
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.ERRORS_FIELD, GSON.toJson(error)));
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable operationRunsTable = context.getTable(StoreDefinition.OperationRunsStore.OPERATION_RUNS);
      operationRunsTable.update(fields);
    }, IOException.class, NotFoundException.class);
  }

  public OperationRun getOperation(NamespaceId namespaceId, String operationId) throws NotFoundException, IOException {
      return TransactionRunners.run(transactionRunner, context -> {
        Optional<StructuredRow> row = getOperationRunInternal(context, operationId);
        if (!row.isPresent()) {
          throw new NotFoundException("operation with id " + operationId + "missing");
        }
        return rowToOperationRun(row.get());
      }, IOException.class, NotFoundException.class);
  }

  public List<OperationRun> listOperations(NamespaceId namespaceId) throws IOException {
    List<OperationRun> operationRuns = new ArrayList<>();
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetheringTable = context
        .getTable(StoreDefinition.OperationRunsStore.OPERATION_RUNS);
      try (CloseableIterator<StructuredRow> iterator = tetheringTable.scan(Range.all(), Integer.MAX_VALUE)) {
        iterator.forEachRemaining(row -> operationRuns.add(rowToOperationRun(row)));
        return operationRuns;
      }
    }, IOException.class);
  }

  private Optional<StructuredRow> getOperationRunInternal(StructuredTableContext context, String operationId)
    throws IOException {
    StructuredTable operationRunsTable = context.getTable(StoreDefinition.OperationRunsStore.OPERATION_RUNS);
    Collection<Field<?>> key = ImmutableList.of(
      Fields.stringField(StoreDefinition.OperationRunsStore.OPERATION_ID_FIELD, operationId));
    return operationRunsTable.read(key);
  }

  private OperationRun rowToOperationRun(StructuredRow row) {
    String operationId = row.getString(StoreDefinition.OperationRunsStore.OPERATION_ID_FIELD);
    String namespace = row.getString(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD);
    String type = row.getString(StoreDefinition.OperationRunsStore.TYPE_FIELD);
    OperationStatus status = OperationStatus.valueOf(
      row.getString(StoreDefinition.OperationRunsStore.STATUS_FIELD));
    OperationMeta metadata = GSON.fromJson(
      row.getString(StoreDefinition.OperationRunsStore.METADATA_FIELD),
      OperationMeta.class);
    List<OperationError> error = GSON.fromJson(
      row.getString(StoreDefinition.OperationRunsStore.ERRORS_FIELD), LIST_ERROR_TYPE);
    long createdAt = row.getLong(StoreDefinition.OperationRunsStore.CREATED_AT_FIELD);
    long updatedAt = row.getLong(StoreDefinition.OperationRunsStore.UPDATED_AT_FIELD);
    return new OperationRun(operationId, new NamespaceId(namespace), type, status, createdAt, updatedAt, metadata, error);
  }

  private void writeOperationRun(StructuredTableContext context, OperationRun operationRun)
    throws IOException {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.OPERATION_ID_FIELD, operationRun.getOperationId()));
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.NAMESPACE_FIELD, operationRun.getNamespace().getNamespace()));
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.STATUS_FIELD, operationRun.getStatus().toString()));
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.TYPE_FIELD, operationRun.getOperationType()));
    fields.add(
      Fields.longField(StoreDefinition.OperationRunsStore.CREATED_AT_FIELD, operationRun.getCreatedAt()));
    fields.add(
      Fields.longField(StoreDefinition.OperationRunsStore.UPDATED_AT_FIELD, operationRun.getCreatedAt()));
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.METADATA_FIELD, GSON.toJson(operationRun.getMetadata())));
    fields.add(
      Fields.stringField(StoreDefinition.OperationRunsStore.ERRORS_FIELD, GSON.toJson(operationRun.getErrors())));
    StructuredTable operationRunsTable = context.getTable(StoreDefinition.OperationRunsStore.OPERATION_RUNS);
    operationRunsTable.upsert(fields);
  }
}
