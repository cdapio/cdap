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

package io.cdap.cdap.internal.state;

import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;

/**
 * Store for app state data.
 */
public class AppStateStore {
  private final TransactionRunner transactionRunner;

  @Inject
  public AppStateStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * @param request with primary keys set
   * @return state of application
   * @throws IOException if state not found
   */
  public Optional<byte[]> getState(AppState request) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.StateStore.STATE);
      Optional<StructuredRow> row = table.read(getKey(request));
      return row.map(this::getStateValue);
    }, IOException.class);
  }

  /**
   * @param request with all the fields set except
   * @throws IOException if exception while saving
   */
  public void saveState(AppState request) throws IOException {
    long now = System.currentTimeMillis();
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.StateStore.STATE);
      table.upsert(getRow(request, now));
    }, TableNotFoundException.class, InvalidFieldException.class);
  }


  /**
   * @param request with key fields set
   * @throws IOException if state not found
   */
  public void deleteState(AppState request) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.StateStore.STATE);
      table.delete(getKey(request));
    }, IOException.class);
  }

  /**
   * Extract state value from the row
   */
  private byte[] getStateValue(StructuredRow structuredRow) {
    return structuredRow.getBytes(StoreDefinition.StateStore.STATE_VALUE_FIELD);
  }

  /**
   * Get Key fields
   */
  private List<Field<?>> getKey(AppState request) {
    List<Field<?>> keyFields = new ArrayList<>(4);
    keyFields.add(Fields.stringField(StoreDefinition.StateStore.NAMESPACE_FIELD, request.getNamespace()));
    keyFields.add(Fields.stringField(StoreDefinition.StateStore.APP_NAME_FIELD, request.getAppName()));
    keyFields.add(Fields.longField(StoreDefinition.StateStore.APP_ID_FIELD, request.getAppId()));
    keyFields.add(Fields.stringField(StoreDefinition.StateStore.STATE_KEY_FIELD, request.getStateKey()));
    return keyFields;
  }

  /**
   * Get all fields
   */
  private List<Field<?>> getRow(AppState request, long updatedTime) {
    List<Field<?>> fields = new ArrayList<>(4);
    fields.addAll(getKey(request));
    fields.add(Fields.bytesField(StoreDefinition.StateStore.STATE_VALUE_FIELD, request.getState()));
    fields.add(Fields.longField(StoreDefinition.StateStore.UPDATED_TIME_FIELD, updatedTime));
    return fields;
  }
}
