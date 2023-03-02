/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store.state;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Store for app state data. It does not wrap its operations in a transaction. It is up to the
 * caller to decide what operations belong in a transaction.
 */
public class AppStateTable {

  private final StructuredTable table;

  public AppStateTable(StructuredTableContext context) {
    this.table = context.getTable(StoreDefinition.AppStateStore.APP_STATE);
  }

  /**
   * Get application state.
   *
   * @param request a {@link AppStateKey} object.
   * @return state of application.
   * @throws IOException if there is an error reading from the table.
   */
  public Optional<byte[]> get(AppStateKey request) throws IOException {
    Optional<StructuredRow> row = table.read(getKeyFields(request));
    return row.map(this::getStateValue);
  }

  /**
   * Save application state.
   *
   * @param request a {@link AppStateKeyValue} object.
   * @throws IOException if there is an error saving to the table
   */
  public void save(AppStateKeyValue request) throws IOException {
    table.upsert(getRow(request));
  }

  /**
   * Delete application state.
   *
   * @param request a {@link AppStateKey} object.
   * @throws IOException if there is an error deleting from the table.
   */
  public void delete(AppStateKey request) throws IOException {
    table.delete(getKeyFields(request));
  }

  /**
   * Delete all states related to an application.
   *
   * @param namespaceId NamespaceId of the application.
   * @param appName AppName of the application.
   * @throws IOException if there is an error reading or deleting from the table.
   */
  public void deleteAll(NamespaceId namespaceId, String appName) throws IOException {
    table.deleteAll(Range.from(
        ImmutableList.of(Fields.stringField(StoreDefinition.AppStateStore.NAMESPACE_FIELD,
                namespaceId.getNamespace()),
            Fields.stringField(StoreDefinition.AppStateStore.APP_NAME_FIELD,
                appName)),
        Range.Bound.INCLUSIVE));
  }

  /**
   * Delete all states related to a namespace.
   *
   * @param namespaceId NamespaceId of the application.
   * @throws IOException if there is an error reading or deleting from the table.
   */
  public void deleteAll(NamespaceId namespaceId) throws IOException {
    table.deleteAll(Range.from(
        ImmutableList.of(Fields.stringField(StoreDefinition.AppStateStore.NAMESPACE_FIELD,
            namespaceId.getNamespace())),
        Range.Bound.INCLUSIVE));
  }

  private byte[] getStateValue(StructuredRow structuredRow) {
    return structuredRow.getBytes(StoreDefinition.AppStateStore.STATE_VALUE_FIELD);
  }

  private List<Field<?>> getKeyFields(AppStateKey request) {
    List<Field<?>> keyFields = new ArrayList<>(3);
    keyFields.add(Fields.stringField(StoreDefinition.AppStateStore.NAMESPACE_FIELD,
        request.getNamespaceId().getNamespace()));
    keyFields.add(
        Fields.stringField(StoreDefinition.AppStateStore.APP_NAME_FIELD, request.getAppName()));
    keyFields.add(
        Fields.stringField(StoreDefinition.AppStateStore.STATE_KEY_FIELD, request.getStateKey()));
    return keyFields;
  }

  private List<Field<?>> getRow(AppStateKeyValue request) {
    List<Field<?>> fields = new ArrayList<>(4);
    fields.addAll(getKeyFields(request));
    fields.add(
        Fields.bytesField(StoreDefinition.AppStateStore.STATE_VALUE_FIELD, request.getState()));
    return fields;
  }
}
