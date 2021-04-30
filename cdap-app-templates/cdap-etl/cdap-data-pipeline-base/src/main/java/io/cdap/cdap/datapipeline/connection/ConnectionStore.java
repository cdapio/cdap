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
 *
 */

package io.cdap.cdap.datapipeline.connection;

import com.google.gson.Gson;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectionId;
import io.cdap.cdap.etl.proto.connection.ConnectionNotFoundException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Store to save connection information.
 *
 *  The table will look like:
 *  primary key                                                 column -> value
 *  [namespace][connection-id]                                  connection_data -> Connection{@link Connection}
 */
public class ConnectionStore {
  public static final StructuredTableId TABLE_ID = new StructuredTableId("connections_store");
  private static final String NAMESPACE_FIELD = "namespace";
  private static final String CONNECTION_ID_FIELD = "connection_id";
  // this field is to ensure the connection information is correctly fetched if a namespace is recreated
  private static final String GENERATION_COL = "generation";
  private static final String CONNECTION_DATA_FIELD = "connection_data";
  private static final String CREATED_COL = "createdtimemillis";
  private static final String UPDATED_COL = "updatedtimemillis";

  public static final StructuredTableSpecification CONNECTION_TABLE_SPEC =
    new StructuredTableSpecification.Builder()
      .withId(TABLE_ID)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.longType(GENERATION_COL),
                  Fields.longType(CREATED_COL),
                  Fields.longType(UPDATED_COL),
                  Fields.stringType(CONNECTION_ID_FIELD),
                  Fields.stringType(CONNECTION_DATA_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, GENERATION_COL, CONNECTION_ID_FIELD)
      .build();
  private static final Gson GSON = new Gson();
  private final TransactionRunner transactionRunner;

  public ConnectionStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Get the connection information about the given connection
   *
   * @param connectionId the id of the connection to look up
   * @return the connection information about the given connection id
   * @throws ConnectionNotFoundException if the connection is not found
   */
  public Connection getConnection(ConnectionId connectionId) throws ConnectionNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      return getConnectionInternal(table, connectionId, true);
    }, ConnectionNotFoundException.class);
  }

  /**
   * Get all the connections in the given namespace
   *
   * @param namespace the namespace to look up
   * @return the list of connections in this namespace
   */
  public List<Connection> listConnections(NamespaceSummary namespace) {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      Range range = Range.singleton(getNamespaceKeys(namespace));
      List<Connection> connections = new ArrayList<>();
      try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
        rowIter.forEachRemaining(
          structuredRow -> connections.add(GSON.fromJson(structuredRow.getString(CONNECTION_DATA_FIELD),
                                                         Connection.class)));
      }
      return connections;
    });
  }

  /**
   * Save the connection in the store.
   *
   * @param connectionId the connection id
   * @param connection the connection information
   */
  public void saveConnection(ConnectionId connectionId, Connection connection) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      Connection oldConnection  = getConnectionInternal(table, connectionId, false);
      Connection newConnection = connection;
      if (oldConnection != null) {
        newConnection = new Connection(connection.getName(), connection.getConnectionType(),
                                       connection.getDescription(), connection.isPreConfigured(),
                                       oldConnection.getCreatedTimeMillis(), connection.getUpdatedTimeMillis(),
                                       connection.getPlugin());
      }

      Collection<Field<?>> fields = getConnectionKeys(connectionId);
      fields.add(Fields.longField(CREATED_COL, newConnection.getCreatedTimeMillis()));
      fields.add(Fields.longField(UPDATED_COL, newConnection.getUpdatedTimeMillis()));
      fields.add(Fields.stringField(CONNECTION_DATA_FIELD, GSON.toJson(newConnection)));
      table.upsert(fields);
    });
  }

  /**
   * Delete the given connection
   *
   * @param connectionId the connection id to delete
   * @throws ConnectionNotFoundException if the connection is not found
   */
  public void deleteConnection(ConnectionId connectionId) throws ConnectionNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      getConnectionInternal(table, connectionId, true);
      table.delete(getConnectionKeys(connectionId));
    }, ConnectionNotFoundException.class);
  }

  // internal get connection method so the save, delete and get operation can all happen in single transaction
  @Nullable
  private Connection getConnectionInternal(
    StructuredTable table, ConnectionId connectionId,
    boolean failIfNotFound) throws IOException, ConnectionNotFoundException {
    Optional<StructuredRow> row = table.read(getConnectionKeys(connectionId));
    if (!row.isPresent()) {
      if (!failIfNotFound) {
        return null;
      }
      throw new ConnectionNotFoundException(connectionId);
    }
    return GSON.fromJson(row.get().getString(CONNECTION_DATA_FIELD), Connection.class);
  }

  private Collection<Field<?>> getConnectionKeys(ConnectionId connectionId) {
    List<Field<?>> keys = new ArrayList<>();
    keys.addAll(getNamespaceKeys(connectionId.getNamespace()));
    keys.add(Fields.stringField(CONNECTION_ID_FIELD, connectionId.getConnection()));
    return keys;
  }

  private Collection<Field<?>> getNamespaceKeys(NamespaceSummary namespace) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.stringField(NAMESPACE_FIELD, namespace.getName()));
    keys.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    return keys;
  }
}
