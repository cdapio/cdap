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

package io.cdap.cdap.storage.spanner;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.FieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * A {@link StructuredTableAdmin} implementation backed by Google Cloud Spanner.
 */
public class SpannerStructuredTableAdmin implements StructuredTableAdmin {

  private final DatabaseId databaseId;
  private final DatabaseAdminClient adminClient;
  private final DatabaseClient databaseClient;
  private final LoadingCache<StructuredTableId, StructuredTableSchema> schemaCache;

  static String getIndexName(StructuredTableId tableId, String column) {
    return String.format("%s_%s_idx", tableId.getName(), column);
  }

  public SpannerStructuredTableAdmin(Spanner spanner, DatabaseId databaseId) {
    this.databaseId = databaseId;
    this.adminClient = spanner.getDatabaseAdminClient();
    this.databaseClient = spanner.getDatabaseClient(databaseId);
    this.schemaCache = CacheBuilder.newBuilder()
      .build(new CacheLoader<StructuredTableId, StructuredTableSchema>() {
        @Override
        public StructuredTableSchema load(StructuredTableId tableId) {
          return loadSchema(tableId);
        }
      });
  }

  @Override
  public void create(StructuredTableSpecification spec) throws IOException, TableAlreadyExistsException {
    List<String> statements = new ArrayList<>();
    statements.add(getCreateTableStatement(spec));

    for (String idxColumn : spec.getIndexes()) {
      statements.add(String.format("CREATE INDEX %s ON %s (%s)",
                                   escapeName(getIndexName(spec.getTableId(), idxColumn)),
                                   escapeName(spec.getTableId().getName()), escapeName(idxColumn)));
    }

    try {
      Uninterruptibles.getUninterruptibly(adminClient.updateDatabaseDdl(databaseId.getInstanceId().getInstance(),
                                                                        databaseId.getDatabase(), statements, null));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SpannerException
        && ((SpannerException) cause).getErrorCode() == ErrorCode.FAILED_PRECONDITION) {
        throw new TableAlreadyExistsException(spec.getTableId());
      }
      throw new IOException("Failed to create table in Spanner", cause);
    }
  }

  @Override
  public boolean exists(StructuredTableId tableId) {
    try {
      getSchema(tableId);
      return true;
    } catch (TableNotFoundException e) {
      return false;
    }
  }

  @Override
  public StructuredTableSchema getSchema(StructuredTableId tableId) throws TableNotFoundException {
    try {
      return schemaCache.get(tableId);
    } catch (ExecutionException | UncheckedExecutionException e) {
      if (e.getCause() instanceof TableNotFoundException) {
        throw (TableNotFoundException) e.getCause();
      }
      throw new RuntimeException("Failed to load table schema for " + tableId, e.getCause());
    }
  }

  @Override
  public void drop(StructuredTableId tableId) throws IOException {
    List<String> ddlStatements = new ArrayList<>();

    // Gather indexes of the table
    Statement statement = Statement.newBuilder("SELECT index_name FROM information_schema.indexes " +
                                                 "WHERE table_name = @table_name AND index_type = 'INDEX'")
      .bind("table_name").to(tableId.getName())
      .build();

    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
      while (resultSet.next()) {
        ddlStatements.add("DROP INDEX " + resultSet.getCurrentRowAsStruct().getString("index_name"));
      }
    }

    try {
      ddlStatements.add("DROP TABLE " + tableId.getName());
      Uninterruptibles.getUninterruptibly(adminClient.updateDatabaseDdl(
        databaseId.getInstanceId().getInstance(), databaseId.getDatabase(), ddlStatements, null));
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  /**
   * Returns the {@link DatabaseClient} used by this admin.
   */
  DatabaseClient getDatabaseClient() {
    return databaseClient;
  }

  private StructuredTableSchema loadSchema(StructuredTableId tableId) throws TableNotFoundException {
    Statement schemaStatement = Statement.newBuilder(
      "SELECT C.column_name, C.spanner_type, I.index_type FROM information_schema.columns C " +
        "LEFT JOIN information_schema.index_columns I " +
        "ON C.column_name = I.column_name AND C.table_name = I.table_name " +
        "WHERE C.table_name = @table_name ORDER BY C.ordinal_position")
      .bind("table_name").to(tableId.getName())
      .build();

    List<FieldType> fields = new ArrayList<>();
    List<String> primaryKeys = new ArrayList<>();
    List<String> indexes = new ArrayList<>();

    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      try (ResultSet resultSet = tx.executeQuery(schemaStatement)) {
        while (resultSet.next()) {
          Struct row = resultSet.getCurrentRowAsStruct();
          String columnName = row.getString("column_name");
          String indexType = row.isNull("index_type") ? null : row.getString("index_type");

          fields.add(new FieldType(columnName, fromSpannerType(row.getString("spanner_type"))));
          if ("PRIMARY_KEY".equalsIgnoreCase(indexType)) {
            primaryKeys.add(columnName);
          } else if ("INDEX".equalsIgnoreCase(indexType)) {
            indexes.add(columnName);
          }
        }
      }
    }

    if (fields.isEmpty()) {
      throw new TableNotFoundException(tableId);
    }

    return new StructuredTableSchema(tableId, fields, primaryKeys, indexes);
  }

  private String getCreateTableStatement(StructuredTableSpecification spec) {
    Set<String> primaryKeys = spec.getPrimaryKeys().stream()
      .map(this::escapeName)
      .collect(Collectors.toCollection(LinkedHashSet::new));

    String statement = spec.getFieldTypes().stream()
      .map(f -> {
        String fieldName = f.getName();

        switch (f.getType()) {
          case INTEGER:
          case LONG:
            return escapeName(fieldName) + " INT64" + (primaryKeys.contains(fieldName) ? " NOT NULL" : "");
          case FLOAT:
          case DOUBLE:
            return escapeName(fieldName) + " FLOAT64" + (primaryKeys.contains(fieldName) ? " NOT NULL" : "");
          case STRING:
            return escapeName(fieldName) + " STRING(MAX)" + (primaryKeys.contains(fieldName) ? " NOT NULL" : "");
          case BYTES:
            return escapeName(fieldName) + " BYTES(MAX)" + (primaryKeys.contains(fieldName) ? " NOT NULL" : "");
          default:
            // This should never happen
            throw new IllegalArgumentException("Unsupported field type " + f.getType());
        }
      }).collect(Collectors.joining(", ", "CREATE TABLE " + escapeName(spec.getTableId().getName()) + " (", ")"));

    if (primaryKeys.isEmpty()) {
      return statement;
    }

    return statement + " PRIMARY KEY (" + String.join(", ", primaryKeys) + ")";
  }

  private FieldType.Type fromSpannerType(String spannerType) {
    switch (spannerType.toLowerCase()) {
      case "int64":
        return FieldType.Type.LONG;
      case "float64":
        return FieldType.Type.DOUBLE;
      case "string(max)":
        return FieldType.Type.STRING;
      case "bytes(max)":
        return FieldType.Type.BYTES;
      default:
        throw new IllegalArgumentException("Unsupport spanner type " + spannerType);
    }
  }

  private String escapeName(String name) {
    return "`" + name + "`";
  }
}
