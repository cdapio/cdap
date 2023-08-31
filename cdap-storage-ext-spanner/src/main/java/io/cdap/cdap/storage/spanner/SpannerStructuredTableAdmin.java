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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.TableDuplicateUpdateException;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.TableSchemaIncompatibleException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.FieldType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StructuredTableAdmin} implementation backed by Google Cloud Spanner.
 */
public class SpannerStructuredTableAdmin implements StructuredTableAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerStructuredTableAdmin.class);
  private final DatabaseId databaseId;
  private final DatabaseAdminClient adminClient;
  private final DatabaseClient databaseClient;
  private final LoadingCache<StructuredTableId, StructuredTableSchema> schemaCache;

  static String getIndexName(StructuredTableId tableId, String column) {
    return String.format("%s_%s_idx", tableId.getName(), column);
  }

  /**
   * Constructor for {@code SpannerStructuredTableAdmin}.
   *
   * @param spanner the gcp Spanner service.
   * @param databaseId the ID of the Spanner instance database.
   */
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
  public void create(StructuredTableSpecification spec)
      throws IOException, TableAlreadyExistsException {
    if (exists(spec.getTableId())) {
      throw new TableAlreadyExistsException(spec.getTableId());
    }
    createTable(spec);
  }

  @Override
  public void createOrUpdate(StructuredTableSpecification spec)
      throws IOException, TableSchemaIncompatibleException {
    if (exists(spec.getTableId())) {
      tryUpdatingTable(spec);
    } else {
      createTable(spec);
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

  private void updateTable(StructuredTableSpecification spec)
      throws IOException, TableNotFoundException, TableSchemaIncompatibleException {
    LOG.error(">>>>  spec: {}", spec);
    StructuredTableId tableId = spec.getTableId();
    StructuredTableSchema cachedTableSchema = getSchema(tableId);
    StructuredTableSchema newTableSchema = convertSpecToCompatibleSchema(spec);

    LOG.error("TableId cachedTableSchema {}", cachedTableSchema.getTableId());
    LOG.error("TableId newTableSchema {}", newTableSchema.getTableId());

    for (String f: cachedTableSchema.getFieldNames()){
      LOG.error("cachedTableSchema.field: {}", f);
    }

    if (newTableSchema.equals(cachedTableSchema)) {
      LOG.trace("The table schema is already up to date: {}", tableId);
      return;
    }
    if (!cachedTableSchema.isCompatible(newTableSchema)) {
      throw new TableSchemaIncompatibleException(tableId);
    }

    List<String> statements = new ArrayList<>();
    statements.addAll(getAddColumnsStatement(newTableSchema, cachedTableSchema));
    statements.addAll(getAddIndicesStatement(newTableSchema, cachedTableSchema));
    try {
      Uninterruptibles.getUninterruptibly(
          adminClient.updateDatabaseDdl(databaseId.getInstanceId().getInstance(),
              databaseId.getDatabase(), statements, null));
      schemaCache.invalidate(tableId);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SpannerException) {
        if (((SpannerException) cause).getErrorCode() == ErrorCode.FAILED_PRECONDITION) {
          LOG.debug("Concurrent table update error: ", e);
          throw new TableDuplicateUpdateException(spec.getTableId());
        }

        if (((SpannerException) cause).getErrorCode() == ErrorCode.NOT_FOUND) {
          LOG.debug(
              "Concurrent table update error, table not found while updating the table schema: ",
              e);
          throw new TableNotFoundException(spec.getTableId());
        }
      }

      throw new IOException("Failed to update table schema in Spanner", cause);
    }
  }

  @VisibleForTesting
  static StructuredTableSchema convertSpecToCompatibleSchema(StructuredTableSpecification spec) {
    List<FieldType> convertedFieldTypes =
        spec.getFieldTypes().stream()
            .map(
                fieldType -> {
                  String spannerType = getSpannerType(fieldType.getType());
                  FieldType.Type convertedType = fromSpannerType(spannerType);
                  return new FieldType(fieldType.getName(), convertedType);
                })
            .collect(Collectors.toList());
    return new StructuredTableSchema(
        spec.getTableId(), convertedFieldTypes, spec.getPrimaryKeys(), spec.getIndexes());
  }

  @Override
  public void drop(StructuredTableId tableId) throws IOException {
    List<String> ddlStatements = new ArrayList<>();

    // Gather indexes of the table
    Statement statement = Statement.newBuilder(
            "SELECT index_name FROM information_schema.indexes "
                + "WHERE table_name = @table_name AND index_type = 'INDEX'")
        .bind("table_name").to(tableId.getName())
        .build();

    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
      while (resultSet.next()) {
        ddlStatements.add(
            "DROP INDEX " + resultSet.getCurrentRowAsStruct().getString("index_name"));
      }
    }

    try {
      ddlStatements.add("DROP TABLE " + tableId.getName());
      Uninterruptibles.getUninterruptibly(adminClient.updateDatabaseDdl(
          databaseId.getInstanceId().getInstance(), databaseId.getDatabase(), ddlStatements, null));
      schemaCache.invalidate(tableId);
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

  private StructuredTableSchema loadSchema(StructuredTableId tableId)
      throws TableNotFoundException {
    // Query the information_schema to reconstruct the StructuredTableSchema
    // See https://cloud.google.com/spanner/docs/information-schema for details
    Statement schemaStatement = Statement.newBuilder(
            "SELECT C.column_name, C.spanner_type, I.index_type, I.ordinal_position FROM information_schema.columns C "

                + "LEFT JOIN information_schema.index_columns I "
                + "ON C.column_name = I.column_name AND C.table_name = I.table_name AND I.ordinal_position is not NULL "

                + "WHERE C.table_name = @table_name ORDER BY C.ordinal_position")
        .bind("table_name").to(tableId.getName())
        .build();

    List<FieldType> fields = new ArrayList<>();
    SortedMap<Long, String> primaryKeysOrderMap = new TreeMap<>();
    Set<String> indexes = new LinkedHashSet<>();

    try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
      try (ResultSet resultSet = tx.executeQuery(schemaStatement)) {
        while (resultSet.next()) {
          Struct row = resultSet.getCurrentRowAsStruct();
          String columnName = row.getString("column_name");
          String indexType = row.isNull("index_type") ? null : row.getString("index_type");

          // If a field is not a primary nor an index, the ordinal_position will be NULL in the index_columns table.
          boolean isIndex = !row.isNull("ordinal_position");

          fields.add(new FieldType(columnName, fromSpannerType(row.getString("spanner_type"))));
          if ("PRIMARY_KEY".equalsIgnoreCase(indexType)) {
            primaryKeysOrderMap.put(row.getLong("ordinal_position"), columnName);
          } else if ("INDEX".equalsIgnoreCase(indexType) && isIndex) {
            indexes.add(columnName);
          }
        }
      }
    }

    if (fields.isEmpty()) {
      throw new TableNotFoundException(tableId);
    }

    List<String> primaryKeys = new ArrayList<>(primaryKeysOrderMap.values());
    // Primary Key fields can still be overly added when it's part of other index, exclude them
    Set<String> nonPrimaryKeyIndexes = Sets.difference(indexes, new HashSet<>(primaryKeys));

    return new StructuredTableSchema(tableId, fields, primaryKeys, nonPrimaryKeyIndexes);
  }

  private String getCreateTableStatement(StructuredTableSpecification spec) {
    Set<String> primaryKeys = spec.getPrimaryKeys().stream()
        .map(this::escapeName)
        .collect(Collectors.toCollection(LinkedHashSet::new));

    String statement = spec.getFieldTypes().stream()
        .map(f -> {
          String fieldName = f.getName();
          return escapeName(fieldName) + " "
              + getSpannerType(f.getType())
              + (primaryKeys.contains(fieldName) ? " NOT NULL" : "");
        }).collect(Collectors.joining(", ",
            "CREATE TABLE " + escapeName(spec.getTableId().getName()) + " (", ")"));

    if (primaryKeys.isEmpty()) {
      return statement;
    }

    return statement + " PRIMARY KEY (" + String.join(", ", primaryKeys) + ")";
  }

  private String getCreateIndexStatement(String idxColumn, StructuredTableSchema schema) {
    String createIndex = String.format("CREATE INDEX %s ON %s (%s)",
        escapeName(getIndexName(schema.getTableId(), idxColumn)),
        escapeName(schema.getTableId().getName()), escapeName(idxColumn));

    // Need to store all the non-primary keys and non index fields so that it can be queried
    Set<String> storingFields = new HashSet<>(schema.getFieldNames());
    storingFields.remove(idxColumn);
    schema.getPrimaryKeys().forEach(storingFields::remove);

    if (!storingFields.isEmpty()) {
      createIndex += " STORING (" + String.join(",", storingFields) + ")";
    }
    return createIndex;
  }

  private static String getSpannerType(FieldType.Type fieldType) {
    switch (fieldType) {
      case INTEGER:
      case LONG:
        return "INT64";
      case FLOAT:
      case DOUBLE:
        return "FLOAT64";
      case STRING:
        return "STRING(MAX)";
      case BYTES:
        return "BYTES(MAX)";
      case BOOLEAN:
        return "BOOL";
      default:
        // This should never happen
        throw new IllegalArgumentException("Unsupported field type " + fieldType);
    }
  }

  private static FieldType.Type fromSpannerType(String spannerType) {
    switch (spannerType.toLowerCase()) {
      case "int64":
        return FieldType.Type.LONG;
      case "float64":
        return FieldType.Type.DOUBLE;
      case "string(max)":
        return FieldType.Type.STRING;
      case "bytes(max)":
        return FieldType.Type.BYTES;
      case "bool":
        return FieldType.Type.BOOLEAN;
      default:
        throw new IllegalArgumentException("Unsupported spanner type " + spannerType);
    }
  }

  private String escapeName(String name) {
    return "`" + name + "`";
  }

  private void createTable(StructuredTableSpecification spec) throws IOException {
    List<String> statements = new ArrayList<>();
    statements.add(getCreateTableStatement(spec));

    StructuredTableSchema schema = new StructuredTableSchema(spec);
    spec.getIndexes()
        .forEach(idxColumn -> statements.add(getCreateIndexStatement(idxColumn, schema)));

    try {
      Uninterruptibles.getUninterruptibly(
          adminClient.updateDatabaseDdl(databaseId.getInstanceId().getInstance(),
              databaseId.getDatabase(), statements, null));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SpannerException
          && ((SpannerException) cause).getErrorCode() == ErrorCode.FAILED_PRECONDITION) {
        LOG.debug("Concurrent table creation error: ", e);
      } else {
        throw new IOException("Failed to create table in Spanner", cause);
      }
    }
  }

  private void tryUpdatingTable(StructuredTableSpecification spec)
      throws IOException, TableSchemaIncompatibleException {
    try {
      updateTable(spec);
    } catch (TableDuplicateUpdateException | TableNotFoundException e) {
      // Invalidate cached schema and retry
      schemaCache.invalidate(spec.getTableId());
      if (e instanceof TableDuplicateUpdateException) {
        // Exception due to adding existing columns or indexes in Spanner
        LOG.debug(String.format("Retry updating the table: %s", spec.getTableId()));
        updateTable(spec);
      }
      if (e instanceof TableNotFoundException) {
        // Exception due to table being deleted while updating the schema
        // re-create it
        LOG.debug(String.format("Re-creating the table: %s", spec.getTableId()));
        createTable(spec);
      }
    }
  }

  private List<String> getAddColumnsStatement(StructuredTableSchema newSpannerSchema,
      StructuredTableSchema cachedTableSchema) {
    Set<String> existingSchemaFields = cachedTableSchema.getFieldNames();
    return newSpannerSchema.getFieldNames().stream()
        .filter(field -> !existingSchemaFields.contains(field))
        .map(field ->
            String.format("ALTER TABLE %s ADD COLUMN %s %s",
                escapeName(newSpannerSchema.getTableId().getName()),
                escapeName(field),
                getSpannerType(newSpannerSchema.getType(field))
            ))
        .collect(Collectors.toList());
  }

  private List<String> getAddIndicesStatement(StructuredTableSchema newSchema,
      StructuredTableSchema cachedTableSchema) {
    Set<String> existingSchemaIndices = cachedTableSchema.getIndexes();
    return newSchema.getIndexes().stream()
        .filter(field -> !existingSchemaIndices.contains(field))
        .map(field -> getCreateIndexStatement(field, newSchema))
        .collect(Collectors.toList());
  }
}
