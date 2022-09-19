/*
 * Copyright © 2019-2022 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.sql;

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.TableSchemaIncompatibleException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.sql.DataSource;

/**
 * Sql structured admin to use jdbc connection to create and drop tables.
 */
public class PostgreSqlStructuredTableAdmin implements StructuredTableAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlStructuredTableAdmin.class);
  // https://www.postgresql.org/docs/9.6/errcodes-appendix.html, "42P01" denotes the table is undefined
  private static final String TABLE_UNDEFINED_SQL_STATE = "42P01";
  private final DataSource dataSource;
  private final LoadingCache<StructuredTableId, StructuredTableSchema> schemaCache;

  @Inject
  PostgreSqlStructuredTableAdmin(DataSource dataSource) {
    this.dataSource = dataSource;
    this.schemaCache = CacheBuilder.newBuilder()
      .build(new CacheLoader<StructuredTableId, StructuredTableSchema>() {
        @Override
        public StructuredTableSchema load(StructuredTableId tableId) throws SQLException {
          return loadSchema(tableId);
        }
      });
  }

  @Override
  public void create(StructuredTableSpecification spec) throws IOException, TableAlreadyExistsException {
    if (exists(spec.getTableId())) {
      throw new TableAlreadyExistsException(spec.getTableId());
    }
    createTable(spec);
  }

  @Override
  public void createOrUpdate(StructuredTableSpecification spec)
    throws IOException, TableSchemaIncompatibleException {
    if (exists(spec.getTableId())) {
      try {
        updateTable(spec);
        return;
      } catch (TableNotFoundException e) {
        LOG.debug(String.format("Table %s not found while updating it, creating it now.", spec.getTableId()));
      }
    }
    createTable(spec);
  }

  @Override
  public boolean exists(StructuredTableId tableId) throws IOException {
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

  private void updateTable(StructuredTableSpecification newSpec)
    throws IOException, TableNotFoundException, TableSchemaIncompatibleException {
    StructuredTableId tableId = newSpec.getTableId();
    StructuredTableSchema newSchema = new StructuredTableSchema(newSpec);
    StructuredTableSchema existingSchema = getSchema(tableId);
    if (newSchema.equals(existingSchema)) {
      LOG.trace("The table schema is already up to date: {}", tableId);
      return;
    }
    if (!existingSchema.isCompatible(newSchema)) {
      throw new TableSchemaIncompatibleException(tableId);
    }

    try (Connection connection = dataSource.getConnection()) {
      // Since each add column and create index statement has "IF NOT EXIST", it should not throw in
      // a race condition when multiple table updates process at the same time, hence no need to retry updateTable
      LOG.debug("Updating table schema {}", newSpec);
      addColumns(connection, newSchema);
      addIndices(connection, newSchema);
      schemaCache.invalidate(tableId);
    } catch (SQLException e) {
      throw new IOException(String.format("Error updating table schema: %s", tableId.getName()), e);
    }
  }

  private void createTable(StructuredTableSpecification spec) throws IOException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        if (!tableExistsInternal(connection, spec.getTableId())) {
          // Create table
          LOG.debug("Creating table {}", spec);
          statement.execute(getCreateStatement(spec));
        }

        // Create indexes
        Set<String> nonExistIndexes = getNonExistIndexes(connection, new StructuredTableSchema(spec));
        for (String indexStatement : getCreateIndexStatements(spec.getTableId(), nonExistIndexes)) {
          LOG.debug("Creating index statement: {}", indexStatement);
          statement.execute(indexStatement);
        }
      }
    } catch (SQLException e) {
      throw new IOException(String.format("Error creating table %s", spec.getTableId()), e);
    }
  }

  // TODO: CDAP-15068 - make drop table idempotent
  @Override
  public void drop(StructuredTableId tableId) throws IOException {
    LOG.info("Dropping table {}", tableId);
    String sqlQuery = getDeleteStatement(tableId.getName());
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(sqlQuery);
      }
      schemaCache.invalidate(tableId);
      // All indexes on a table are dropped when a table is dropped.
    } catch (SQLException e) {
      throw new IOException(String.format("Error dropping table %s", tableId), e);
    }
  }

  private boolean tableExistsInternal(Connection connection, StructuredTableId tableId) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet rs = metaData.getTables(null, null, tableId.getName(), null)) {
      return rs.next();
    }
  }

  // Add new non-primary key columns to table
  private void addColumns(Connection connection, StructuredTableSchema schema)
    throws IOException, TableNotFoundException {
    StructuredTableId tableId = schema.getTableId();
    try {
      Optional<String> columnStatement = getAddColumnStatement(tableId,
                                                               connection.getMetaData(), schema);
      if (!columnStatement.isPresent()) {
        LOG.debug("No new columns to add for table {}", schema.getTableId());
        return;
      }
      try (Statement statement = connection.createStatement()) {
        LOG.debug("Create adding columns statement: {}", columnStatement);
        statement.execute(columnStatement.get());
      }
    } catch (SQLException e) {
      // PostgreSQL error 42P01 denotes the database query is on an undefined table.
      if (TABLE_UNDEFINED_SQL_STATE.equalsIgnoreCase(e.getSQLState())) {
        throw new TableNotFoundException(tableId);
      }
      throw new IOException(String.format("Error adding columns to table: %s", tableId.getName()), e);
    }
  }

  // Add new non-primary key indices to table
  private void addIndices(Connection connection, StructuredTableSchema schema)
    throws IOException, TableNotFoundException {
    try (Statement statement = connection.createStatement()) {
      // Create indexes
      for (String indexStatement : getCreateIndexStatements(schema.getTableId(),
                                                            getNonExistIndexes(connection, schema))) {
        LOG.debug("Create index statement: {}", indexStatement);
        statement.execute(indexStatement);
      }
    } catch (SQLException e) {
      if (TABLE_UNDEFINED_SQL_STATE.equalsIgnoreCase(e.getSQLState())) {
        throw new TableNotFoundException(schema.getTableId());
      }
      throw new IOException(String.format("Error adding indices to table: %s", schema.getTableId().getName()), e);
    }
  }

  private Set<String> getNonExistIndexes(Connection connection,
                                         StructuredTableSchema schema) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    Set<String> existingIndexes = new HashSet<>();
    try (ResultSet rs = metaData.getIndexInfo(null, null,
                                              schema.getTableId().getName(), false, false)) {
      while (rs.next()) {
        // the COLUMN_NAME will return the column that is indexed, which is more intuitive to use INDEX_NAME, also
        // null can be returned if the created index is of tableIndexStatistic, though we don't create indexes of this
        // type, it is safe to only add non-null column name.
        String columnName = rs.getString("COLUMN_NAME");
        if (columnName != null) {
          existingIndexes.add(columnName);
        }
      }
    }
    return Sets.difference(schema.getIndexes(), existingIndexes);
  }

  private String getCreateStatement(StructuredTableSpecification specification) {
    StringBuilder createStmt = new StringBuilder();
    createStmt.append("CREATE TABLE ").append(specification.getTableId().getName()).append(" (");

    // append the columns with sql type
    createStmt.append(
      specification.getFieldTypes().stream()
        .map(f -> f.getName() + " " + getPostgreSQLType(f.getType()))
        .collect(Collectors.joining(","))
    );

    // append primary key
    createStmt.append(", PRIMARY KEY (").append(Joiner.on(",").join(specification.getPrimaryKeys())).append("))");
    return createStmt.toString();
  }

  private List<String> getCreateIndexStatements(StructuredTableId tableId, Set<String> indexColumns) {
    String tableName = tableId.getName();
    return indexColumns.stream()
      .map(indexColumn -> String.format("CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s (%s)",
                                        tableName, indexColumn, tableName, indexColumn))
      .collect(Collectors.toList());
  }

  private Optional<String> getAddColumnStatement(StructuredTableId tableId,
                                                 DatabaseMetaData metaData,
                                                 StructuredTableSchema schema) throws SQLException {
    Set<String> existingFields = new HashSet<>();
    ResultSet resultSet = metaData.getColumns(null, null, tableId.getName(), null);
    while (resultSet.next()) {
      existingFields.add(resultSet.getString("COLUMN_NAME"));
    }

    // Since schema is backward compatible, they have the same primary keys
    // The new different fields are all non-primary key columns
    List<String> fieldsToAdd = schema.getFieldNames()
      .stream()
      .filter(f -> !existingFields.contains(f))
      .collect(Collectors.toList());

    if (fieldsToAdd.isEmpty()) {
      return Optional.empty();
    }

    String addColumnsStatement = fieldsToAdd.stream()
      .map(fieldName ->
             String.format(" ADD COLUMN IF NOT EXISTS %s %s", fieldName, getPostgreSQLType(schema.getType(fieldName))))
      .collect(Collectors.joining(", ", "ALTER TABLE " + tableId.getName(), ";"));

    return Optional.of(addColumnsStatement);
  }

  private String getDeleteStatement(String tableName) {
    return "DROP TABLE " + tableName + ";";
  }

  private String getPostgreSQLType(FieldType.Type fieldType) {
    switch (fieldType) {
      case INTEGER:
        return "int";
      case STRING:
        return "text";
      case LONG:
        return "bigint";
      case DOUBLE:
        return "double precision";
      case FLOAT:
        return "real";
      case BYTES:
        return "bytea";
      default:
        // this should never happen since all the fields are from the specification and validated there
        throw new IllegalStateException(
          String.format("UnSupported type: %s", fieldType));
    }
  }

  private FieldType.Type fromSQLType(String sqlType) {
    switch (sqlType.toLowerCase()) {
      case "integer":
      case "int":
        return FieldType.Type.INTEGER;
      case "text":
        return FieldType.Type.STRING;
      case "bigint":
        return FieldType.Type.LONG;
      case "double precision":
        return FieldType.Type.DOUBLE;
      case "real":
        return FieldType.Type.FLOAT;
      case "bytea":
        return FieldType.Type.BYTES;
      default:
        throw new IllegalArgumentException("Unsupport spanner type " + sqlType);
    }
  }

  private StructuredTableSchema loadSchema(StructuredTableId tableId)
    throws TableNotFoundException, SQLException {
    // Query the information_schema, pg_index and pg_attribute to reconstruct the StructuredTableSchema
    // using just one query, instead of calling MetaData.getColumns(), MetaData.getPrimaryKeys() and
    // MetaData.getIndexInfo() that is hitting Database 3 times
    String schemaStatement = String.format("SELECT C.column_name, C.data_type, I.is_primarykey_index "
                                             + "FROM information_schema.columns C LEFT JOIN "
                                             + "(SELECT a.attname AS column_name, i.indisprimary AS "
                                             + "is_primarykey_index, format_type(a.atttypid, a.atttypmod) "
                                             + "AS data_type FROM pg_index i JOIN pg_attribute a ON a.attrelid = "
                                             + "i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = "
                                             + "'%s'::regclass) I ON I.column_name=C.column_name "
                                             + "WHERE table_name='%s' order by ordinal_position;",
                                             tableId.getName(), tableId.getName());

    List<FieldType> fields = new ArrayList<>();
    List<String> primaryKeys = new ArrayList<>();
    List<String> indexes = new ArrayList<>();

    try (Connection connection = dataSource.getConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(schemaStatement);
      while (resultSet.next()) {
        String columnName = resultSet.getString("column_name");
        String indexType = resultSet.getString("is_primarykey_index");

        fields.add(new FieldType(columnName, fromSQLType(resultSet.getString("data_type"))));

        // "t" as true indicates primary key index, "f" as false indicates normal index, null means it's just a column
        if ("t".equalsIgnoreCase(indexType)) {
          primaryKeys.add(columnName);
        } else if ("f".equalsIgnoreCase(indexType)) {
          indexes.add(columnName);
        }
      }
    } catch (SQLException e) {
      // PostgreSQL error 42P01: database query is on an undefined table
      if (TABLE_UNDEFINED_SQL_STATE.equalsIgnoreCase(e.getSQLState())) {
        throw new TableNotFoundException(tableId);
      }
      throw new SQLException(e.getCause());
    }

    if (fields.isEmpty()) {
      throw new TableNotFoundException(tableId);
    }

    return new StructuredTableSchema(tableId, fields, primaryKeys, indexes);
  }
}
