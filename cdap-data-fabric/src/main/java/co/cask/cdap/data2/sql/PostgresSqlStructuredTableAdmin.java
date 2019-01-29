/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.data2.sql;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import co.cask.cdap.spi.data.table.StructuredTableSpecificationRegistry;
import co.cask.cdap.spi.data.table.field.FieldType;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.sql.DataSource;

/**
 * Sql structured admin to use jdbc connection to create and drop tables.
 */
public class PostgresSqlStructuredTableAdmin implements StructuredTableAdmin {
  private final DataSource dataSource;

  public PostgresSqlStructuredTableAdmin(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void create(StructuredTableSpecification spec) throws IOException, AlreadyExistsException {
    try (Connection connection = dataSource.getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet rs = metaData.getTables(null, null,
                                        spec.getTableId().getName(), null);
      if (rs.next()) {
        throw new AlreadyExistsException(spec.getTableId());
      }
      Statement statement = connection.createStatement();
      statement.execute(getCreateStatement(spec));
      StructuredTableSpecificationRegistry.registerSpecification(spec);
      statement.close();
    } catch (SQLException e) {
      throw new IOException(String.format("Error creating table %s", spec.getTableId()), e);
    }
  }

  @Nullable
  @Override
  public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
    return StructuredTableSpecificationRegistry.getSpecification(tableId);
  }

  @Override
  public void drop(StructuredTableId tableId) throws IOException {
    String sqlQuery = getDeleteStatement(tableId.getName());
    try (Connection connection = dataSource.getConnection()) {
      Statement statement = connection.createStatement();
      statement.execute(sqlQuery);
      StructuredTableSpecificationRegistry.removeSpecification(tableId);
    } catch (SQLException e) {
      throw new IOException(String.format("Error dropping table %s", tableId), e);
    }
  }

  private String getCreateStatement(StructuredTableSpecification specification) {
    StringBuilder createStmt = new StringBuilder();
    createStmt.append("CREATE TABLE ").append(specification.getTableId().getName()).append(" (");

    // append the columns with sql type
    createStmt.append(
      specification.getFieldTypes().stream()
        .map(f -> f.getName() + " " + getPostgresSqlType(f))
        .collect(Collectors.joining(","))
    );

    // append primary key
    createStmt.append(", PRIMARY KEY (").append(Joiner.on(",").join(specification.getPrimaryKeys())).append("))");
    return createStmt.toString();
  }

  private String getDeleteStatement(String tableName) {
    return "DROP TABLE " + tableName + ";";
  }

  private String getPostgresSqlType(FieldType field) {
    String sqlType;

    FieldType.Type type = field.getType();
    switch (type) {
      case INTEGER:
        sqlType = "int";
        break;
      case STRING:
        sqlType = "text";
        break;
      case LONG:
        sqlType = "bigint";
        break;
      case DOUBLE:
        sqlType = "double precision";
        break;
      case FLOAT:
        sqlType = "real";
        break;
      case BYTES:
        sqlType = "bytea";
        break;
      default:
        // this should never happen since all the fields are from the specification and validated there
        throw new IllegalStateException(
          String.format("The type %s of the field %s is not a valid type", type, field.getName()));
    }
    return sqlType;
  }
}
