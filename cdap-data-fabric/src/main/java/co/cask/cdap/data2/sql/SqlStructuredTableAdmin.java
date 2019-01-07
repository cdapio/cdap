/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.spi.data.InvalidFieldException;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import co.cask.cdap.spi.data.table.field.FieldType;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class SqlStructuredTableAdmin implements StructuredTableAdmin {
  private final Connection connection;
  // TODO: CDAP-14673 convert this into schema registry
  private final Map<StructuredTableId, StructuredTableSpecification> specMap;

  public SqlStructuredTableAdmin(Connection connection) {
    this.connection = connection;
    this.specMap = new HashMap<>();
  }

  @Override
  public void create(StructuredTableSpecification spec) throws IOException {
    String sqlQuery = getCreateStatement(spec);
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(true);
      statement.execute(sqlQuery);
      specMap.put(spec.getTableId(), spec);
    } catch (SQLException e) {
      throw new IOException(String.format("Error creating table %s", spec.getTableId()), e);
    }
  }

  @Nullable
  @Override
  public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
    return specMap.get(tableId);
  }

  @Override
  public void drop(StructuredTableId tableId) throws IOException {
    String sqlQuery = getDeleteStatement(tableId.getName());
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(true);
      statement.execute(sqlQuery);
      specMap.remove(tableId);
    } catch (SQLException e) {
      throw new IOException(String.format("Error dropping table %s", tableId), e);
    }
  }

  private String getCreateStatement(StructuredTableSpecification specification) {
    StringBuilder createStmt = new StringBuilder();
    createStmt.append("CREATE TABLE ").append(specification.getTableId().getName()).append(" (");

    for (FieldType field : specification.getFieldTypes()) {
      String name = field.getName();
      createStmt.append(name).append(" ");
      FieldType.Type fieldType = field.getType();
      String sqlType;

      switch (fieldType) {
        case INTEGER:
          sqlType = "int";
          break;
        case STRING:
          sqlType = "varchar(255)";
          break;
        case LONG:
          sqlType = "bigint";
          break;
        case DOUBLE:
          sqlType = "double(15, 25)";
          break;
        case FLOAT:
          sqlType = "float(7, 24)";
          break;
        default:
          throw new InvalidFieldException(specification.getTableId(), name);
      }
      createStmt.append(sqlType).append(", ");
    }

    // remove trailing ", "
    createStmt.deleteCharAt(createStmt.length() - 1)
      .deleteCharAt(createStmt.length() - 1)
      .append(",");

    createStmt.append(" PRIMARY KEY (").append(Joiner.on(",").join(specification.getPrimaryKeys())).append("))");

    return createStmt.toString();
  }

  private String getDeleteStatement(String tableName) {
    return "drop table " + tableName + ";";
  }
}
