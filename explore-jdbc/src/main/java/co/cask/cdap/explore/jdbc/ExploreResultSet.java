/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.explore.jdbc;

import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Result set created by an {@link ExploreStatement}, containing the results of a query made to the Explore service.
 */
public class ExploreResultSet extends BaseExploreResultSet {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreResultSet.class);

  private QueryResult currentRow;
  private int nextRowNb = 1;
  private ExploreResultSetMetaData metaData;

  private ExploreStatement statement;
  private int maxRows = 0;

  private ExploreExecutionResult executionResult;

  ExploreResultSet(ExploreExecutionResult executionResult, ExploreStatement statement, int maxRows)
    throws SQLException {
    this(executionResult, statement.getFetchSize());
    this.statement = statement;
    this.maxRows = maxRows;
  }

  public ExploreResultSet(ExploreExecutionResult executionResult, int fetchSize) throws SQLException {
    this.executionResult = executionResult;
    setFetchSize(fetchSize);
  }

  @Override
  public boolean next() throws SQLException {
    if (isClosed()) {
      throw new SQLException("ResultSet is closed");
    }
    boolean res = (maxRows <= 0 || nextRowNb <= maxRows) && executionResult.hasNext();
    if (res) {
      nextRowNb++;
      currentRow = executionResult.next();
    }
    return res;
  }

  @Override
  public void close() throws SQLException {
    if (isClosed()) {
      // No-op
      return;
    }
    try {
      executionResult.close();
      if (statement != null) {
        statement.closeClientOperation();
      }
    } catch (IOException e) {
      LOG.error("Could not close the query results", e);
      throw new SQLException(e);
    } finally {
      executionResult = null;
      statement = null;
      setIsClosed(true);
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Resultset is closed");
    }
    if (metaData == null) {
      try {
        List<ColumnDesc> columnDescs = executionResult.getResultSchema();
        metaData = new ExploreResultSetMetaData(columnDescs);
      } catch (ExploreException e) {
        LOG.error("Caught exception", e);
        throw new SQLException(e);
      }
    }
    return metaData;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (isClosed()) {
      throw new SQLException("ResultSet is closed");
    }
    if (currentRow == null) {
      throw new SQLException("No row found.");
    }
    List<Object> columns = currentRow.getColumns();
    if (columns.isEmpty()) {
      throw new SQLException("RowSet does not contain any columns!");
    }
    if (columnIndex < 1 || columnIndex > columns.size()) {
      throw new SQLException("Invalid columnIndex: " + columnIndex);
    }

    int columnType = getMetaData().getColumnType(columnIndex);
    try {
      Object evaluated = evaluate(columnType, columns.get(columnIndex - 1));
      setWasNull(evaluated == null);
      return evaluated;
    } catch (Exception e) {
      e.printStackTrace();
      throw new SQLException("Unrecognized column type:" + columnType, e);
    }
  }

  @Override
  public int findColumn(String name) throws SQLException {
    if (isClosed()) {
      throw new SQLException("Resultset is closed");
    }
    if (metaData == null) {
      getMetaData();
    }
    // Column names are case insensitive, as per the ResultSet interface javadoc
    return metaData.getColumnPosition(name.toLowerCase());
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    if (isClosed()) {
      throw new SQLException("Resultset is closed");
    }
    executionResult.setFetchSize(rows);
  }

  @Override
  public int getFetchSize() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Resultset is closed");
    }
    return executionResult.getFetchSize();
  }
}
