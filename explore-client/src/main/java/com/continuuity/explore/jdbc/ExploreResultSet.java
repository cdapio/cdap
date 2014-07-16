/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.explore.jdbc;

import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Result set created by an {@link ExploreStatement}, containing the results of a query made to the Explore service.
 */
public class ExploreResultSet extends BaseExploreResultSet {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreResultSet.class);

  private boolean hasMoreResults = true;
  private Iterator<Result> rowsItr;
  private Result currentRow;
  private ExploreResultSetMetaData metaData;

  private ExploreStatement statement;
  private Handle stmtHandle;
  private Explore exploreClient;

  public ExploreResultSet(Explore exploreClient, ExploreStatement statement, Handle stmtHandle) throws SQLException {
    this(exploreClient, stmtHandle, statement.getFetchSize());
    this.statement = statement;
  }

  public ExploreResultSet(Explore exploreClient, Handle stmtHandle, int fetchSize) throws SQLException {
    this.exploreClient = exploreClient;
    this.statement = null;
    this.stmtHandle = stmtHandle;
    setFetchSize(fetchSize);
  }

  @Override
  public boolean next() throws SQLException {
    if (isClosed()) {
      throw new SQLException("ResultSet is closed");
    }

    if (!hasMoreResults) {
      return false;
    }

    if (rowsItr != null && rowsItr.hasNext()) {
      currentRow = rowsItr.next();
      return true;
    }

    try {
      if (stmtHandle == null) {
        throw new SQLException("Handle is null.");
      }
      List<Result> fetchedRows;
      fetchedRows = exploreClient.nextResults(stmtHandle, getFetchSize());
      rowsItr = fetchedRows.iterator();
      if (!rowsItr.hasNext()) {
        hasMoreResults = false;
        currentRow = null;
        return false;
      }
      currentRow = rowsItr.next();
      return true;
    } catch (HandleNotFoundException e) {
      LOG.error("Could not fetch results with handle {}", stmtHandle);
      throw new SQLException("Could not fetch results with handle " + stmtHandle, e);
    } catch (ExploreException e) {
      LOG.error("Caught exception", e);
      throw new SQLException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    if (isClosed()) {
      // No-op
      return;
    }
    try {
      if (statement != null) {
        statement.closeClientOperation();
      }
    } finally {
      exploreClient = null;
      stmtHandle = null;
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
        List<ColumnDesc> columnDescs = exploreClient.getResultSchema(stmtHandle);
        metaData = new ExploreResultSetMetaData(columnDescs);
      } catch (ExploreException e) {
        LOG.error("Caught exception", e);
        throw new SQLException(e);
      } catch (HandleNotFoundException e) {
        LOG.error("Handle not found when retrieving result set meta data", e);
        throw new SQLException("Handle not found when retrieving result set meta data", e);
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
}
