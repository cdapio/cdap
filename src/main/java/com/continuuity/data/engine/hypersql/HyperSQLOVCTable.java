package com.continuuity.data.engine.hypersql;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.Scanner;
import com.google.common.base.Objects;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Implementation of an OVCTable over a HyperSQL table.
 */
public class HyperSQLOVCTable
implements OrderedVersionedColumnarTable {

  private static final Logger Log =
      LoggerFactory.getLogger(HyperSQLOVCTable.class);

  private final String tableName;
  private final String quotedTableName;
  private final Connection connection;

  HyperSQLOVCTable(final String tableName, Connection connection) {
    // quoting the table name to ensure that arbitrary characters are legal
    this.tableName = tableName;
    this.quotedTableName = "\""+ tableName + "\"";
    this.connection = connection;
  }

  private static final String ROW_TYPE = "VARBINARY(1024)";
  private static final String COLUMN_TYPE = "VARBINARY(1024)";
  private static final String VERSION_TYPE = "BIGINT";
  private static final String TYPE_TYPE = "INT";
  private static final String VALUE_TYPE = "VARBINARY(1048576)";

  private static final byte [] NULL_VAL = new byte [0];

  private enum Type {
    UNDELETE_ALL (0),
    DELETE_ALL   (1),
    DELETE       (2),
    VALUE        (3);
    int i;
    Type(int i) {
      this.i = i;
    }
    static Type from(int i) {
      switch (i) {
        case 0: return UNDELETE_ALL;
        case 1: return DELETE_ALL;
        case 2: return DELETE;
        case 3: return VALUE;
      }
      return null;
    }
    boolean isUndeleteAll() {
      return this == UNDELETE_ALL;
    }
    boolean isDeleteAll() {
      return this == DELETE_ALL;
    }
    boolean isDelete() {
      return this == DELETE;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("name", name())
          .add("int", this.i)
          .toString();
    }
  }

  boolean openTable() throws OperationException {
    try {
      DatabaseMetaData md = this.connection.getMetaData();
      ResultSet rs = md.getTables(null, null, this.tableName, null);
      if (rs.next()) {
        // System.out.println("Table " + this.tableName + " already exists");
        return true;
      }
      return false;
    } catch (SQLException e) {
      handleSQLException(e, "create");
    }
    return false;
  }

  void initializeTable() throws OperationException {
    String createStatement = "CREATE CACHED TABLE " + this.quotedTableName + " (" +
        "rowkey " + ROW_TYPE + " NOT NULL, " +
        "column " + COLUMN_TYPE + " NOT NULL, " +
        "version " + VERSION_TYPE + " NOT NULL, " +
        "kvtype " + TYPE_TYPE + " NOT NULL, " +
        "id BIGINT IDENTITY, " +
        "value " + VALUE_TYPE + " NOT NULL)";
    String indexStatement = "CREATE INDEX \"theBigIndex" +
      this.tableName + "\" ON " +
        this.quotedTableName + " (rowkey, column, version DESC, kvtype, id DESC)";

    Statement stmt = null;
    try {
      stmt = this.connection.createStatement();
      // System.out.println(createStatement);
      stmt.executeUpdate(createStatement);
      // System.out.println(indexStatement);
      stmt.executeUpdate(indexStatement);
    } catch (SQLException e) {
      // fail silent if table/index already exists
      // SQL state for determining the duplicate table create exception
      // http://docs.oracle.com/javase/tutorial/jdbc/basics/sqlexception.html
      if(!e.getSQLState().equalsIgnoreCase("42504")) {
        handleSQLException(e, "create");
      }
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
  }

  // Administrative Operations

  @Override
  public void clear() throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement("DELETE FROM " + this.quotedTableName);
      ps.executeUpdate();
    } catch (SQLException e) {
      handleSQLException(e, "delete");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
  }

  // Simple Write Operations

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value)
      throws OperationException {
    performInsert(row, column, version, Type.VALUE, value);
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values)
      throws OperationException {
    performInsert(row, columns, version, Type.VALUE, values);
  }

  // Delete Operations

  @Override
  public void delete(byte[] row, byte[] column, long version)
      throws OperationException {
    performInsert(row, column, version, Type.DELETE, NULL_VAL);
  }

  @Override
  public void delete(byte[] row, byte[][] columns, long version)
      throws OperationException {
    performInsert(row, columns, version, Type.DELETE,
        generateDeleteVals(columns.length));
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version)
      throws OperationException {
    performInsert(row, column, version, Type.DELETE_ALL, NULL_VAL);
  }

  @Override
  public void deleteAll(byte[] row, byte[][] columns, long version)
      throws OperationException {
    performInsert(row, columns, version, Type.DELETE_ALL,
        generateDeleteVals(columns.length));
  }

  @Override
  public void undeleteAll(byte[] row, byte[] column, long version)
      throws OperationException {
    performInsert(row, column, version, Type.UNDELETE_ALL, NULL_VAL);
  }

  @Override
  public void undeleteAll(byte[] row, byte[][] columns, long version)
      throws OperationException {
    performInsert(row, columns, version, Type.UNDELETE_ALL,
        generateDeleteVals(columns.length));
  }

  private byte[][] generateDeleteVals(int length) {
    byte [][] values = new byte[length][];
    for (int i=0;i<values.length;i++) values[i] = NULL_VAL;
    return values;
  }

  // Read-Modify-Write Operations

  @Override
  public long increment(byte[] row, byte[] column, long amount,
      ReadPointer readPointer, long writeVersion) throws OperationException {
    PreparedStatement ps = null;
    long newAmount = amount;
    try {
      try {
        ps = this.connection.prepareStatement(
            "SELECT version, kvtype, id, value " +
                "FROM " + this.quotedTableName + " " +
                "WHERE rowkey = ? AND column = ? " +
                "ORDER BY version DESC, kvtype ASC, id DESC");
        ps.setBytes(1, row);
        ps.setBytes(2, column);
        ResultSet result = ps.executeQuery();
        ImmutablePair<Long, byte[]> latest =
            filteredLatest(result, readPointer);
        if (latest != null) {
          newAmount += Bytes.toLong(latest.getSecond());
        }
        ps.close();
      } catch (SQLException e) {
        handleSQLException(e, "select");
      }
      try {
        ps = this.connection.prepareStatement(
            "INSERT INTO " + this.quotedTableName +
                " (rowkey, column, version, kvtype, value) " +
                "VALUES ( ? , ? , ? , ? , ?)");
        ps.setBytes(1, row);
        ps.setBytes(2, column);
        ps.setLong(3, writeVersion);
        ps.setInt(4, Type.VALUE.i);
        ps.setBytes(5, Bytes.toBytes(newAmount));
        ps.executeUpdate();
      } catch (SQLException e) {
        handleSQLException(e, "insert");
      }
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
    return newAmount;
  }

  @Override
  public Map<byte[], Long> increment(byte[] row, byte[][] columns,
      long[] amounts, ReadPointer readPointer, long writeVersion)
      throws OperationException {
    // TODO: This is not atomic across columns, it just loops over them
    Map<byte[],Long> ret = new TreeMap<byte[],Long>(Bytes.BYTES_COMPARATOR);
    for (int i=0; i<columns.length; i++) {
      ret.put(columns[i], increment(row, columns[i], amounts[i],
          readPointer, writeVersion));
    }
    return ret;
  }

  @Override
  public void compareAndSwap(byte[] row, byte[] column,
                             byte[] expectedValue, byte[] newValue, ReadPointer readPointer,
                             long writeVersion) throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "SELECT version, kvtype, id, value " +
          "FROM " + this.quotedTableName + " " +
          "WHERE rowkey = ? AND column = ? " +
          "ORDER BY version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ResultSet result = ps.executeQuery();
      ImmutablePair<Long, byte[]> latest = filteredLatest(result, readPointer);
      byte [] existingValue = latest == null ? null : latest.getSecond();
      ps.close(); ps = null;

      // handle invalid cases regarding non-existent values
      if (existingValue == null && expectedValue != null)
        throw new OperationException(StatusCode.WRITE_CONFLICT,
            "CompareAndSwap expected value mismatch");
      if (existingValue != null && expectedValue == null)
        throw new OperationException(StatusCode.WRITE_CONFLICT,
            "CompareAndSwap expected value mismatch");

      // if nothing existed, just write
      // TODO: this is not atomic and thus is broken?  how to make it atomic?
      if (expectedValue == null) {
        put(row, column, writeVersion, newValue);
        return;
      }

      // check if expected == existing, fail if not
      if (!Bytes.equals(expectedValue, existingValue))
        throw new OperationException(StatusCode.WRITE_CONFLICT,
            "CompareAndSwap expected value mismatch");

      // if newValue is null, just delete.
      // TODO: this can't be rolled back!
      if (newValue == null) {
        deleteAll(row, column, latest.getFirst());
        return;
      }

      // Perform update!
      ps = this.connection.prepareStatement(
          "INSERT INTO " + this.quotedTableName +
          " (rowkey, column, version, kvtype, value) VALUES ( ?, ? , ? , ? , ? )");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ps.setLong(3, writeVersion);
      ps.setInt(4, Type.VALUE.i);
      ps.setBytes(5, newValue);
      ps.executeUpdate();

    } catch (SQLException e) {
        handleSQLException(e, "compareAndSwap");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
  }

  // Read Operations

  @Override
  public OperationResult<Map<byte[], byte[]>>
  get(byte[] row, ReadPointer readPointer) throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "SELECT column, version, kvtype, id, value " +
              "FROM " + this.quotedTableName + " " +
              "WHERE rowkey = ? " +
          "ORDER BY column ASC, version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      ResultSet result = ps.executeQuery();
      return new OperationResult<Map<byte[], byte[]>>(
          filteredLatestColumns(result, readPointer, -1));

    } catch (SQLException e) {
      handleSQLException(e, "select");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
    throw new InternalError("this point should never be reached.");
  }

  @Override
  public OperationResult<byte[]>
  get(byte[] row, byte[] column, ReadPointer readPointer)
      throws OperationException {

    OperationResult<ImmutablePair<byte[], Long>> res =
        getWithVersion(row, column, readPointer);
    if (res.isEmpty())
      return new OperationResult<byte[]>(res.getStatus(), res.getMessage());
    else
      return new OperationResult<byte[]>(res.getValue().getFirst());
  }

  @Override
  public OperationResult<ImmutablePair<byte[], Long>>
  getWithVersion(byte[] row, byte[] column, ReadPointer readPointer)
      throws OperationException {

    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "SELECT version, kvtype, id, value " +
              "FROM " + this.quotedTableName + " " +
              "WHERE rowkey = ? AND column = ? " +
          "ORDER BY version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ResultSet result = ps.executeQuery();
      ImmutablePair<Long,byte[]> latest = filteredLatest(result, readPointer);

      if (latest == null)
        return new OperationResult<ImmutablePair<byte[], Long>>(
            StatusCode.KEY_NOT_FOUND);

      return new OperationResult<ImmutablePair<byte[], Long>>(
          new ImmutablePair<byte[],Long>(
              latest.getSecond(), latest.getFirst()));

    } catch (SQLException e) {
      handleSQLException(e, "select", ps);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
    throw new InternalError("this point should never be reached.");
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit,
      ReadPointer readPointer) throws OperationException {

    PreparedStatement ps = null;
    try {
      String columnChecks = "";
      if (startColumn != null) columnChecks += " AND column >= ?";
      if (stopColumn != null) columnChecks += " AND column < ?";
      // we cannot push down the limit into the SQL, because we post-filter
      // by timestamp, hence we can't know the correct limit pre-filtering.
      ps = this.connection.prepareStatement(
          "SELECT column, version, kvtype, id, value " +
              "FROM " + this.quotedTableName + " " +
              "WHERE rowkey = ?" + columnChecks + " " +
          "ORDER BY column ASC, version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      int idx = 2;
      if (startColumn != null) {
        ps.setBytes(idx, startColumn);
        idx++;
      }
      if (stopColumn != null) {
        ps.setBytes(idx, stopColumn);
      }
      ResultSet result = ps.executeQuery();
      if (result == null) {
        return new OperationResult<Map<byte[], byte[]>>(
            StatusCode.KEY_NOT_FOUND);
      }
      Map<byte[], byte[]> filtered =
          filteredLatestColumns(result, readPointer, limit);
      if (filtered.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(
            StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(filtered);
      }
    } catch (SQLException e) {
      handleSQLException(e, "select");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
    throw new InternalError("this point should never be reached.");
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  get(byte[] row, byte[][] columns, ReadPointer readPointer) throws OperationException {
    PreparedStatement ps = null;
    try {
      String [] columnChecks = new String[columns.length];
      String columnCheck = "column = ?";
      for (int i=0; i<columnChecks.length; i++) {
        columnChecks[i] = columnCheck;
      }
      columnCheck = StringUtils.join(columnChecks, " OR ");

      ps = this.connection.prepareStatement(
          "SELECT column, version, kvtype, id, value " +
              "FROM " + this.quotedTableName + " " +
              "WHERE rowkey = ? AND (" + columnCheck + ") " +
          "ORDER BY column ASC, version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      int idx = 2;
      for (byte [] column : columns) {
        ps.setBytes(idx++, column);
      }
      ResultSet result = ps.executeQuery();
      if (result == null) {
        return new OperationResult<Map<byte[], byte[]>>(
            StatusCode.KEY_NOT_FOUND);
      }
      Map<byte[], byte[]> filtered =
          filteredLatestColumns(result, readPointer, -1);
      if (filtered.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(
            StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(filtered);
      }
    } catch (SQLException e) {
      handleSQLException(e, "select");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
    throw new InternalError("this point should never be reached.");
  }

  // Scan Operations

  @Override
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer)
      throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "SELECT rowkey, column, version, kvtype, id " +
              "FROM " + this.quotedTableName + " " +
          "ORDER BY rowkey ASC, column ASC, version DESC, kvtype ASC, id DESC");
      ResultSet result = ps.executeQuery();
      List<byte[]> keys = new ArrayList<byte[]>(limit > 1024 ? 1024 : limit);
      int returned = 0;
      int skipped = 0;
      long lastDelete = -1;
      long undeleted = -1;
      byte [] lastRow = new byte[0];
      byte [] curRow = new byte[0];
      byte [] curCol = new byte [0];
      byte [] lastCol = new byte [0];
      while (result.next() && returned < limit) {
        // See if we already included this row
        byte [] row = result.getBytes(1);
        if (Bytes.equals(lastRow, row)) continue;

        // See if this is a new row (clear col/del tracking if so)
        if (!Bytes.equals(curRow, row)) {
          lastCol = new byte[0];
          curCol = new byte[0];
          lastDelete = -1;
          undeleted = -1;
        }
        curRow = row;

        // Check visibility of this entry
        long curVersion = result.getLong(3);
        // Check if this entry is visible, skip if not
        if (!readPointer.isVisible(curVersion)) continue;

        byte [] column = result.getBytes(2);
        // Check if this column has been completely deleted
        if (Bytes.equals(lastCol, column)) {
          continue;
        }
        // Check if this is a new column, reset delete pointers if so
        if (!Bytes.equals(curCol, column)) {
          curCol = column;
          lastDelete = -1;
          undeleted = -1;
        }
        // Check if type is a delete and execute accordingly
        Type type = Type.from(result.getInt(4));
        if (type.isUndeleteAll()) {
          undeleted = curVersion;
          continue;
        }
        if (type.isDeleteAll()) {
          if (undeleted == curVersion) continue;
          else {
            // The rest of this column has been deleted, act like we returned it
            lastCol = column;
            continue;
          }
        }
        if (type.isDelete()) {
          lastDelete = curVersion;
          continue;
        }
        if (curVersion == lastDelete) continue;
        // Column is valid, therefore row is valid, add row
        lastRow = row;
        if (skipped < offset) {
          skipped++;
        } else {
          keys.add(row);
          returned++;
        }
      }
      return keys;
    } catch (SQLException e) {
      handleSQLException(e, "select");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
    throw new InternalError("this point should never be reached.");
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer) {
    throw new UnsupportedOperationException("Scans currently not supported");
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns,
      ReadPointer readPointer) {
    throw new UnsupportedOperationException("Scans currently not supported");
  }

  @Override
  public Scanner scan(ReadPointer readPointer) {
    throw new UnsupportedOperationException("Scans currently not supported");
  }

  // Private Helper Methods

  private void performInsert(byte [] row, byte [] column, long version,
      Type type, byte [] value) throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "INSERT INTO " + this.quotedTableName +
          " (rowkey, column, version, kvtype, value) VALUES ( ?, ?, ?, ?, ? )");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ps.setLong(3, version);
      ps.setInt(4, type.i);
      ps.setBytes(5, value);
      ps.executeUpdate();
    } catch (SQLException e) {
      handleSQLException(e, "insert");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
  }

  private void performInsert(byte [] row, byte [][] columns, long version,
      Type type, byte [][] values) throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "INSERT INTO " + this.quotedTableName +
          " (rowkey, column, version, kvtype, value) VALUES ( ?, ?, ?, ?, ? )");
      for (int i=0; i<columns.length; i++) {
        ps.setBytes(1, row);
        ps.setBytes(2, columns[i]);
        ps.setLong(3, version);
        ps.setInt(4, type.i);
        ps.setBytes(5, values[i]);
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      handleSQLException(e, "insert");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          handleSQLException(e, "close");
        }
      }
    }
  }

  /**
   * Result has (version, kvtype, id, value)
   * @throws SQLException
   */
  private ImmutablePair<Long, byte[]> filteredLatest(
      ResultSet result, ReadPointer readPointer) throws SQLException {
    if (result == null) return null;
    long lastDelete = -1;
    long undeleted = -1;
    while (result.next()) {
      long curVersion = result.getLong(1);
      if (!readPointer.isVisible(curVersion)) continue;
      Type type = Type.from(result.getInt(2));
      if (type.isUndeleteAll()) {
        undeleted = curVersion;
        continue;
      }
      if (type.isDeleteAll()) {
        if (undeleted == curVersion) continue;
        else break;
      }
      if (type.isDelete()) {
        lastDelete = curVersion;
        continue;
      }
      if (curVersion == lastDelete) continue;
      return new ImmutablePair<Long, byte[]>(curVersion,
          result.getBytes(4));
    }
    return null;
  }

  /**
   * Result has (column, version, kvtype, id, value)
   * @throws SQLException
   */
  private Map<byte[], byte[]> filteredLatestColumns(ResultSet result,
      ReadPointer readPointer, int limit) throws SQLException {

    // negative limit means unlimited results
    if (limit <= 0) limit = Integer.MAX_VALUE;

    Map<byte[],byte[]> map = new TreeMap<byte[],byte[]>(Bytes.BYTES_COMPARATOR);
    if (result == null) return map;
    byte [] curCol = new byte [0];
    byte [] lastCol = new byte [0];
    long lastDelete = -1;
    long undeleted = -1;
    while (result.next()) {
      long curVersion = result.getLong(2);
      // Check if this entry is visible, skip if not
      if (!readPointer.isVisible(curVersion)) continue;
      byte [] column = result.getBytes(1);
      // Check if this column has already been included in result, skip if so
      if (Bytes.equals(lastCol, column)) {
        continue;
      }
      // Check if this is a new column, reset delete pointers if so
      if (!Bytes.equals(curCol, column)) {
        curCol = column;
        lastDelete = -1;
        undeleted = -1;
      }
      // Check if type is a delete and execute accordingly
      Type type = Type.from(result.getInt(3));
      if (type.isUndeleteAll()) {
        undeleted = curVersion;
        continue;
      }
      if (type.isDeleteAll()) {
        if (undeleted == curVersion) continue;
        else {
          // The rest of this column has been deleted, act like we returned it
          lastCol = column;
          continue;
        }
      }
      if (type.isDelete()) {
        lastDelete = curVersion;
        continue;
      }
      if (curVersion == lastDelete) continue;
      lastCol = column;
      map.put(column, result.getBytes(5));

      // break out if limit reached
      if (map.size() >= limit) break;
    }
    return map;
  }

  // TODO: Let out exceptions?  These are only for code bugs since we are in
  //       memory and file modes only so availability not an issue?
  private void handleSQLException(SQLException e, String where)
      throws OperationException {
    String msg = "HyperSQL exception on " + where + "(error code = " +
        e.getErrorCode() + ")";
    Log.error(msg, e);
    throw new OperationException(StatusCode.SQL_ERROR, msg, e);
  }

  private void handleSQLException(SQLException e, String where,
      PreparedStatement ps) throws OperationException {
    String msg = "HyperSQL exception on " + where + "(error code = " +
        e.getErrorCode() + ") (statement = " + ps.toString() + ")";
    Log.error(msg, e);
    throw new OperationException(StatusCode.SQL_ERROR, msg, e);
  }
}
