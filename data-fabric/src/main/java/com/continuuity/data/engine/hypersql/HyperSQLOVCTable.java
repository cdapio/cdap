package com.continuuity.data.engine.hypersql;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.table.AbstractOVCTable;
import com.continuuity.data.table.Scanner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Implementation of an OVCTable over a HyperSQL table.
 */
public class HyperSQLOVCTable extends AbstractOVCTable {

  private static final Logger Log = LoggerFactory.getLogger(HyperSQLOVCTable.class);

  private final String tableName;
  private final String quotedTableName;
  private final Connection connection;

  HyperSQLOVCTable(final String tableName, Connection connection) {
    // quoting the table name to ensure that arbitrary characters are legal
    this.tableName = tableName;
    this.quotedTableName = "\"" + tableName + "\"";
    this.connection = connection;
  }

  private static final String ROW_TYPE = "VARBINARY(1024)";
  private static final String COLUMN_TYPE = "VARBINARY(1024)";
  private static final String VERSION_TYPE = "BIGINT";
  private static final String TYPE_TYPE = "INT";
  private static final String VALUE_TYPE = "VARBINARY(1048576)";

  private static final byte[] NULL_VAL = new byte[0];

  private enum Type {
    UNDELETE_ALL(0),
    DELETE_ALL(1),
    DELETE(2),
    VALUE(3);
    int i;

    Type(int i) {
      this.i = i;
    }

    static Type from(int i) {
      switch (i) {
        case 0:
          return UNDELETE_ALL;
        case 1:
          return DELETE_ALL;
        case 2:
          return DELETE;
        case 3:
          return VALUE;
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
      return Objects.toStringHelper(this).add("name", name()).add("int", this.i).toString();
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
      throw createOperationException(e, "create");
    }
  }

  void initializeTable() throws OperationException {
    String createStatement = "CREATE CACHED TABLE " + this.quotedTableName + " (" +
      "rowkey " + ROW_TYPE + " NOT NULL, " +
      "column " + COLUMN_TYPE + " NOT NULL, " +
      "version " + VERSION_TYPE + " NOT NULL, " +
      "kvtype " + TYPE_TYPE + " NOT NULL, " +
      "id BIGINT IDENTITY, " +
      "value " + VALUE_TYPE + " NOT NULL," +
      "UNIQUE (rowkey, column, version, kvtype, id)) ";
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
      if (!e.getSQLState().equalsIgnoreCase("42504")) {
        throw createOperationException(e, "create");
      }
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
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
      throw createOperationException(e, "delete");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  // Simple Write Operations

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value) throws OperationException {
    performInsert(row, column, version, Type.VALUE, value);
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values) throws OperationException {
    performInsert(row, columns, version, Type.VALUE, values);
  }

  @Override
  public void put(byte[][] rows, byte[][] columns, long version, byte[][] values) throws OperationException {
    performInsert(rows, columns, version, Type.VALUE, values);
  }

  @Override
  public void put(byte[][] rows, byte[][][] columnsPerRow, long version, byte[][][] valuesPerRow)
    throws OperationException {
    performInsert(rows, columnsPerRow, version, Type.VALUE, valuesPerRow);
  }

  // Delete Operations

  @Override
  public void delete(byte[] row, byte[] column, long version) throws OperationException {
    performInsert(row, column, version, Type.DELETE, NULL_VAL);
  }

  @Override
  public void delete(byte[] row, byte[][] columns, long version) throws OperationException {
    performInsert(row, columns, version, Type.DELETE, generateDeleteVals(columns.length));
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version) throws OperationException {
    performInsert(row, column, version, Type.DELETE_ALL, NULL_VAL);
  }

  @Override
  public void deleteAll(byte[] row, byte[][] columns, long version) throws OperationException {
    performInsert(row, columns, version, Type.DELETE_ALL, generateDeleteVals(columns.length));
  }

  @Override
  public void deleteDirty(byte[] row, byte[][] columns, long version) throws OperationException {
    deleteAll(row, columns, version);
  }

  @Override
  public void deleteDirty(byte[][] rows) throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement("DELETE FROM " + this.quotedTableName +
                                              " WHERE rowkey = ? ");
      for (int i = 0; i < rows.length; i++) {
        ps.setBytes(1, rows[i]);
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      throw createOperationException(e, "delete");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  @Override
  public void deleteRowsDirtily(byte[] startRow, byte[] stopRow) throws OperationException {
    Preconditions.checkNotNull(startRow, "start row cannot be null");
    PreparedStatement ps = null;
    StringBuilder stmnt = new StringBuilder("DELETE FROM ");
    stmnt.append(this.quotedTableName);
    stmnt.append(" WHERE rowkey >= ?");
    if (stopRow != null) {
      stmnt.append(" AND rowkey < ?");
    }
    stmnt.append(";");
    try {
      ps = this.connection.prepareStatement(stmnt.toString());
      ps.setBytes(1, startRow);
      if (stopRow != null) {
        ps.setBytes(2, stopRow);
      }
      ps.executeUpdate();
    } catch (SQLException e) {
      throw createOperationException(e, "delete");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  @Override
  public void undeleteAll(byte[] row, byte[] column, long version) throws OperationException {
    performInsert(row, column, version, Type.UNDELETE_ALL, NULL_VAL);
  }

  @Override
  public void undeleteAll(byte[] row, byte[][] columns, long version) throws OperationException {
    performInsert(row, columns, version, Type.UNDELETE_ALL, generateDeleteVals(columns.length));
  }

  private byte[][] generateDeleteVals(int length) {
    byte[][] values = new byte[length][];
    for (int i = 0; i < values.length; i++) {
      values[i] = NULL_VAL;
    }
    return values;
  }

  // Read-Modify-Write Operations

  @Override
  public long increment(byte[] row, byte[] column, long amount, ReadPointer readPointer, long writeVersion)
    throws OperationException {
    PreparedStatement ps = null;
    long newAmount = amount;
    try {
      try {
        ps = this.connection.prepareStatement("SELECT version, kvtype, id, value " +
                                                "FROM " + this.quotedTableName + " " +
                                                "WHERE rowkey = ? AND column = ? " +
                                                "ORDER BY version DESC, kvtype ASC, id DESC");
        ps.setBytes(1, row);
        ps.setBytes(2, column);
        ResultSet result = ps.executeQuery();
        ImmutablePair<Long, byte[]> latest = filteredLatest(result, readPointer);
        if (latest != null) {
          try {
            newAmount += Bytes.toLong(latest.getSecond());
          } catch (IllegalArgumentException e) {
            throw new OperationException(StatusCode.ILLEGAL_INCREMENT, e.getMessage(), e);
          }
        }
        ps.close();
      } catch (SQLException e) {
        throw createOperationException(e, "select");
      }
      try {
        ps = this.connection.prepareStatement("INSERT INTO " + this.quotedTableName +
                                                " (rowkey, column, version, kvtype, value) " +
                                                "VALUES ( ? , ? , ? , ? , ?)");
        ps.setBytes(1, row);
        ps.setBytes(2, column);
        ps.setLong(3, writeVersion);
        ps.setInt(4, Type.VALUE.i);
        ps.setBytes(5, Bytes.toBytes(newAmount));
        ps.executeUpdate();
      } catch (SQLException e) {
        throw createOperationException(e, "insert");
      }
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
    return newAmount;
  }

  @Override
  public Map<byte[], Long> increment(byte[] row, byte[][] columns, long[] amounts, ReadPointer readPointer,
                                     long writeVersion)
    throws OperationException {
    // TODO: This is not atomic across columns, it just loops over them
    Map<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < columns.length; i++) {
      ret.put(columns[i], increment(row, columns[i], amounts[i], readPointer, writeVersion));
    }
    return ret;
  }

  @Override
  public synchronized long incrementAtomicDirtily(byte[] row, byte[] column, long amount) throws OperationException {
    PreparedStatement ps = null;
    try {
      //Execute select/insert update in one transaction.
      this.connection.setAutoCommit(false);

      ps = this.connection.prepareStatement("SELECT version, kvtype, id, value " +
                                              "FROM " + this.quotedTableName + " " +
                                              "WHERE rowkey = ? AND column = ? " +
                                              "ORDER BY version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      ps.setBytes(2, column);

      ResultSet result = ps.executeQuery();
      ps.close();

      ImmutablePair<Long, byte[]> latest = latest(result);
      long newAmount = amount;

      if (latest == null) {
        ps = this.connection.prepareStatement("INSERT INTO " + this.quotedTableName +
                                                " (rowkey, column, version, kvtype, value) VALUES ( ?, ? , ? , ? , " +
                                                "? )");
        ps.setBytes(1, row);
        ps.setBytes(2, column);
        ps.setLong(3, TransactionOracle.DIRTY_WRITE_VERSION);
        ps.setInt(4, Type.VALUE.i);
        ps.setBytes(5, Bytes.toBytes(newAmount));
        ps.executeUpdate();
      } else {
        ps = this.connection.prepareStatement("UPDATE " + this.quotedTableName +
                                                " SET value = ? " +
                                                " WHERE rowkey = ? AND column = ? AND version = ? ");
        newAmount = Bytes.toLong(latest.getSecond()) + amount;
        ps.setBytes(1, Bytes.toBytes(newAmount));
        ps.setBytes(2, row);
        ps.setBytes(3, column);
        ps.setLong(4, TransactionOracle.DIRTY_WRITE_VERSION);
        ps.executeUpdate();
      }
      this.connection.commit();
      return newAmount;
    } catch (SQLException e) {
      throw createOperationException(e, "compareAndSwap");
    } finally {
      try {
        this.connection.setAutoCommit(true);
        if (ps != null) {
          ps.close();
        }
      } catch (SQLException e) {
        throw createOperationException(e, "close");
      }
    }
  }

  @Override
  public void compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue,
                             ReadPointer readPointer, long writeVersion)
    throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement("SELECT version, kvtype, id, value " +
                                              "FROM " + this.quotedTableName + " " +
                                              "WHERE rowkey = ? AND column = ? " +
                                              "ORDER BY version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ResultSet result = ps.executeQuery();
      ImmutablePair<Long, byte[]> latest = filteredLatest(result, readPointer);
      byte[] existingValue = latest == null ? null : latest.getSecond();
      ps.close();
      ps = null;

      // handle invalid cases regarding non-existent values
      if (existingValue == null && expectedValue != null) {
        throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
      }
      if (existingValue != null && expectedValue == null) {
        throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
      }

      // if nothing existed, just write
      // TODO: this is not atomic and thus is broken?  how to make it atomic?
      if (expectedValue == null) {
        put(row, column, writeVersion, newValue);
        return;
      }

      // check if expected == existing, fail if not
      if (!Bytes.equals(expectedValue, existingValue)) {
        throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
      }

      // if newValue is null, just delete.
      // TODO: this can't be rolled back!
      if (newValue == null) {
        deleteAll(row, column, latest.getFirst());
        return;
      }

      // Perform update!
      ps = this.connection.prepareStatement("INSERT INTO " + this.quotedTableName +
                                              " (rowkey, column, version, kvtype, value) VALUES ( ?, ? , ? , ? , ? )");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ps.setLong(3, writeVersion);
      ps.setInt(4, Type.VALUE.i);
      ps.setBytes(5, newValue);
      ps.executeUpdate();

    } catch (SQLException e) {
      throw createOperationException(e, "compareAndSwap");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  @Override
  public boolean compareAndSwapDirty(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue)
    throws OperationException {
    PreparedStatement ps = null;
    Boolean oldAutoCommitValue = null;
    Integer oldTransactionIsolation = null;
    try {
      // Obtain read lock on the row that we are interested in
      oldAutoCommitValue = this.connection.getAutoCommit();
      oldTransactionIsolation = this.connection.getTransactionIsolation();
      this.connection.setAutoCommit(false);
      this.connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

      ps = this.connection.prepareStatement("SELECT version, kvtype, id, value " +
                                              "FROM " + this.quotedTableName + " " +
                                              "WHERE rowkey = ? AND column = ? " +
                                              "ORDER BY version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ResultSet result = ps.executeQuery();
      ImmutablePair<Long, byte[]> latest = latest(result);
      byte[] existingValue = latest == null ? null : latest.getSecond();
      ps.close();
      ps = null;

      if ((existingValue == null && expectedValue == null) || Bytes.equals(existingValue, expectedValue)) {
        // if newValue is null, just delete.
        if (newValue == null || newValue.length == 0) {
          deleteAll(row, column, latest.getFirst());
          ps = this.connection.prepareStatement("DELETE from " + this.quotedTableName + " " +
                                                  "WHERE rowkey = ? AND column = ? AND version = ? ");
          ps.setBytes(1, row);
          ps.setBytes(2, column);
          ps.setLong(3, TransactionOracle.DIRTY_WRITE_VERSION);
          ps.executeUpdate();
        } else {
          // Perform update!
          if (existingValue == null) {
            ps = this.connection.prepareStatement("INSERT INTO " + this.quotedTableName +
                                                    " (rowkey, column, version, kvtype, value) VALUES ( ?, ? , ? , " +
                                                    "? , ? )");
            ps.setBytes(1, row);
            ps.setBytes(2, column);
            ps.setLong(3, TransactionOracle.DIRTY_WRITE_VERSION);
            ps.setInt(4, Type.VALUE.i);
            ps.setBytes(5, newValue);
            ps.executeUpdate();
          } else {
            ps = this.connection.prepareStatement("UPDATE " + this.quotedTableName +
                                                    " SET value = ? " +
                                                    " WHERE rowkey = ? AND column = ? AND version = ? ");
            ps.setBytes(1, newValue);
            ps.setBytes(2, row);
            ps.setBytes(3, column);
            ps.setLong(4, TransactionOracle.DIRTY_WRITE_VERSION);
            ps.executeUpdate();
          }
        }
        this.connection.commit();
        return true;
      }

      return false;
    } catch (SQLException e) {
      throw createOperationException(e, "compareAndSwap");
    } finally {
      try {
        // Release the read lock
        if (oldAutoCommitValue != null) {
          this.connection.setAutoCommit(oldAutoCommitValue);
        }
        if (oldTransactionIsolation != null) {
          this.connection.setTransactionIsolation(oldTransactionIsolation);
        }
        if (ps != null) {
          ps.close();
        }
      } catch (SQLException e) {
        throw createOperationException(e, "close");
      }
    }
  }
// Read Operations

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, ReadPointer readPointer) throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement("SELECT column, version, kvtype, id, value " +
                                              "FROM " + this.quotedTableName + " " +
                                              "WHERE rowkey = ? " +
                                              "ORDER BY column ASC, version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      ResultSet result = ps.executeQuery();
      Map<byte[], byte[]> resMap = filteredLatestColumns(result, readPointer, -1);
      if (resMap.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
      }
      return new OperationResult<Map<byte[], byte[]>>(resMap);

    } catch (SQLException e) {
      throw createOperationException(e, "select");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  @Override
  public OperationResult<byte[]> get(byte[] row, byte[] column, ReadPointer readPointer) throws OperationException {

    OperationResult<ImmutablePair<byte[], Long>> res = getWithVersion(row, column, readPointer);
    if (res.isEmpty()) {
      return new OperationResult<byte[]>(res.getStatus(), res.getMessage());
    } else {
      return new OperationResult<byte[]>(res.getValue().getFirst());
    }
  }

  @Override
  public OperationResult<ImmutablePair<byte[], Long>> getWithVersion(byte[] row, byte[] column, ReadPointer readPointer)
    throws OperationException {

    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement("SELECT version, kvtype, id, value " +
                                              "FROM " + this.quotedTableName + " " +
                                              "WHERE rowkey = ? AND column = ? " +
                                              "ORDER BY version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ResultSet result = ps.executeQuery();
      ImmutablePair<Long, byte[]> latest = filteredLatest(result, readPointer);

      if (latest == null) {
        return new OperationResult<ImmutablePair<byte[], Long>>(StatusCode.KEY_NOT_FOUND);
      }

      return new OperationResult<ImmutablePair<byte[], Long>>(new ImmutablePair<byte[], Long>(latest.getSecond(),
                                                                                              latest.getFirst()));

    } catch (SQLException e) {
      throw createOperationException(e, "select", ps);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit,
                                                  ReadPointer readPointer)
    throws OperationException {

    PreparedStatement ps = null;
    try {
      String columnChecks = "";
      if (startColumn != null) {
        columnChecks += " AND column >= ?";
      }
      if (stopColumn != null) {
        columnChecks += " AND column < ?";
      }
      // we cannot push down the limit into the SQL, because we post-filter
      // by timestamp, hence we can't know the correct limit pre-filtering.
      ps = this.connection.prepareStatement("SELECT column, version, kvtype, id, value " +
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
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
      }
      Map<byte[], byte[]> filtered = filteredLatestColumns(result, readPointer, limit);
      if (filtered.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(filtered);
      }
    } catch (SQLException e) {
      throw createOperationException(e, "select");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[][] columns, ReadPointer readPointer)
    throws OperationException {
    PreparedStatement ps = null;
    try {
      String[] columnChecks = new String[columns.length];
      String columnCheck = "column = ?";
      for (int i = 0; i < columnChecks.length; i++) {
        columnChecks[i] = columnCheck;
      }
      columnCheck = StringUtils.join(columnChecks, " OR ");

      ps = this.connection.prepareStatement("SELECT column, version, kvtype, id, value " +
                                              "FROM " + this.quotedTableName + " " +
                                              "WHERE rowkey = ? AND (" + columnCheck + ") " +
                                              "ORDER BY column ASC, version DESC, kvtype ASC, id DESC");
      ps.setBytes(1, row);
      int idx = 2;
      for (byte[] column : columns) {
        ps.setBytes(idx++, column);
      }
      ResultSet result = ps.executeQuery();
      if (result == null) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
      }
      Map<byte[], byte[]> filtered = filteredLatestColumns(result, readPointer, -1);
      if (filtered.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(filtered);
      }
    } catch (SQLException e) {
      throw createOperationException(e, "select");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  @Override
  public OperationResult<byte[]> getDirty(byte[] row, byte[] column) throws OperationException {
    return get(row, column, TransactionOracle.DIRTY_READ_POINTER);
  }

  @Override
  public OperationResult<Map<byte[], Map<byte[], byte[]>>> getAllColumns(byte[][] rows, byte[][] columns,
                                                                         ReadPointer readPointer)
    throws OperationException {
    PreparedStatement ps = null;
    try {
      final String[] rowConditions = new String[rows.length];
      String rowConditionStr = " rowKey = ? ";
      for (int i = 0; i < rows.length; ++i) {
        rowConditions[i] = rowConditionStr;
      }
      rowConditionStr = StringUtils.join(rowConditions, " OR ");

      final String[] colConditions = new String[columns.length];
      String colConditionStr = " column = ? ";
      for (int i = 0; i < columns.length; ++i) {
        colConditions[i] = colConditionStr;
      }
      colConditionStr = StringUtils.join(colConditions, " OR ");

      ps = this.connection.prepareStatement("SELECT rowkey, column, version, kvtype, id, value " +
                                              "FROM " + this.quotedTableName + " " +
                                              "WHERE (" + rowConditionStr + ") AND ( " + colConditionStr + " ) " +
                                              "ORDER BY rowkey ASC, column ASC, version DESC, kvtype ASC, id DESC");

      int idx = 1;
      for (int i = 0; i < rows.length; ++i) {
        ps.setBytes(idx++, rows[i]);
      }
      for (int i = 0; i < columns.length; ++i) {
        ps.setBytes(idx++, columns[i]);
      }
      ResultSet result = ps.executeQuery();
      if (result == null) {
        return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(StatusCode.KEY_NOT_FOUND);
      }
      Map<byte[], Map<byte[], byte[]>> filtered = filteredLatestColumnsWithKey(result, readPointer, -1);
      if (filtered.isEmpty()) {
        return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], Map<byte[], byte[]>>>(filtered);
      }
    } catch (SQLException e) {
      throw createOperationException(e, "select");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  // Scan Operations

  @Override
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer) throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement("SELECT rowkey, column, version, kvtype, id " +
                                              "FROM " + this.quotedTableName + " " +
                                              "ORDER BY rowkey ASC, column ASC, version DESC, kvtype ASC, id DESC");
      ResultSet result = ps.executeQuery();
      List<byte[]> keys = new ArrayList<byte[]>(limit > 1024 ? 1024 : limit);
      int returned = 0;
      int skipped = 0;
      long lastDelete = -1;
      long undeleted = -1;
      byte[] lastRow = new byte[0];
      byte[] curRow = new byte[0];
      byte[] curCol = new byte[0];
      byte[] lastCol = new byte[0];
      while (result.next() && returned < limit) {
        // See if we already included this row
        byte[] row = result.getBytes(1);
        if (Bytes.equals(lastRow, row)) {
          continue;
        }

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
        if (!readPointer.isVisible(curVersion)) {
          continue;
        }

        byte[] column = result.getBytes(2);
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
          if (undeleted == curVersion) {
            continue;
          } else {
            // The rest of this column has been deleted, act like we returned it
            lastCol = column;
            continue;
          }
        }
        if (type.isDelete()) {
          lastDelete = curVersion;
          continue;
        }
        if (curVersion == lastDelete) {
          continue;
        }
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
      throw createOperationException(e, "select");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer) {
    PreparedStatement ps = null;
    try {
      if (startRow == null && stopRow == null){
        ps = this.connection.prepareStatement("SELECT rowkey, column, version, kvtype, id, value FROM " +
                                                 this.quotedTableName + " " + "ORDER BY rowKey, column ASC," +
                                                 "version DESC, kvtype ASC, id DESC");
      } else if (startRow == null){
        ps = this.connection.prepareStatement("SELECT rowkey, column, version, kvtype, id, value FROM " +
                                                 this.quotedTableName + "  " + "WHERE rowKey < ?"
                                                 + " " + "ORDER BY rowKey, column ASC," +
                                                 "version DESC, kvtype ASC, id DESC");
        ps.setBytes(1, stopRow);

      } else if (stopRow == null){
        ps = this.connection.prepareStatement("SELECT rowkey, column, version, kvtype, id, value FROM " +
                                                 this.quotedTableName + "  " + "WHERE rowKey >= ?"
                                                 + " " + "ORDER BY rowKey, column ASC," +
                                                 "version DESC, kvtype ASC, id DESC");
        ps.setBytes(1, startRow);
      } else {
        ps = this.connection.prepareStatement("SELECT rowkey, column, version, kvtype, id, value FROM " +
                                                 this.quotedTableName + "  " + "WHERE rowKey >= ? AND "   +
                                                 "rowKey < ? "  + " " + "ORDER BY rowKey, column ASC," +
                                                 "version DESC, kvtype ASC, id DESC");
        ps.setBytes(1, startRow);
        ps.setBytes(2, stopRow);
      }

      ResultSet resultSet = ps.executeQuery();
      return new ResultSetScanner(resultSet, readPointer);

    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      if (ps != null){
        try {
          ps.close();
        } catch (SQLException e){
          throw Throwables.propagate(e);
        }
      }
    }
  }

  // Private Helper Methods

  private void performInsert(byte[] row, byte[] column, long version, Type type, byte[] value)
    throws OperationException {
    performInsert(new byte[][]{row}, new byte[][]{column}, version, type, new byte[][]{value});
  }

  private void performInsert(byte[][] rows, byte[][] columns, long version, Type type, byte[][] values)
    throws OperationException {
    assert (rows.length == columns.length);
    assert (rows.length == values.length);

    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement("INSERT INTO " + this.quotedTableName +
                                              " (rowkey, column, version, kvtype, value) VALUES ( ?, ?, ?, ?, ? )");
      for (int i = 0; i < rows.length; ++i) {
        ps.setBytes(1, rows[i]);
        ps.setBytes(2, columns[i]);
        ps.setLong(3, version);
        ps.setInt(4, type.i);
        ps.setBytes(5, values[i]);
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      throw createOperationException(e, "insert");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  private void performInsert(byte[][] rows, byte[][][] columnsPerRow, long version, Type type, byte[][][] valuesPerRow)
    throws OperationException {
    assert (rows.length == columnsPerRow.length);
    assert (rows.length == valuesPerRow.length);

    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement("INSERT INTO " + this.quotedTableName +
                                              " (rowkey, column, version, kvtype, value) VALUES ( ?, ?, ?, ?, ? )");
      for (int i = 0; i < rows.length; ++i) {
        byte[][] columns = columnsPerRow[i];
        byte[][] values = valuesPerRow[i];
        for (int j = 0; j < columns.length; j++) {
          ps.setBytes(1, rows[i]);
          ps.setBytes(2, columns[j]);
          ps.setLong(3, version);
          ps.setInt(4, type.i);
          ps.setBytes(5, values[j]);
          ps.executeUpdate();
        }
      }
    } catch (SQLException e) {
      throw createOperationException(e, "insert");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  private void performInsert(byte[] row, byte[][] columns, long version, Type type, byte[][] values)
    throws OperationException {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement("INSERT INTO " + this.quotedTableName +
                                              " (rowkey, column, version, kvtype, value) VALUES ( ?, ?, ?, ?, ? )");
      for (int i = 0; i < columns.length; i++) {
        ps.setBytes(1, row);
        ps.setBytes(2, columns[i]);
        ps.setLong(3, version);
        ps.setInt(4, type.i);
        ps.setBytes(5, values[i]);
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      throw createOperationException(e, "insert");
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw createOperationException(e, "close");
        }
      }
    }
  }

  /**
   * Result has (version, kvtype, id, value).
   *
   * @throws SQLException
   */
  private ImmutablePair<Long, byte[]> latest(ResultSet result) throws SQLException {
    if (result == null) {
      return null;
    }
    long lastDelete = -1;
    long undeleted = -1;
    while (result.next()) {
      long curVersion = result.getLong(1);
      Type type = Type.from(result.getInt(2));
      if (type.isUndeleteAll()) {
        undeleted = curVersion;
        continue;
      }
      if (type.isDeleteAll()) {
        if (undeleted == curVersion) {
          continue;
        } else {
          break;
        }
      }
      if (type.isDelete()) {
        lastDelete = curVersion;
        continue;
      }
      if (curVersion == lastDelete) {
        continue;
      }
      return new ImmutablePair<Long, byte[]>(curVersion, result.getBytes(4));
    }
    return null;
  }

  /**
   * Result has (version, kvtype, id, value).
   *
   * @throws SQLException
   */
  private ImmutablePair<Long, byte[]> filteredLatest(ResultSet result, ReadPointer readPointer) throws SQLException {
    if (result == null) {
      return null;
    }
    long lastDelete = -1;
    long undeleted = -1;
    while (result.next()) {
      long curVersion = result.getLong(1);
      if (!readPointer.isVisible(curVersion)) {
        continue;
      }
      Type type = Type.from(result.getInt(2));
      if (type.isUndeleteAll()) {
        undeleted = curVersion;
        continue;
      }
      if (type.isDeleteAll()) {
        if (undeleted == curVersion) {
          continue;
        } else {
          break;
        }
      }
      if (type.isDelete()) {
        lastDelete = curVersion;
        continue;
      }
      if (curVersion == lastDelete) {
        continue;
      }
      return new ImmutablePair<Long, byte[]>(curVersion, result.getBytes(4));
    }
    return null;
  }

  /**
   * Result has (column, version, kvtype, id, value).
   *
   * @throws SQLException
   */
  private Map<byte[], byte[]> filteredLatestColumns(ResultSet result, ReadPointer readPointer, int limit)
    throws SQLException {

    // negative limit means unlimited results
    if (limit <= 0) {
      limit = Integer.MAX_VALUE;
    }

    Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    if (result == null) {
      return map;
    }
    byte[] curCol = new byte[0];
    byte[] lastCol = new byte[0];
    long lastDelete = -1;
    long undeleted = -1;
    while (result.next()) {
      long curVersion = result.getLong(2);
      // Check if this entry is visible, skip if not
      if (!readPointer.isVisible(curVersion)) {
        continue;
      }
      byte[] column = result.getBytes(1);
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
        if (undeleted == curVersion) {
          continue;
        } else {
          // The rest of this column has been deleted, act like we returned it
          lastCol = column;
          continue;
        }
      }
      if (type.isDelete()) {
        lastDelete = curVersion;
        continue;
      }
      if (curVersion == lastDelete) {
        continue;
      }
      lastCol = column;
      map.put(column, result.getBytes(5));

      // break out if limit reached
      if (map.size() >= limit) {
        break;
      }
    }
    return map;
  }

  /**
   * Result has (row, column, version, kvtype, id, value).
   *
   * @throws SQLException
   */
  private Map<byte[], Map<byte[], byte[]>> filteredLatestColumnsWithKey(ResultSet result, ReadPointer readPointer,
                                                                        int limit)
    throws SQLException {

    // negative limit means unlimited results
    if (limit <= 0) {
      limit = Integer.MAX_VALUE;
    }

    Map<byte[], Map<byte[], byte[]>> map = new TreeMap<byte[], Map<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
    if (result == null) {
      return map;
    }

    boolean newRow = true;
    byte[] lastRow = new byte[0];

    byte[] curCol = new byte[0];
    byte[] lastCol = new byte[0];
    long lastDelete = -1;
    long undeleted = -1;
    while (result.next()) {

      byte[] rowKey = result.getBytes(1);
      if (!Bytes.equals(lastRow, rowKey)) {
        newRow = true;
        lastRow = rowKey;
      }

      long curVersion = result.getLong(3);
      // Check if this entry is visible, skip if not
      if (!readPointer.isVisible(curVersion)) {
        continue;
      }
      byte[] column = result.getBytes(2);
      // Check if this column has already been included in result, skip if so
      if (!newRow && Bytes.equals(lastCol, column)) {
        continue;
      }
      // Check if this is a new column, reset delete pointers if so
      if (newRow || !Bytes.equals(curCol, column)) {
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
        if (undeleted == curVersion) {
          continue;
        } else {
          // The rest of this column has been deleted, act like we returned it
          lastCol = column;
          continue;
        }
      }
      if (type.isDelete()) {
        lastDelete = curVersion;
        continue;
      }
      if (curVersion == lastDelete) {
        continue;
      }
      lastCol = column;

      Map<byte[], byte[]> colMap = map.get(rowKey);
      if (colMap == null) {
        colMap = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
        map.put(rowKey, colMap);
      }

      colMap.put(column, result.getBytes(6));

      // break out if limit reached
      if (map.size() >= limit) {
        break;
      }
    }
    return map;
  }

  // TODO: Let out exceptions?  These are only for code bugs since we are in
  //       memory and file modes only so availability not an issue?
  private OperationException createOperationException(SQLException e, String where) {
    String msg = "HyperSQL exception on " + where + "(error code = " +
      e.getErrorCode() + ")";
    Log.error(msg, e);
    return new OperationException(StatusCode.SQL_ERROR, msg, e);
  }

  private OperationException createOperationException(SQLException e, String where, PreparedStatement ps)
    throws OperationException {
    String msg = "HyperSQL exception on " + where + "(error code = " +
      e.getErrorCode() + ") (statement = " + ps.toString() + ")";
    Log.error(msg, e);
    return new OperationException(StatusCode.SQL_ERROR, msg, e);
  }

  /**
   * Implements Scanner using a ResultSet.
   * The current implementation first reads all value from result set and stores it in memory
   * The resultSet expects the following values to be present
   *
   * - rowkey(index=1), column(index=2), version(index=3), kvtype(index=4), id(index=5), value(index=6)
   */
  public class ResultSetScanner implements  Scanner {

    private Map<byte[], Map<byte[], byte[]>> rowColumnValueMap = new TreeMap<byte[],
      Map<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
    private Iterator<Map.Entry<byte[], Map<byte[], byte[]>>> rowColumnValueIterator;

    public ResultSetScanner(ResultSet resultSet, ReadPointer readPointer) {
      populateRowColumnValueMap(resultSet, readPointer);
      rowColumnValueIterator = this.rowColumnValueMap.entrySet().iterator();
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
      if (rowColumnValueIterator.hasNext()){
        Map.Entry<byte[], Map<byte[], byte[]>> entry  = rowColumnValueIterator.next();
        return new ImmutablePair<byte[], Map<byte[], byte[]>>(entry.getKey(), entry.getValue());
      } else {
        return null;
      }
    }

    private void populateRowColumnValueMap(ResultSet result, ReadPointer readPointer) {

      try {
        if (result == null){
          return;
        }

        byte [] curCol = new byte [0];
        byte [] lastCol = new byte [0];
        byte [] curRow = new byte[0];
        long lastDelete = -1;
        long undeleted = -1;

        while (result.next()) {
          long curVersion = result.getLong(3);

          if (readPointer != null && !readPointer.isVisible(curVersion)) {
            continue;
          }

          byte[] row = result.getBytes(1);

          // See if this is a new row (clear col/del tracking if so)
          if (!Bytes.equals(curRow, row)) {
            lastCol = new byte[0];
            curCol = new byte[0];
            lastDelete = -1;
            undeleted = -1;
          }

          curRow = row;

          byte[] column = result.getBytes(2);
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
            if (undeleted == curVersion) {
              continue;
            } else {
              // The rest of this column has been deleted, act like we returned it
              lastCol = column;
              continue;
            }
          }
          if (type.isDelete()) {
            lastDelete = curVersion;
            continue;
          }
          if (curVersion == lastDelete) {
            continue;
          }
          // Column is valid, therefore row is valid, add row
          lastCol = column;

          Map<byte[], byte[]> colMap = rowColumnValueMap.get(row);
          if (colMap == null) {
            colMap = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
            rowColumnValueMap.put(row, colMap);
          }
          colMap.put(column, result.getBytes(6));
        }
     } catch (SQLException e){
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void close() {
    }
  }

}
