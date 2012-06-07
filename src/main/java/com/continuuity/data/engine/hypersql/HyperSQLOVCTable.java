package com.continuuity.data.engine.hypersql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.Scanner;


public class HyperSQLOVCTable
implements OrderedVersionedColumnarTable {

  private final String tableName;
  private final Connection connection;

  public HyperSQLOVCTable(final String tableName, Connection connection) {
    this.tableName = tableName;
    this.connection = connection;
  }

  public static final String ROW_TYPE = "VARCHAR(1024)";
  public static final String COLUMN_TYPE = "VARCHAR(1024)";
  public static final String TIMESTAMP_TYPE = "BIGINT";
  public static final String VALUE_TYPE = "VARBINARY(2)";

  public void initializeTable() {
    String createStatement = "CREATE TABLE " + this.tableName + " (" +
        "row " + ROW_TYPE + " NOT NULL, " +
        "qualifier " + COLUMN_TYPE + " NOT NULL, " +
        "timestamp " + TIMESTAMP_TYPE + " NOT NULL, " +
        "value " + VALUE_TYPE + " NOT NULL, " +
        "PRIMARY KEY (row, qualifier, timestamp, value)";

    Statement stmt = null;
    try {
      stmt = this.connection.createStatement();
      stmt.executeUpdate(createStatement);
    } catch (SQLException e) {
      // fail silent (table already exists)
    } finally {
      if (stmt != null) { try {
        stmt.close();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      } }
    }
  }

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value) {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "INSERT INTO " + this.tableName +
          " (row, qualifier, version, value) VALUES ( ? , ? , ? , ? )");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ps.setLong(3, version);
      ps.setBytes(4, value);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values) {
    for (int i=0; i<columns.length; i++) {
      put(row, columns[i], version, values[i]);
    }
  }

  @Override
  public void delete(byte[] row, byte[] column, long version) {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "DELETE FROM " + this.tableName +
          " WHERE row = ? AND qualifier = ? AND version = ?");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ps.setLong(3, version);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version) {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "DELETE FROM " + this.tableName +
          " WHERE row = ? AND qualifier = ? AND version <= ?");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ps.setLong(3, version);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void undeleteAll(byte[] row, byte[] column, long version) {
    throw new RuntimeException("Unsupported by hypersql ovc table");
  }

  @Override
  public long increment(byte[] row, byte[] column, long amount,
      ReadPointer readPointer, long writeVersion) {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "SELECT version, value FROM " + this.tableName + " WHERE " +
          "row = ? AND qualifier = ? ORDER BY version DESC");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ResultSet result = ps.executeQuery();
      long newAmount = amount;
      ImmutablePair<Long, byte[]> latest = filteredLatest(result, readPointer);
      if (latest != null) {
        newAmount += Bytes.toLong(latest.getSecond());
      }
      ps.close();
      ps = this.connection.prepareStatement(
          "INSERT INTO " + this.tableName +
          " (row, qualifier, timestamp, value) VALUES ( ? , ? , ? , ? )");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ps.setLong(3, writeVersion);
      ps.setBytes(4, Bytes.toBytes(newAmount));
      ps.executeUpdate();
      return amount;

    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public boolean compareAndSwap(byte[] row, byte[] column,
      byte[] expectedValue, byte[] newValue, ReadPointer readPointer,
      long writeVersion) {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "SELECT version, value FROM " + this.tableName + " WHERE " +
          "row = ? AND qualifier = ? ORDER BY version DESC");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ResultSet result = ps.executeQuery();
      ImmutablePair<Long, byte[]> latest = filteredLatest(result, readPointer);
      byte [] existingValue = latest == null ? null : latest.getSecond();
      ps.close(); ps = null;

      // handle invalid cases regarding non-existent values
      if (existingValue == null && expectedValue != null) return false;
      if (existingValue != null && expectedValue == null) return false;

      // if nothing existed, just write
      // TODO: this is not atomic and thus is broken?  how to make it atomic?
      if (expectedValue == null) {
        put(row, column, writeVersion, newValue);
        return true;
      }

      // check if expected == existing, fail if not
      if (!Bytes.equals(expectedValue, existingValue)) {
        return false;
      }

      // if newValue is null, just delete.
      // TODO: this can't be rolled back!
      if (newValue == null) {
        delete(row, column, latest.getFirst());
        return true;
      }

      // Perform update!
      ps = this.connection.prepareStatement(
          "INSERT INTO " + this.tableName +
          " (row, qualifier, timestamp, value) VALUES ( ? , ? , ? , ? )");
      ps.setBytes(1, row);
      ps.setBytes(2, column);
      ps.setLong(3, writeVersion);
      ps.setBytes(4, newValue);
      ps.executeUpdate();
      return true;

    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, ReadPointer readPointer) {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "SELECT qualifier, version, value FROM " + this.tableName + " WHERE " +
          "row = ? ORDER BY qualifier ASC, version DESC");
      ps.setBytes(1, row);
      ResultSet result = ps.executeQuery();
      return filteredLatestColumns(result, readPointer);
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public byte[] get(byte[] row, byte[] column, ReadPointer readPointer) {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "SELECT version, value FROM " + this.tableName + " WHERE " +
          "row = ? AND qualifier = ? ORDER BY version DESC");
      ps.setBytes(1, row);
      ResultSet result = ps.executeQuery();
      return filteredLatest(result, readPointer).getSecond();
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public ImmutablePair<byte[], Long> getWithVersion(byte[] row, byte[] column,
      ReadPointer readPointer) {
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(
          "SELECT version, value FROM " + this.tableName + " WHERE " +
          "row = ? AND qualifier = ? ORDER BY version DESC");
      ps.setBytes(1, row);
      ResultSet result = ps.executeQuery();
      ImmutablePair<Long,byte[]> latest = filteredLatest(result, readPointer);
      return new ImmutablePair<byte[],Long>(latest.getSecond(),
          latest.getFirst());
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, byte[] startColumn,
      byte[] stopColumn, ReadPointer readPointer) {
    PreparedStatement ps = null;
    try {
      String qualifierChecks = "";
      if (startColumn != null) qualifierChecks += " AND qualifier >= ?";
      if (stopColumn != null) qualifierChecks += " AND qualifier < ?";
      ps = this.connection.prepareStatement(
          "SELECT qualifier, version, value FROM " + this.tableName + " WHERE "+
              "row = ? " + qualifierChecks +
          "ORDER BY qualifier ASC, version DESC");
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
      return filteredLatestColumns(result, readPointer);
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, byte[][] columns,
      ReadPointer readPointer) {
    PreparedStatement ps = null;
    try {
      String [] qualifierChecks = new String[columns.length];
      String qualifierCheck = "qualifier = ?";
      for (int i=0; i<qualifierChecks.length; i++) {
        qualifierChecks[i] = qualifierCheck;
      }
      qualifierCheck = StringUtils.join(qualifierChecks, " OR ");

      ps = this.connection.prepareStatement(
          "SELECT qualifier, version, value FROM " + this.tableName + " WHERE "+
              "row = ? AND (" + qualifierChecks + ") " +
          "ORDER BY qualifier ASC, version DESC");
      ps.setBytes(1, row);
      int idx = 2;
      for (byte [] column : columns) {
        ps.setBytes(idx++, column);
      }
      ResultSet result = ps.executeQuery();
      return filteredLatestColumns(result, readPointer);
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    } finally {
      if (ps != null) {
        try {
          ps.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer) {
    throw new RuntimeException("Scans currently not supported");
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns,
      ReadPointer readPointer) {
    throw new RuntimeException("Scans currently not supported");
  }

  @Override
  public Scanner scan(ReadPointer readPointer) {
    throw new RuntimeException("Scans currently not supported");
  }

  private ImmutablePair<Long, byte[]> filteredLatest(
      ResultSet result, ReadPointer readPointer) throws SQLException {
    if (result == null) return new ImmutablePair<Long,byte[]>(-1L, null);
    while (result.next()) {
      long curVersion = result.getLong(1);
      if (!readPointer.isVisible(curVersion)) continue;
      return new ImmutablePair<Long, byte[]>(curVersion,
          result.getBytes(2));
    }
    return null;
  }

  /**
   * Result has (qualifier, version, value)
   * @param result
   * @param readPointer
   * @return
   * @throws SQLException
   */
  private Map<byte[], byte[]> filteredLatestColumns(ResultSet result,
      ReadPointer readPointer) throws SQLException {
    Map<byte[],byte[]> map = new TreeMap<byte[],byte[]>(Bytes.BYTES_COMPARATOR);
    if (result == null) return map;
    byte [] lastCol = new byte [0];
    while (result.next()) {
      long curVersion = result.getLong(2);
      if (!readPointer.isVisible(curVersion)) continue;
      byte [] column = result.getBytes(1);
      if (Bytes.equals(lastCol, column)) {
        continue;
      }
      lastCol = column;
      map.put(column, result.getBytes(3));
    }
    return map;
  }
}
