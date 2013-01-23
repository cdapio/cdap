package com.continuuity.data.engine.hbase;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.Scanner;
import com.google.common.collect.Lists;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HBaseOVCTable implements OrderedVersionedColumnarTable {
  private static final Logger Log = LoggerFactory.getLogger(HBaseOVCTable.class);

  static final byte DATA = (byte)0x00; // regular data
  static final byte DELETE_VERSION = (byte)0x01; // delete of a specific version
  static final byte DELETE_ALL = (byte)0x02; // delete of all versions of a cell up to specific version
  static final byte[] DELETE_VERSION_VALUE = new byte[] {DELETE_VERSION};
  static final byte[] DELETE_ALL_VALUE = new byte[] {DELETE_ALL};

  private final HTable readTable;
  private final LinkedList<HTable> writeTables;
  private final Configuration conf;
  private final byte[] tableName;
  private final byte[] family;

  private final IOExceptionHandler exceptionHandler;

  public HBaseOVCTable(Configuration conf,
                       final byte [] tableName,
                       final byte[] family,
                       IOExceptionHandler exceptionHandler)
      throws OperationException {
    try {
      this.readTable = new HTable(conf, tableName);
      this.writeTables = new LinkedList<HTable>();
      this.writeTables.add(new HTable(conf, tableName));
      this.writeTables.add(new HTable(conf, tableName));
      this.conf = conf;
      this.tableName = tableName;
      this.family = family;
      this.exceptionHandler = exceptionHandler;
    } catch (IOException e) {
      exceptionHandler.handle(e);
      throw new InternalError("this point should never be reached.");
    }
  }

  private synchronized HTable getWriteTable() throws IOException {
    HTable writeTable = this.writeTables.pollFirst();
    return writeTable == null ?
        new HTable(this.conf, this.tableName) : writeTable;
  }

  private synchronized void returnWriteTable(HTable table) {
    this.writeTables.add(table);
  }

  private byte[] prependWithTypePrefix(final byte[] value, byte typePrefix) {
    byte[] newValue = new byte[value.length+1];
    System.arraycopy(value,0,newValue,1,value.length);
    newValue[0] = typePrefix;
    return newValue;
  }

  private byte[] removeTypePrefix(final byte[] value) {
    return Arrays.copyOfRange(value,1,value.length);
  }

  @Override
  public void put(byte[] row, byte[] column, long version, byte[] value)  throws OperationException {
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      writeTable.put(new Put(row).add(this.family, column, version, prependWithTypePrefix(value, DATA)));
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) returnWriteTable(writeTable);
    }
  }
  public void logGet(byte[] row, byte[] family, byte[] column, long version, byte prefixType, byte[] value) {
    String s;
    switch (prefixType) {
      case DATA: s="DATA"; break;
      case DELETE_VERSION: s="DELETE_VERSION"; break;
      case DELETE_ALL: s="DELETE_ALL"; break;
      default: s="UNKNOWN";
    }
    Log.error("Getting row {}.{}:{}.{} with prefix type {} and value {} from HBase",new Object[]{Bytes.toString(row),Bytes.toString(family),column,version,s,value});
  }
  public void logPut(byte[] row, byte[] family, byte[] column, long version, byte prefixType, byte[] value) {
    String s;
    switch (prefixType) {
      case DATA: s="DATA"; break;
      case DELETE_VERSION: s="DELETE_VERSION"; break;
      case DELETE_ALL: s="DELETE_ALL"; break;
      default: s="UNKNOWN";
    }
    Log.error("Putting row {}.{}:{}.{} with prefix type {} and value {} in HBase",new Object[]{Bytes.toString(row),Bytes.toString(family),column,version,s,value});
  }
  @Override
  public void put(byte[] row, byte[][] columns, long version, byte[][] values) throws OperationException {
    assert (columns.length == values.length);
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
      Put put = new Put(row);
      for (int i = 0; i < columns.length; i++) {
//        System.err.println("value="+Bytes.toLong(values[i]));
//        System.err.println("column="+Bytes.toLong(columns[i]));
//        logPut(row,this.family,columns[i],version,DATA,values[i]);  // row-key.family:qualifier
        put.add(this.family, columns[i], version, prependWithTypePrefix(values[i], DATA));
      }
      writeTable.put(put);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) returnWriteTable(writeTable);
    }
  }

  @Override
  public void delete(byte[] row, byte[] column, long version) throws OperationException {
    HTable writeTable = null;
    //point delete (unlike deleteAll) is only used internally for undoing operations, like write(?) or delete
    //by deleting deletes!
    //if delete happens to be in the same transacation
    try {
      writeTable = getWriteTable();
//      Delete delete = new Delete(row);
//      delete.deleteColumn(this.family, column, version);
//      writeTable.delete(delete);
      Put delPut = new Put(row);
        //adding tombstone and version to value of cell, even though we are not using version
//        delPut.add(this.family, column, version, prependWithTypePrefix(Bytes.toBytes(version), DELETE_VERSION));
      delPut.add(this.family, column, version, DELETE_VERSION_VALUE);
      writeTable.put(delPut);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) returnWriteTable(writeTable);
    }
  }

  @Override
  public void delete(byte[] row, byte[][] columns, long version) throws OperationException {
    HTable writeTable = null;
    //point delete (unlike deleteAll) is only used internally for undoing operations, like write(?) or delete
    //by deleting deletes!
    try {
      writeTable = getWriteTable();
//      Delete delete = new Delete(row);
      Put delPut = new Put(row);
      //adding tombstone and version to value of cell, even though we are not using version
      //byte[] delValue = prependWithTypePrefix(Bytes.toBytes(version), DELETE_VERSION);
      for (byte [] column : columns)
//        delete.deleteColumn(this.family, column, version);
        //delPut.add(this.family, column, version, delValue);
        delPut.add(this.family, column, version, DELETE_VERSION_VALUE);
      //writeTable.delete(delete);
      writeTable.put(delPut);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) returnWriteTable(writeTable);
    }
  }

  @Override
  public void deleteAll(byte[] row, byte[] column, long version) throws OperationException {
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
//      Delete delete = new Delete(row);
//      delete.deleteColumns(this.family, column, version);
//      writeTable.delete(delete);
      Put delPut = new Put(row);
      delPut.add(this.family, column, version, DELETE_ALL_VALUE);
      writeTable.put(delPut);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) returnWriteTable(writeTable);
    }
  }

  @Override
  public void deleteAll(byte[] row, byte[][] columns, long version) throws OperationException {
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
//      Delete delete = new Delete(row);
      Put delPut = new Put(row);
      for (byte [] column : columns)
//        delete.deleteColumns(this.family, column, version);
        delPut.add(this.family, column, version, DELETE_ALL_VALUE);
//      writeTable.delete(delete);
      writeTable.put(delPut);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) returnWriteTable(writeTable);
    }
  }

  @Override
  public void undeleteAll(byte[] row, byte[] column, long version) throws OperationException {
    HTable writeTable = null;
    try {
      writeTable = getWriteTable();
//      Delete delete = new Delete(row);
//      delete.undeleteColumns(this.family, column, version);
//      writeTable.delete(delete);
      Put undelPut = new Put(row);
      //undelPut.add(this.family, column, version, prependWithTypePrefix(Bytes.toBytes(version), DELETE_VERSION));
      undelPut.add(this.family, column, version, DELETE_VERSION_VALUE);
      writeTable.put(undelPut);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) returnWriteTable(writeTable);
    }
  }

  @Override
  public void undeleteAll(byte[] row, byte[][] columns, long version) throws OperationException {
    HTable writeTable = null;
    //
    try {
      writeTable = getWriteTable();
//      Delete delete = new Delete(row);
      Put undelPut = new Put(row);
      byte[] undelValue = prependWithTypePrefix(Bytes.toBytes(version), DELETE_VERSION);
      for (byte [] column : columns)
//        delete.undeleteColumns(this.family, column, version);
        //undelPut.add(this.family, column, version, undelValue);
        undelPut.add(this.family, column, version, DELETE_VERSION_VALUE);
//      writeTable.delete(delete);
      writeTable.put(undelPut);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    } finally {
      if (writeTable != null) returnWriteTable(writeTable);
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, ReadPointer readPointer) throws OperationException {
    Set<Long> deleted = Sets.newHashSet();
    try {
      Get get = new Get(row);
      get.addFamily(this.family);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      boolean fastForwardDueToDeleteAll=false;
      byte[] previousColumn=null;
      //assumption: result.raw() has elements sorted by column (all cells from same column before next column)
      for (KeyValue kv : result.raw()) {
        byte [] column = kv.getQualifier();
        if (Bytes.equals(previousColumn,column) && fastForwardDueToDeleteAll) {
          continue;
        }
        if (!Bytes.equals(previousColumn,column)) {
          deleted.clear();
        }
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) continue;
        if (deleted.contains(version))  {
          deleted.remove(version);
          continue;
        }
        byte [] value = kv.getValue();
        byte typePrefix=value[0];
        if (typePrefix==DATA) {
          byte[] trueValue=removeTypePrefix(value);
          logGet(row,this.family,column,version,typePrefix,trueValue);
          map.put(column, trueValue);
          deleted.clear(); // necessary?
        }
        if (typePrefix==DELETE_ALL) {
          fastForwardDueToDeleteAll=true;
          deleted.clear();
        }
        if (typePrefix==DELETE_VERSION) {
          //deleted.add(Bytes.toLong(removeTypePrefix(value)));
          deleted.add(version);
        }
        previousColumn=column;
      }
      return new OperationResult<Map<byte[], byte[]>>(map);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<Map<byte[], byte[]>>(
        StatusCode.COLUMN_NOT_FOUND);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[][] columns, ReadPointer readPointer)
    throws OperationException {
    Set<Long> deleted = Sets.newHashSet();
    try {
      Get get = new Get(row);
      for (byte [] column : columns) get.addColumn(this.family, column);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      byte[] last = null;
      for (KeyValue kv : result.raw()) {
        byte [] column = kv.getQualifier();
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) continue;
        if (deleted.contains(version))  {
          deleted.remove(version);
          continue;
        }
        if (Bytes.equals(last, column)) continue;
        byte [] value = kv.getValue();
        byte typePrefix=value[0];
        if (typePrefix==DATA) {
          map.put(column, removeTypePrefix(value));
          deleted.clear();
          last=column;
        }
        if (typePrefix==DELETE_ALL) {
          deleted.clear();
          last=column;
        }
        if (typePrefix==DELETE_VERSION) {
          //deleted.add(Bytes.toLong(removeTypePrefix(value)));
          deleted.add(version);
        }
      }
      if (map.isEmpty()) {
        return new
          OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(map);
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<Map<byte[], byte[]>>(
      StatusCode.COLUMN_NOT_FOUND);
  }

  @Override
  public OperationResult<byte[]> get(byte[] row, byte[] column, ReadPointer readPointer) throws OperationException {
    Set<Long> deleted = Sets.newHashSet();
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) continue;
        if (deleted.contains(version)) continue;
        byte [] value = kv.getValue();
        if (value == null || value.length == 0)
          return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
        byte typePrefix=value[0];
        switch (typePrefix) {
          case DATA:
            return new OperationResult<byte[]>(removeTypePrefix(value));
          case DELETE_VERSION:
            // the version (timestamp) of the deleted data is in the value, behind the DELETE_VERSION prefix
            //deleted.add(Bytes.toLong(removeTypePrefix(value)));
            deleted.add(version);
            break;
          case DELETE_ALL:
            //return null;
            //return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
            return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
        }
        //return null;
        //return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
        //return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
      }
      //return null;
      //return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
      return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<byte[]>(StatusCode.COLUMN_NOT_FOUND);
  }

  private long getMaxStamp(ReadPointer readPointer) {
    return readPointer.getMaximum() == Long.MAX_VALUE ? readPointer.getMaximum() : readPointer.getMaximum() + 1;
  }

  @Override
  public OperationResult<ImmutablePair<byte[], Long>> getWithVersion(byte[] row, byte[] column, ReadPointer readPointer)
      throws OperationException {
    Set<Long> deleted = new HashSet<Long>();
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) continue;
        if (deleted.contains(version)) continue;
        byte [] value = kv.getValue();
        byte typePrefix=value[0];
        switch (typePrefix) {
          case DATA:
            byte[] trueValue=removeTypePrefix(value);
            return new OperationResult<ImmutablePair<byte[], Long>>(new ImmutablePair<byte[], Long>(trueValue, version));
          case DELETE_VERSION:
            // the version (timestamp) of the deleted data is in the value, behind the DELETE_VERSION prefix
            //deleted.add(Bytes.toLong(removeTypePrefix(value)));
            deleted.add(version);
            break;
          case DELETE_ALL:
            return null;
          //return new OperationResult<byte[]>(null);
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<ImmutablePair<byte[], Long>>(
        StatusCode.COLUMN_NOT_FOUND);
  }
  @Override
  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit,
                                                  ReadPointer readPointer) throws OperationException {
    // limit, startColumn and stopColumn refer to number of columns, not values!
    boolean done=false;

    try {
      // prepare a get for hbase
      Get get = new Get(row);
      get.addFamily(this.family);
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();

      // negative limit means unlimited, map that to int.max
      if (limit <= 0) limit = Integer.MAX_VALUE;
      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      int currentLimit = limit;
      byte[] currentStartColumn=startColumn;
      byte[] currentLatestColumn=null;
      int currentResultSize;
      while (!done) {
        // push down the column range and the limit into the get as a filter
        List<Filter> filters = Lists.newArrayList();
        if (currentStartColumn != null || stopColumn != null)
          filters.add(new ColumnRangeFilter(currentStartColumn, true, stopColumn, false));
        if (currentLimit != Integer.MAX_VALUE)
          filters.add(new ColumnPaginationFilter(currentLimit, 0));
        if (filters.size() > 1)
          get.setFilter(new FilterList(filters));
        else if (filters.size() == 1)
          get.setFilter(filters.get(0));

        Result result = this.readTable.get(get);
        currentResultSize = result.size();
        byte[] previousColumn = null;

        Set<Long> currentDeleted = new HashSet<Long>();
        for (KeyValue kv : result.raw()) {
          byte [] column = kv.getQualifier();
          currentLatestColumn=column;
          // filter out versions that are invisible under current ReadPointer
          long version = kv.getTimestamp();
          if (!readPointer.isVisible(version))  {
            continue;
          }
          // make sure that we skip repeated occurrences of the same column -
          // they would be older revisions that overwrite the most recent one
          // in the result map!
          if (currentDeleted.contains(version))  {
            currentDeleted.remove(version);
            continue;
          }
          if (Bytes.equals(previousColumn, column)) continue;
          byte [] value = kv.getValue();
          byte typePrefix=value[0];
          if (typePrefix==DATA) {
            map.put(column, removeTypePrefix(value));
            currentDeleted.clear();
            previousColumn=column;
          }
          if (typePrefix==DELETE_ALL) {
            currentDeleted.clear();
            previousColumn=column;
          }
          if (typePrefix==DELETE_VERSION) {
            //currentDeleted.add(Bytes.toLong(removeTypePrefix(value)));
            currentDeleted.add(version);
          }

          // add to the result
          // and remember this column to be able to filter out older revisions
          // of the same column (which would follow next in the hbase result)
          previousColumn = column;
        }
        if (limit!=Integer.MAX_VALUE && map.size() < limit && currentResultSize==currentLimit) {
          done=false;
          currentLimit=limit-map.size();
          currentStartColumn=Bytes.incrementBytes(currentLatestColumn,1);
        }
        else done=true;

      }
      if (map.isEmpty()) {
        return new OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
      } else {
        return new OperationResult<Map<byte[], byte[]>>(map);
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    // as fall-back return "not found".
    return new OperationResult<Map<byte[], byte[]>>(
      StatusCode.COLUMN_NOT_FOUND);
  }

  //old method, please remove after review
//  @Override
//  public OperationResult<Map<byte[], byte[]>> get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit,
//      ReadPointer readPointer) throws OperationException {
//    // limit, startColumn and stopColumn refer to number of columns, not values!
//    try {
//      // prepare a get for hbase
//      Get get = new Get(row);
//      get.addFamily(this.family);
//      get.setTimeRange(0, getMaxStamp(readPointer));
//      get.setMaxVersions();
//
//      // negative limit means unlimited, map that to int.max
//      if (limit <= 0) limit = Integer.MAX_VALUE;
//
//      // push down the column range and the limit into the get as a filter
//      List<Filter> filters = Lists.newArrayList();
//      if (startColumn != null || stopColumn != null)
//        filters.add(new ColumnRangeFilter(startColumn, true, stopColumn, false));
//      if (limit != Integer.MAX_VALUE)
//        filters.add(new ColumnPaginationFilter(limit, 0));
//      if (filters.size() > 1)
//        get.setFilter(new FilterList(filters));
//      else if (filters.size() == 1)
//        get.setFilter(filters.get(0));
//
//      Result result = this.readTable.get(get);
//      Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
//      byte[] last = null;
//      for (KeyValue kv : result.raw()) {
//        // filter out versions that are invisible under current ReadPointer
//        long version = kv.getTimestamp();
//        if (!readPointer.isVisible(version)) continue;
//        // make sure that we skip repeated occurrences of the same column -
//        // they would be older revisions that overwrite the most recent one
//        // in the result map!
//        byte [] column = kv.getQualifier();
//        if (Bytes.equals(last, column)) continue;
//        // add to the result
//        map.put(kv.getQualifier(), kv.getValue());
//        // and remember this column to be able to filter out older revisions
//        // of the same column (which would follow next in the hbase result)
//        last = column;
//      }
//      if (map.isEmpty()) {
//        return new
//            OperationResult<Map<byte[], byte[]>>(StatusCode.COLUMN_NOT_FOUND);
//      } else {
//        return new OperationResult<Map<byte[], byte[]>>(map);
//      }
//    } catch (IOException e) {
//      this.exceptionHandler.handle(e);
//    }
//    // as fall-back return "not found".
//    return new OperationResult<Map<byte[], byte[]>>(
//        StatusCode.COLUMN_NOT_FOUND);
//  }

  @Override
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer) throws OperationException {
    //invisible (due to readPointer) and deleted cells (due to DELETED_VERSION or DELETED_ALL)
    // do not count towards limit and offset
    // negative limit means unlimited, map that to int.max
    if (limit <= 0) limit = Integer.MAX_VALUE;
    List<byte[]> keys = new ArrayList<byte[]>(limit > 1024 ? 1024 : limit);
    int returned = 0;
    int skipped = 0;
    Set<Long> deleted = Sets.newHashSet();
    boolean fastForwardDueToDeleteAll=false;
    byte[] previousColumn=null;
    try {
      Scan scan = new Scan();
      scan.setTimeRange(0, getMaxStamp(readPointer));
      scan.setMaxVersions();
      ResultScanner scanner = this.readTable.getScanner(scan);
      Result result;
      while ((result = scanner.next()) != null) {
        for (KeyValue kv : result.raw()) {
          byte[] column=kv.getQualifier();
          if (Bytes.equals(previousColumn,column) && fastForwardDueToDeleteAll) {
            continue;
          }
          long version=kv.getTimestamp();
          if (!readPointer.isVisible(version)) continue;
          if (deleted.contains(version))  {
            deleted.remove(version);  // necessary?
            continue;
          }
          byte [] value = kv.getValue();
          byte typePrefix=value[0];
          if (typePrefix==DATA) {
            if (skipped < offset) {
              skipped++;
            } else if (returned < limit) {
              returned++;
              keys.add(kv.getRow());
            }
            if (returned == limit) return keys;
          }
          if (typePrefix==DELETE_ALL) {
            fastForwardDueToDeleteAll=true;
            deleted.clear();
          }
          if (typePrefix==DELETE_VERSION) {
            //deleted.add(Bytes.toLong(removeTypePrefix(value)));
            deleted.add(version);
          }
          previousColumn=column;
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    return keys;
  }

  @Override
  public long increment(byte[] row, byte[] column, long amount, ReadPointer readPointer, long writeVersion)
    throws OperationException {
      try {
        KeyValue kv = getLatestVisible(row, column, readPointer);
        long l=amount;
        if (kv!=null) {
          l+=Bytes.toLong(removeTypePrefix(kv.getValue()));
        }
        this.readTable.put(new Put(row).add(this.family, column, writeVersion, prependWithTypePrefix(Bytes.toBytes(l), DATA)));
        Log.error("Incremented value of row {}.{}:{} by {} to {} ",new Object[]{Bytes.toString(row),Bytes.toString(this.family),Bytes.toString(column),amount,l});
        return l;
      } catch (IOException e) {
        this.exceptionHandler.handle(e);
        return -1L;
      }
  }

  @Override
  public Map<byte[], Long> increment(byte[] row, byte[][] columns, long[] amounts, ReadPointer readPointer,
                                     long writeVersion) throws OperationException {
    Map<byte[],Long> ret = new TreeMap<byte[],Long>(Bytes.BYTES_COMPARATOR);
// old code, please remove after review
//    try {
//      Increment increment = new Increment(row);
//      increment.setTimeRange(0, getMaxStamp(readPointer));
//      increment.setWriteVersion(writeVersion);
//      for (int i=0; i<columns.length; i++) {
//        tryGetForFun(row, columns[i], amounts[i], readPointer);
//
//        increment.addColumn(this.family, columns[i], amounts[i]);
//      }
//      Result result = this.readTable.increment(increment);
//      for (KeyValue kv : result.raw()) {
//        ret.put(kv.getQualifier(), Bytes.toLong(kv.getValue()));
//        System.err.println("incremented result.value for "+Bytes.toString(row)+":"+Bytes.toString(kv.getQualifier())+" is "+kv.getValue());
//        System.err.println("incremented result.value for "+Bytes.toString(row)+":"+Bytes.toString(kv.getQualifier())+" as long is "+Bytes.toLong(kv.getValue()));
//      }
//      return ret;
//    } catch (IOException e) {
//      this.exceptionHandler.handle(e);
//      return ret;
//    }
    List<Put> puts = new ArrayList<Put>(columns.length);
    try {
      for (int i=0; i<columns.length; i++) {
        KeyValue kv = getLatestVisible(row, columns[i], readPointer);
        long l=amounts[i];
        if (kv!=null)
          l+=Bytes.toLong(removeTypePrefix(kv.getValue()));
        Put put = new Put(row);
        put.add(this.family, columns[i], writeVersion, prependWithTypePrefix(Bytes.toBytes(l), DATA));
        puts.add(put);
        ret.put(columns[i], l);  // (A)
      }
      this.readTable.put(puts);
// or maybe (B)
//      for (int i=0; i<columns.length; i++) {
//        KeyValue kv = getLatestVisible(row, columns[i], readPointer);
//        if (kv==null)
//          ret.put(columns[i], -1L);
//        else {
//          long l=Bytes.toLong(kv.getValue());
//          byte[] qual=kv.getQualifier();
//          ret.put(qual,l);
//        }
//      }
      return ret;
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
      ret = new TreeMap<byte[],Long>(Bytes.BYTES_COMPARATOR);
      return ret;
    }
  }

//  private KeyValue getLatestVisible(final byte [] row, final byte [] qualifier, ReadPointer readPointer)
//    throws OperationException {
//      try {
//        Get get = new Get(row);
//        get.addColumn(this.family, qualifier);
//        // read rows that were written up until the start of the current transaction (=getMaxStamp(readPointer))
//        get.setTimeRange(0, getMaxStamp(readPointer));
//        get.setMaxVersions();
//        Result result = this.readTable.get(get);
//        for (KeyValue kv : result.raw()) {
//          if (readPointer.isVisible(kv.getTimestamp())) {
//            return kv;
//          }
//        }
//      } catch (IOException e) {
//        this.exceptionHandler.handle(e);
//      }
//    return null;
//  }

  private KeyValue getLatestVisible(final byte [] row, final byte [] qualifier, ReadPointer readPointer)
    throws OperationException {
    Set<Long> deleted = Sets.newHashSet();
    try {
      Get get = new Get(row);
      get.addColumn(this.family, qualifier);
      // read rows that were written up until the start of the current transaction (=getMaxStamp(readPointer))
      get.setTimeRange(0, getMaxStamp(readPointer));
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        if (!readPointer.isVisible(version)) continue;
        if (deleted.contains(version)) continue;
        byte [] value = kv.getValue();
        byte typePrefix=value[0];
        switch (typePrefix) {
          case DATA:
            return kv;
          case DELETE_VERSION:
            // the version (timestamp) of the deleted data is in the value, behind the DELETE_VERSION prefix
            //deleted.add(Bytes.toLong(removeTypePrefix(value)));
            deleted.add(version);
            break;
          case DELETE_ALL:
            return null;
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
    return null;
  }

  private boolean equalValues(KeyValue keyValue, byte[] value) {
    if ( (value==null && keyValue==null)
      //next line correct?
      || (value==null && keyValue!=null && keyValue.getValue()==null)
      || (value!=null && keyValue!=null && Bytes.equals(removeTypePrefix(keyValue.getValue()), value))
      ) {
      return true;
    }
    return false;
  }

  @Override
  public void compareAndSwap(byte[] row, byte[] column,
                             byte[] expectedValue, byte[] newValue,
                             ReadPointer readPointer,
                             long writeVersion) throws OperationException {
//    old code, please remove after code review
//    try {
//      if (newValue == null) {
//        Delete delete = new Delete(row);
//        delete.deleteColumns(this.family, column, writeVersion);
//        if (!this.readTable.checkAndDelete(row, this.family, column,
//            expectedValue, readPointer.getMaximum(), delete)) {
//          throw new OperationException(StatusCode.WRITE_CONFLICT,
//              "CompareAndSwap expected value mismatch");
//        }
//      } else {
//        Put put = new Put(row);
//        put.add(this.family, column, writeVersion, newValue);
//        if (!this.readTable.checkAndPut(row, this.family, column,
//            expectedValue, readPointer.getMaximum(), put)) {
//          throw new OperationException(StatusCode.WRITE_CONFLICT,
//              "CompareAndSwap expected value mismatch");
//        }
//      }
//    } catch (IOException e) {
//      this.exceptionHandler.handle(e);
//    }
    try {
      if (newValue == null) {
        if (equalValues(getLatestVisible(row,column,readPointer), expectedValue)) {
//          Delete delete = new Delete(row);
//          delete.deleteColumns(this.family, column, writeVersion);
//          this.readTable.delete(delete);
          this.readTable.put(new Put(row).add(this.family, column, writeVersion, DELETE_ALL_VALUE));
        } else {
          throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
        }
      } else {
        if (equalValues(getLatestVisible(row,column,readPointer), expectedValue)) {
//          Put put = new Put(row);
//          put.add(this.family, column, writeVersion, newValue);
//          this.readTable.put(put);
          this.readTable.put(new Put(row).add(this.family, column, writeVersion, prependWithTypePrefix(newValue, DATA)));
        } else {
          throw new OperationException(StatusCode.WRITE_CONFLICT, "CompareAndSwap expected value mismatch");
        }
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }

  @Override
  public void clear() throws OperationException {
    try {
      HBaseAdmin hba = new HBaseAdmin(conf);
      HTableDescriptor htd = hba.getTableDescriptor(tableName);
      hba.disableTable(tableName);
      hba.deleteTable(tableName);
      hba.createTable(htd);
    } catch(IOException ioe) {
      this.exceptionHandler.handle(ioe);
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow,
      ReadPointer readPointer) {
    throw new UnsupportedOperationException("Scans currently not supported");
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow,
      byte[][] columns, ReadPointer readPointer) {
    throw new UnsupportedOperationException("Scans currently not supported");
  }

  @Override
  public Scanner scan(ReadPointer readPointer) {
    throw new UnsupportedOperationException("Scans currently not supported");
  }

  public static interface IOExceptionHandler {
    public void handle(IOException e) throws OperationException;
  }

  public static class ToOperationExceptionHandler implements
  IOExceptionHandler {
    @Override
    public void handle(IOException e) throws OperationException {
      String msg = "HBase IO exception: " + e.getMessage();
      Log.error(msg, e);
      throw new OperationException(StatusCode.HBASE_ERROR, msg, e);
    }
  }
  public void dumpColumn(byte[] row, byte[] column) throws OperationException {
    try {
      Get get = new Get(row);
      get.addColumn(this.family, column);
      get.setTimeRange(0, Long.MAX_VALUE);
      get.setMaxVersions();
      Result result = this.readTable.get(get);
      for (KeyValue kv : result.raw()) {
        long version = kv.getTimestamp();
        byte [] value = kv.getValue();
        if (value == null || value.length == 0) Log.error("value == null || value.length");
        Log.error("{}.{}:{}.{} -> {}",new Object[]{Bytes.toString(row),Bytes.toString(family),column,version,value});
      }
    } catch (IOException e) {
      this.exceptionHandler.handle(e);
    }
  }
}
