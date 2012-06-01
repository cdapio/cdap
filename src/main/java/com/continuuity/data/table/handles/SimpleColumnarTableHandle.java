package com.continuuity.data.table.handles;

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicOracle;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.TTQueueTableOnVCTable;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.ColumnarTableHandle;
import com.continuuity.data.table.VersionedColumnarTable;
import com.continuuity.data.table.converter.ColumnarOnVersionedColumnarTable;

public class SimpleColumnarTableHandle implements ColumnarTableHandle {
  
  private final ConcurrentSkipListMap<byte[], ColumnarTable> tables =
      new ConcurrentSkipListMap<byte[],ColumnarTable>(
          Bytes.BYTES_COMPARATOR);
  
  private final ConcurrentSkipListMap<byte[], TTQueueTable> queueTables =
      new ConcurrentSkipListMap<byte[],TTQueueTable>(
          Bytes.BYTES_COMPARATOR);
  
  private Object queueTableLock = new Object();
  private VersionedColumnarTable queueTable = null;
  private TimestampOracle timeOracle = new MemoryStrictlyMonotonicOracle();
  private Configuration conf = new Configuration();
  
  @Override
  public ColumnarTable getTable(byte[] tableName) {
    ColumnarTable table = this.tables.get(tableName);
    if (table != null) return table;
    table = new ColumnarOnVersionedColumnarTable(new MemoryOVCTable(tableName),
        timeOracle);
    ColumnarTable existing = this.tables.putIfAbsent(tableName, table);
    return existing != null ? existing : table;
  }

  @Override
  public TTQueueTable getQueueTable(byte[] queueTableName) {
    TTQueueTable table = this.queueTables.get(queueTableName);
    if (table != null) return table;
    if (queueTable == null) {
      synchronized (queueTableLock) {
        if (queueTable == null) {
          queueTable = new MemoryOVCTable(Bytes.toBytes("queueTable"));
        }
      }
    }
    table = new TTQueueTableOnVCTable(queueTable, timeOracle, conf);
    TTQueueTable existing = this.queueTables.putIfAbsent(queueTableName, table);
    return existing != null ? existing : table;
  }

}
