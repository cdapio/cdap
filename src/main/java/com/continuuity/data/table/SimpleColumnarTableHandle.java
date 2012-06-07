package com.continuuity.data.table;

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.TTQueueTableOnVCTable;
import com.google.inject.Inject;

public abstract class SimpleColumnarTableHandle implements ColumnarTableHandle {
  
  private final ConcurrentSkipListMap<byte[], ColumnarTable> tables =
      new ConcurrentSkipListMap<byte[],ColumnarTable>(
          Bytes.BYTES_COMPARATOR);
  
  private final ConcurrentSkipListMap<byte[], TTQueueTable> queueTables =
      new ConcurrentSkipListMap<byte[],TTQueueTable>(
          Bytes.BYTES_COMPARATOR);
  
  @Inject
  private TimestampOracle timeOracle;
  
  private Object queueTableLock = new Object();
  private VersionedColumnarTable queueTable = null;
  private CConfiguration conf = new CConfiguration();
  
  @Override
  public ColumnarTable getTable(byte[] tableName) {
    ColumnarTable table = this.tables.get(tableName);
    if (table != null) return table;
    table = createNewTable(tableName, timeOracle);
    ColumnarTable existing = this.tables.putIfAbsent(tableName, table);
    return existing != null ? existing : table;
  }

  private static final byte [] queueCTable = Bytes.toBytes("__queueCTable");

  @Override
  public TTQueueTable getQueueTable(byte[] queueTableName) {
    TTQueueTable table = this.queueTables.get(queueTableName);
    if (table != null) return table;
    if (queueTable == null) {
      synchronized (queueTableLock) {
        if (queueTable == null) {
          queueTable = new MemoryOVCTable(queueCTable, timeOracle);
        }
      }
    }
    table = new TTQueueTableOnVCTable(queueTable, timeOracle, conf);
    TTQueueTable existing = this.queueTables.putIfAbsent(queueTableName, table);
    return existing != null ? existing : table;
  }

  public abstract ColumnarTable createNewTable(byte [] tableName,
      TimestampOracle timeOracle);

}
