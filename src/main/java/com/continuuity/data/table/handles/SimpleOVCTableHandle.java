package com.continuuity.data.table.handles;

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.TTQueueTableOnVCTable;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;

public class SimpleOVCTableHandle implements OVCTableHandle {
  
  private static final ConcurrentSkipListMap<byte[], OrderedVersionedColumnarTable> tables =
      new ConcurrentSkipListMap<byte[],OrderedVersionedColumnarTable>(
          Bytes.BYTES_COMPARATOR);
  
  private static final ConcurrentSkipListMap<byte[], TTQueueTable> queueTables =
      new ConcurrentSkipListMap<byte[],TTQueueTable>(
          Bytes.BYTES_COMPARATOR);
  
  private TimestampOracle timeOracle;
  private Configuration conf;
  
  public SimpleOVCTableHandle(TimestampOracle timeOracle, Configuration conf) {
    this.timeOracle = timeOracle;
    this.conf = conf;
  }
  
  @Override
  public OrderedVersionedColumnarTable getTable(byte[] tableName) {
    OrderedVersionedColumnarTable table = SimpleOVCTableHandle.tables.get(tableName);
    if (table != null) return table;
    table = new MemoryOVCTable(tableName);
    OrderedVersionedColumnarTable existing =
        SimpleOVCTableHandle.tables.putIfAbsent(tableName, table);
    return existing != null ? existing : table;
  }

  private static final byte [] queueOVCTable = Bytes.toBytes("__queueOVCTable");
  
  @Override
  public TTQueueTable getQueueTable(byte[] queueTableName) {
    TTQueueTable queueTable = SimpleOVCTableHandle.queueTables.get(queueTableName);
    if (queueTable != null) return queueTable;
    OrderedVersionedColumnarTable table = getTable(queueOVCTable);
    
    queueTable = new TTQueueTableOnVCTable(table, timeOracle, conf);
    TTQueueTable existing = SimpleOVCTableHandle.queueTables.putIfAbsent(
        queueTableName, queueTable);
    return existing != null ? existing : queueTable;
  }

}
