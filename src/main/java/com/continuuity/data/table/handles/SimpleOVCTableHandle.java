package com.continuuity.data.table.handles;

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.TTQueueTableOnVCTable;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.google.inject.Inject;

public abstract class SimpleOVCTableHandle implements OVCTableHandle {
  
  private final ConcurrentSkipListMap<byte[], OrderedVersionedColumnarTable> tables =
      new ConcurrentSkipListMap<byte[],OrderedVersionedColumnarTable>(
          Bytes.BYTES_COMPARATOR);
  
  private final ConcurrentSkipListMap<byte[], TTQueueTable> queueTables =
      new ConcurrentSkipListMap<byte[],TTQueueTable>(
          Bytes.BYTES_COMPARATOR);

  /**
   * This is the timestamp generator that we will use
   */
  @Inject
  protected TimestampOracle timeOracle;

  /**
   * A configuration object. Not currently used (for real)
   */
  private Configuration conf = new CConfiguration();

  @Override
  public OrderedVersionedColumnarTable getTable(byte[] tableName) {
    OrderedVersionedColumnarTable table = this.tables.get(tableName);

    if (table != null) return table;
    table = createNewTable(tableName);

    OrderedVersionedColumnarTable existing =
        this.tables.putIfAbsent(tableName, table);

    return existing != null ? existing : table;
  }

  private static final byte [] queueOVCTable = Bytes.toBytes("__queueOVCTable");
  
  @Override
  public TTQueueTable getQueueTable(byte[] queueTableName) {
    TTQueueTable queueTable = this.queueTables.get(queueTableName);
    if (queueTable != null) return queueTable;
    OrderedVersionedColumnarTable table = getTable(queueOVCTable);
    
    queueTable = new TTQueueTableOnVCTable(table, timeOracle, conf);
    TTQueueTable existing = this.queueTables.putIfAbsent(
        queueTableName, queueTable);
    return existing != null ? existing : queueTable;
  }

  public abstract OrderedVersionedColumnarTable createNewTable(byte [] tableName);

}
