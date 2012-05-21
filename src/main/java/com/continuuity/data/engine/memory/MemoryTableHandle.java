package com.continuuity.data.engine.memory;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.SimpleQueueTable;
import com.continuuity.data.engine.SimpleTableHandle;

public class MemoryTableHandle implements SimpleTableHandle {

  private static final Map<byte[],MemoryVersionedTable> tables =
      new TreeMap<byte[],MemoryVersionedTable>(Bytes.BYTES_COMPARATOR);

  private static final Map<byte[],MemoryQueueTable> queues =
      new TreeMap<byte[],MemoryQueueTable>(Bytes.BYTES_COMPARATOR);

  @Override
  public MemoryVersionedTable getTable(byte[] tableName) {
    synchronized (tables) {
      MemoryVersionedTable table = MemoryTableHandle.tables.get(tableName);
      if (table == null) {
        table = new MemoryVersionedTable(tableName);
        MemoryTableHandle.tables.put(tableName, table);
      }
      return table;
    }
  }

  @Override
  public SimpleQueueTable getQueueTable(byte[] queueName) {
    synchronized (queues) {
      MemoryQueueTable queueTable = MemoryTableHandle.queues.get(queueName);
      if (queueTable == null) {
        queueTable = new MemoryQueueTable();
        MemoryTableHandle.queues.put(queueName, queueTable);
      }
      return queueTable;
    }
  }

}
