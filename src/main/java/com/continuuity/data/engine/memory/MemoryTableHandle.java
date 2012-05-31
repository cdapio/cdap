package com.continuuity.data.engine.memory;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.SimpleQueueTable;
import com.continuuity.data.engine.SimpleTableHandle;

@Deprecated
public class MemoryTableHandle implements SimpleTableHandle {

  private static final Map<byte[],MemoryVersionedTable> TABLES =
      new TreeMap<byte[],MemoryVersionedTable>(Bytes.BYTES_COMPARATOR);

  private static final Map<byte[],MemoryQueueTable> QUEUES =
      new TreeMap<byte[],MemoryQueueTable>(Bytes.BYTES_COMPARATOR);

  private static final byte [] DELIMITER = new byte [] { '_' };
  
  private final byte [] scope;

  public MemoryTableHandle(byte [] scope) {
    this.scope = scope;
  }

  @Override
  public MemoryVersionedTable getTable(byte[] tableName) {
    byte [] fullName = Bytes.add(scope, DELIMITER, tableName);
    synchronized (TABLES) {
      MemoryVersionedTable table = MemoryTableHandle.TABLES.get(fullName);
      if (table == null) {
        table = new MemoryVersionedTable(fullName);
        MemoryTableHandle.TABLES.put(fullName, table);
      }
      return table;
    }
  }

  @Override
  public SimpleQueueTable getQueueTable(byte[] queueName) {
    byte [] fullName = Bytes.add(scope, DELIMITER, queueName);
    synchronized (QUEUES) {
      MemoryQueueTable queueTable = MemoryTableHandle.QUEUES.get(fullName);
      if (queueTable == null) {
        queueTable = new MemoryQueueTable();
        MemoryTableHandle.QUEUES.put(fullName, queueTable);
      }
      return queueTable;
    }
  }

}
