package com.continuuity.data.engine.memory;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.VersionedQueueTable;
import com.continuuity.data.engine.VersionedTableHandle;

public class MemoryVersionedTableHandle implements VersionedTableHandle {

  private static final Map<byte[],MemoryVersionedTable> TABLES =
      new TreeMap<byte[],MemoryVersionedTable>(Bytes.BYTES_COMPARATOR);

  private static final Map<byte[],MemoryVersionedQueueTable> QUEUES =
      new TreeMap<byte[],MemoryVersionedQueueTable>(Bytes.BYTES_COMPARATOR);

  private static final byte [] DELIMITER = new byte [] { '_' };
  
  private final byte [] scope;

  public MemoryVersionedTableHandle(byte [] scope) {
    this.scope = scope;
  }

  @Override
  public MemoryVersionedTable getTable(byte[] tableName) {
    byte [] fullName = Bytes.add(scope, DELIMITER, tableName);
    synchronized (TABLES) {
      MemoryVersionedTable table = MemoryVersionedTableHandle.TABLES.get(fullName);
      if (table == null) {
        table = new MemoryVersionedTable(fullName);
        MemoryVersionedTableHandle.TABLES.put(fullName, table);
      }
      return table;
    }
  }

  @Override
  public VersionedQueueTable getQueueTable(byte[] queueName) {
    byte [] fullName = Bytes.add(scope, DELIMITER, queueName);
    synchronized (QUEUES) {
      MemoryVersionedQueueTable queueTable =
          MemoryVersionedTableHandle.QUEUES.get(fullName);
      if (queueTable == null) {
        queueTable = new MemoryVersionedQueueTable();
        MemoryVersionedTableHandle.QUEUES.put(fullName, queueTable);
      }
      return queueTable;
    }
  }

}
