package com.continuuity.data.engine.memory;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.SimpleQueueTable;
import com.continuuity.data.engine.SimpleTable;
import com.continuuity.data.engine.SimpleTableHandle;

@Deprecated
public class MemorySimpleTableHandle implements SimpleTableHandle {

  private static final Map<byte[],MemoryVersionedTable> TABLES =
      new TreeMap<byte[],MemoryVersionedTable>(Bytes.BYTES_COMPARATOR);

  private static final Map<byte[],MemoryQueueTable> QUEUES =
      new TreeMap<byte[],MemoryQueueTable>(Bytes.BYTES_COMPARATOR);

  private static final byte [] DELIMITER = new byte [] { '_' };
  
  private final byte [] scope;

  public MemorySimpleTableHandle(byte [] scope) {
    this.scope = scope;
  }

  @Override
  public SimpleTable getTable(byte[] tableName) {
    byte [] fullName = Bytes.add(scope, DELIMITER, tableName);
    synchronized (TABLES) {
      MemoryVersionedTable table = MemorySimpleTableHandle.TABLES.get(fullName);
      if (table == null) {
        table = new MemoryVersionedTable(fullName);
        MemorySimpleTableHandle.TABLES.put(fullName, table);
      }
      return table;
    }
  }

  @Override
  public SimpleQueueTable getQueueTable(byte[] queueName) {
    byte [] fullName = Bytes.add(scope, DELIMITER, queueName);
    synchronized (QUEUES) {
      MemoryQueueTable queueTable = MemorySimpleTableHandle.QUEUES.get(fullName);
      if (queueTable == null) {
        queueTable = new MemoryQueueTable();
        MemorySimpleTableHandle.QUEUES.put(fullName, queueTable);
      }
      return queueTable;
    }
  }

}
