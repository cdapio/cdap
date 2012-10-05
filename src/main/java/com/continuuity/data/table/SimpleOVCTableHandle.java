package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.Bytes;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.TTQueueTableOnVCTable;
import com.google.inject.Inject;

import java.util.concurrent.ConcurrentSkipListMap;

public abstract class SimpleOVCTableHandle implements OVCTableHandle {

  protected final ConcurrentSkipListMap<byte[], OrderedVersionedColumnarTable>
      openTables =
        new ConcurrentSkipListMap<byte[],OrderedVersionedColumnarTable>(
            Bytes.BYTES_COMPARATOR);
  
  protected final ConcurrentSkipListMap<byte[], TTQueueTable> queueTables =
      new ConcurrentSkipListMap<byte[],TTQueueTable>(
          Bytes.BYTES_COMPARATOR);
  
  protected final ConcurrentSkipListMap<byte[], TTQueueTable> streamTables =
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
  private CConfiguration conf = new CConfiguration();

  @Override
  public OrderedVersionedColumnarTable getTable(byte[] tableName)
      throws OperationException {
    OrderedVersionedColumnarTable table = this.openTables.get(tableName);

    // we currently have an open table for this name
    if (table != null) return table;

    // the table is not open, but it may exist in the data fabric
    table = openTable(tableName);

    // we successfully opened the table
    if (table != null) return table;

    // table could not be opened, try to create it
    table = createNewTable(tableName);

    // some other thread may have created/found and added it already
    OrderedVersionedColumnarTable existing =
        this.openTables.putIfAbsent(tableName, table);

    return existing != null ? existing : table;
  }

  public static final byte [] queueOVCTable = Bytes.toBytes("queueOVCTable");
  public static final byte [] streamOVCTable = Bytes.toBytes("streamOVCTable");

  @Override
  public TTQueueTable getQueueTable(byte[] queueTableName)
      throws OperationException {
    TTQueueTable queueTable = this.queueTables.get(queueTableName);
    if (queueTable != null) return queueTable;
    OrderedVersionedColumnarTable table = getTable(queueOVCTable);
    
    queueTable = new TTQueueTableOnVCTable(table, timeOracle, conf);
    TTQueueTable existing = this.queueTables.putIfAbsent(
        queueTableName, queueTable);
    return existing != null ? existing : queueTable;
  }
  
  @Override
  public TTQueueTable getStreamTable(byte[] streamTableName)
      throws OperationException {
    TTQueueTable streamTable = this.streamTables.get(streamTableName);
    if (streamTable != null) return streamTable;
    OrderedVersionedColumnarTable table = getTable(streamOVCTable);
    
    streamTable = new TTQueueTableOnVCTable(table, timeOracle, conf);
    TTQueueTable existing = this.streamTables.putIfAbsent(
        streamTableName, streamTable);
    return existing != null ? existing : streamTable;
  }

  /**
   * attempts to create the table. If the table already exists, attempt
   * to open it instead.
   * @param tableName the name of the table
   * @return the new table
   * @throws OperationException if an operation fails
   */
  public abstract OrderedVersionedColumnarTable createNewTable(
      byte [] tableName) throws OperationException;

  /**
   * attempts to open an existing table.
   * @param tableName the name of the table
   * @return the table if successful, null otherwise
   * @throws OperationException
   */
  public abstract OrderedVersionedColumnarTable openTable(
      byte [] tableName) throws OperationException;

}
