package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.TTQueueTableNewOnVCTable;
import com.google.inject.Inject;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.ConcurrentSkipListMap;

public abstract class AbstractOVCTableHandle implements OVCTableHandle {

  protected final ConcurrentSkipListMap<byte[], TTQueueTable> queueTables =
      new ConcurrentSkipListMap<byte[],TTQueueTable>(
          Bytes.BYTES_COMPARATOR);

  protected final ConcurrentSkipListMap<byte[], TTQueueTable> streamTables =
    new ConcurrentSkipListMap<byte[],TTQueueTable>(Bytes.BYTES_COMPARATOR);

  /**
   * This is the timestamp generator that we will use
   */
  @Inject
  protected TransactionOracle oracle;

  /**
   * A configuration object. Not currently used (for real)
   */
  private CConfiguration conf = new CConfiguration();

  @Override
  public abstract OrderedVersionedColumnarTable getTable(byte[] tableName) throws OperationException;

  public static final byte [] queueOVCTable = Bytes.toBytes("queueOVCTable");
  public static final byte [] streamOVCTable = Bytes.toBytes("streamOVCTable");

  @Override
  public TTQueueTable getQueueTable(byte[] queueTableName)
      throws OperationException {
    TTQueueTable queueTable = this.queueTables.get(queueTableName);
    if (queueTable != null) return queueTable;
    OrderedVersionedColumnarTable table = getTable(queueOVCTable);

    // queueTable = new TTQueueTableOnVCTable(table, oracle, conf);
    queueTable = new TTQueueTableNewOnVCTable(table, oracle, conf);
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

    // streamTable = new TTQueueTableOnVCTable(table, oracle, conf);
    streamTable = new TTQueueTableNewOnVCTable(table, oracle, conf);
    TTQueueTable existing = this.streamTables.putIfAbsent(
        streamTableName, streamTable);
    return existing != null ? existing : streamTable;
  }
}
