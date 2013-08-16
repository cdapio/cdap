package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.TTQueueTableOnVCTable;
import com.google.inject.Inject;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Provides common implementations for some OVC TableHandle methods.
 */
public abstract class AbstractOVCTableHandle implements OVCTableHandle {

  protected final ConcurrentSkipListMap<byte[], TTQueueTable> queueTables =
      new ConcurrentSkipListMap<byte[], TTQueueTable>(
          Bytes.BYTES_COMPARATOR);

  protected final ConcurrentSkipListMap<byte[], TTQueueTable> streamTables =
    new ConcurrentSkipListMap<byte[], TTQueueTable>(Bytes.BYTES_COMPARATOR);

  /**
   * This is the timestamp generator that we will use.
   */
  @Inject
  protected TransactionOracle oracle;

  /**
   * A configuration object. Not currently used (for real).
   */
  private CConfiguration conf = new CConfiguration();

  @Override
  public abstract OrderedVersionedColumnarTable getTable(byte[] tableName) throws OperationException;

  public static final byte [] QUEUE_OVC_TABLES = Bytes.toBytes("QUEUE_OVC_TABLES");
  public static final byte [] STREAM_OVC_TABLES = Bytes.toBytes("STREAM_OVC_TABLES");

  @Override
  public TTQueueTable getQueueTable(byte[] queueTableName)
      throws OperationException {
    TTQueueTable queueTable = this.queueTables.get(queueTableName);
    if (queueTable != null) {
      return queueTable;
    }
    OrderedVersionedColumnarTable table = getTable(QUEUE_OVC_TABLES);

    queueTable = new TTQueueTableOnVCTable(table, oracle, conf);
    TTQueueTable existing = this.queueTables.putIfAbsent(
        queueTableName, queueTable);
    return existing != null ? existing : queueTable;
  }

  @Override
  public TTQueueTable getStreamTable(byte[] streamTableName)
    throws OperationException {
    TTQueueTable streamTable = this.streamTables.get(streamTableName);
    if (streamTable != null) {
      return streamTable;
    }
    OrderedVersionedColumnarTable table = getTable(STREAM_OVC_TABLES);

    streamTable = new TTQueueTableOnVCTable(table, oracle, conf);
    TTQueueTable existing = this.streamTables.putIfAbsent(
        streamTableName, streamTable);
    return existing != null ? existing : streamTable;
  }
}
