/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStore;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Factory for creating {@link StreamConsumerStateStore} in HBase.
 */
public final class HBaseStreamConsumerStateStoreFactory implements StreamConsumerStateStoreFactory {

  private final Configuration hConf;
  private final String storeTableName;
  private final HBaseTableUtil tableUtil;
  private boolean tableCreated;

  @Inject
  HBaseStreamConsumerStateStoreFactory(Configuration hConf, DataSetAccessor dataSetAccessor, HBaseTableUtil tableUtil) {
    this.hConf = hConf;
    this.storeTableName = HBaseTableUtil.getHBaseTableName(
      dataSetAccessor.namespace(QueueConstants.STREAM_TABLE_PREFIX, DataSetAccessor.Namespace.SYSTEM) + ".state.store");
    this.tableUtil = tableUtil;
  }

  @Override
  public synchronized StreamConsumerStateStore create(StreamConfig streamConfig) throws IOException {
    byte[] tableName = Bytes.toBytes(storeTableName);

    if (!tableCreated) {
      HBaseAdmin admin = new HBaseAdmin(hConf);
      try {
        HTableDescriptor htd = new HTableDescriptor(tableName);

        HColumnDescriptor hcd = new HColumnDescriptor(QueueEntryRow.COLUMN_FAMILY);
        htd.addFamily(hcd);
        hcd.setMaxVersions(1);

        tableUtil.createTableIfNotExists(admin, tableName, htd, null,
                                         QueueConstants.MAX_CREATE_TABLE_WAIT, TimeUnit.MILLISECONDS);
        tableCreated = true;
      } finally {
        admin.close();
      }
    }

    HTable hTable = new HTable(hConf, tableName);
    hTable.setWriteBufferSize(Constants.Stream.HBASE_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    return new HBaseStreamConsumerStateStore(streamConfig, hTable);
  }

  @Override
  public synchronized void dropAll() throws IOException {
    tableCreated = false;
    HBaseAdmin admin = new HBaseAdmin(hConf);
    try {
      byte[] tableName = Bytes.toBytes(storeTableName);

      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    } finally {
      admin.close();
    }
  }
}
