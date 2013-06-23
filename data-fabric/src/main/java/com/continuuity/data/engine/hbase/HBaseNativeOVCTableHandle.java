/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.hbase;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.TTQueueTableOnHBaseNative;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.hbase.ttqueue.HBQConstants;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * This class implements the table handle for an "improved" HBase that has extensions to handle queues and
 * transactions.
 */
public class HBaseNativeOVCTableHandle extends HBaseOVCTableHandle {

  private final ConcurrentSkipListMap<byte[], HTable> htables =
    new ConcurrentSkipListMap<byte[], HTable>(Bytes.BYTES_COMPARATOR);

  @Inject
  public HBaseNativeOVCTableHandle(@Named("HBaseOVCTableHandleCConfig")CConfiguration conf,
                                   @Named("HBaseOVCTableHandleHConfig")Configuration hConf) throws IOException {
    super(conf, hConf);
  }

  @Override
  public String getName() {
    return "native";
  }

  @Override
  protected HBaseOVCTable createOVCTable(byte[] tableName) throws OperationException {
    return new HBaseNativeOVCTable(conf, hConf, tableName, FAMILY, new HBaseIOExceptionHandler());
  }

  @Override
  protected OrderedVersionedColumnarTable createNewTable(byte[] tableName) throws OperationException {
    try {
      createTable(tableName, FAMILY);
      return createOVCTable(tableName);
    } catch (IOException e) {
      exceptionHandler.handle(e);
    }
    return null;
  }

  @Override
  public TTQueueTable getQueueTable(byte[] queueTableName) throws OperationException {
    TTQueueTable queueTable = this.queueTables.get(queueTableName);
    if (queueTable != null) {
      return queueTable;
    }
    HTable table = getHTable(queueOVCTable, HBQConstants.HBQ_FAMILY);
    
    queueTable = new TTQueueTableOnHBaseNative(table, oracle, conf, hConf);
    TTQueueTable existing = this.queueTables.putIfAbsent(queueTableName, queueTable);
    return existing != null ? existing : queueTable;
  }
  
  @Override
  public TTQueueTable getStreamTable(byte[] streamTableName) throws OperationException {
    TTQueueTable streamTable = this.streamTables.get(streamTableName);
    if (streamTable != null) {
      return streamTable;
    }
    HTable table = getHTable(streamOVCTable, HBQConstants.HBQ_FAMILY);

    streamTable = new TTQueueTableOnHBaseNative(table, oracle, conf, hConf);
    TTQueueTable existing = this.streamTables.putIfAbsent(streamTableName, streamTable);
    return existing != null ? existing : streamTable;
  }

  public HTable getHTable(byte[] tableName, byte[] family) throws OperationException {
    HTable table = this.htables.get(tableName);
    if (table != null) {
      return table;
    }
    try {
      table = createTable(tableName, family);
    } catch (IOException e) {
      throw new OperationException(StatusCode.HBASE_ERROR, "Error creating table", e);
    }
    HTable existing = this.htables.putIfAbsent(tableName, table);
    return existing != null ? existing : table;
  }

}
