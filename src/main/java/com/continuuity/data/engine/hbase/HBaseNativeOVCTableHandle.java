/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.hbase;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.engine.hbase.HBaseOVCTable.IOExceptionHandler;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.TTQueueTableOnHBaseNative;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.continuuity.hbase.ttqueue.HBQConstants;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class HBaseNativeOVCTableHandle extends SimpleOVCTableHandle {

  private final Configuration conf;
  private final HBaseAdmin admin;

  private final ConcurrentSkipListMap<byte[], HTable> htables =
      new ConcurrentSkipListMap<byte[],HTable>(Bytes.BYTES_COMPARATOR);
  
  private static final IOExceptionHandler exceptionHandler =
      new HBaseIOExceptionHandler();

  private static final byte [] FAMILY = Bytes.toBytes("fam");

  @Inject
  public HBaseNativeOVCTableHandle(
      @Named("HBaseOVCTableHandleConfig")Configuration conf)
          throws IOException {
    this.conf = conf;
    this.admin = new HBaseAdmin(conf);
  }

  @Override
  public OrderedVersionedColumnarTable createNewTable(byte[] tableName)
      throws OperationException {
    HBaseOVCTable table = null;
    try {
      createTable(tableName, FAMILY);
      table = new HBaseOVCTable(this.conf, tableName, FAMILY,
          new HBaseIOExceptionHandler());
    } catch (IOException e) {
      exceptionHandler.handle(e);
    }
    return table;
  }

  @Override
  public TTQueueTable getQueueTable(byte[] queueTableName)
      throws OperationException {
    TTQueueTable queueTable = this.queueTables.get(queueTableName);
    if (queueTable != null) return queueTable;
    HTable table = getHTable(queueOVCTable, HBQConstants.HBQ_FAMILY);
    
    queueTable = new TTQueueTableOnHBaseNative(table, timeOracle, conf);
    TTQueueTable existing = this.queueTables.putIfAbsent(
        queueTableName, queueTable);
    return existing != null ? existing : queueTable;
  }
  
  @Override
  public TTQueueTable getStreamTable(byte[] streamTableName)
      throws OperationException {
    TTQueueTable streamTable = this.streamTables.get(streamTableName);
    if (streamTable != null) return streamTable;
    HTable table = getHTable(streamOVCTable, HBQConstants.HBQ_FAMILY);
    
    streamTable = new TTQueueTableOnHBaseNative(table, timeOracle, conf);
    TTQueueTable existing = this.streamTables.putIfAbsent(
        streamTableName, streamTable);
    return existing != null ? existing : streamTable;
  }

  public HTable getHTable(byte[] tableName, byte[] family)
      throws OperationException {
    HTable table = this.htables.get(tableName);
    if (table != null) return table;
    try {
      table = createTable(tableName, family);
    } catch (IOException e) {
      throw new OperationException(StatusCode.HBASE_ERROR,
          "Error creating table", e);
    }
    HTable existing = this.htables.putIfAbsent(tableName, table);
    return existing != null ? existing : table;
  }

  private HTable createTable(byte [] tableName, byte [] family)
      throws IOException {
    if (this.admin.tableExists(tableName)) {
      return new HTable(this.conf, tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    htd.addFamily(hcd);
    this.admin.createTable(htd);
    return new HTable(this.conf, tableName);
  }

  @Override
  public String getName() {
    return "hbase";
  }

  public static class HBaseIOExceptionHandler implements IOExceptionHandler {
    @Override
    public void handle(IOException e) {
      throw new RuntimeException(e);
    }
  }
}
