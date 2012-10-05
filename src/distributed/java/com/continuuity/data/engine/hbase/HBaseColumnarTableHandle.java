/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.hbase;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hbase.HBaseOVCTable.IOExceptionHandler;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.SimpleColumnarTableHandle;
import com.continuuity.data.table.converter.ColumnarOnVersionedColumnarTable;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseColumnarTableHandle extends SimpleColumnarTableHandle {
  
  private final CConfiguration conf;
  private final HBaseAdmin admin;
  
  private static final IOExceptionHandler exceptionHandler =
      new HBaseOVCTable.ToOperationExceptionHandler();

  private static final byte [] FAMILY = Bytes.toBytes("fam");

  @Inject
  public HBaseColumnarTableHandle(
      @Named("HBaseOVCTableHandleConfig")CConfiguration conf)
  throws IOException {
    this.conf = conf;
    this.admin = new HBaseAdmin(conf);
  }
  
  @Override
  public ColumnarTable createNewTable(byte[] tableName,
      TimestampOracle timeOracle) throws OperationException {
    HBaseOVCTable table = null;
    try {
      createTable(tableName);
    } catch (IOException e) {
      exceptionHandler.handle(e);
    }
    table = new HBaseOVCTable(conf, tableName, FAMILY,
          exceptionHandler);
    return new ColumnarOnVersionedColumnarTable(table, timeOracle);
  }

  private HTable createTable(byte [] tableName) throws IOException {
    if (this.admin.tableExists(tableName)) {
      return new HTable(conf, tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    htd.addFamily(hcd);
    this.admin.createTable(htd);
    return new HTable(conf, tableName);
  }
}
