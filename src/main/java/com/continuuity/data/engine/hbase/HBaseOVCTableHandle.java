/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.hbase;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.engine.hbase.HBaseOVCTable.IOExceptionHandler;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseOVCTableHandle extends SimpleOVCTableHandle {

  private final Configuration conf;
  private final HBaseAdmin admin;

  private static final IOExceptionHandler exceptionHandler =
      new HBaseIOExceptionHandler();

  private static final byte [] FAMILY = Bytes.toBytes("fam");

  @Inject
  public HBaseOVCTableHandle(
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
      createTable(tableName);
      table = new HBaseOVCTable(this.conf, tableName, FAMILY,
          new HBaseIOExceptionHandler());
    } catch (IOException e) {
      exceptionHandler.handle(e);
    }
    return table;
  }

  private HTable createTable(byte [] tableName) throws IOException {
    if (this.admin.tableExists(tableName)) {
      return new HTable(this.conf, tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
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
