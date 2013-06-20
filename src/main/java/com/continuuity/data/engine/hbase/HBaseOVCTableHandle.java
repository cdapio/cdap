/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.hbase;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hbase.HBaseOVCTable.IOExceptionHandler;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.SimpleOVCTableHandle;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class implements the table handle for HBase.
 */
public class HBaseOVCTableHandle extends SimpleOVCTableHandle {

  private static final Logger Log = LoggerFactory.getLogger(HBaseOVCTableHandle.class);

  protected final CConfiguration conf;
  protected final Configuration hConf;
  protected final HBaseAdmin admin;

  protected final IOExceptionHandler exceptionHandler = new HBaseIOExceptionHandler();

  protected static final byte [] FAMILY = Bytes.toBytes("f");

  @Inject
  public HBaseOVCTableHandle(
      @Named("HBaseOVCTableHandleCConfig")CConfiguration conf,
      @Named("HBaseOVCTableHandleHConfig")Configuration hConf)
          throws IOException {
    this.conf = conf;
    this.hConf = hConf;
    this.admin = new HBaseAdmin(hConf);
  }

  protected OrderedVersionedColumnarTable createOVCTable(byte[] tableName) throws OperationException {
    return new HBaseOVCTable(conf, hConf, tableName, FAMILY, new HBaseIOExceptionHandler());
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
  protected OrderedVersionedColumnarTable openTable(byte[] tableName) throws OperationException {
    try {
      if (this.admin.tableExists(tableName)) {
        return createOVCTable(tableName);
      }
    } catch (IOException e) {
      exceptionHandler.handle(e);
    }
    return null;
  }

  protected HTable createTable(byte [] tableName, byte [] family) throws IOException {
    if (this.admin.tableExists(tableName)) {
      Log.debug("Attempt to creating table '" + tableName + "', which already exists. Opening existing table instead.");
      return new HTable(this.hConf, tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    htd.addFamily(hcd);
    try {
      Log.info("Creating table '" + new String(tableName) + "'");
      this.admin.createTable(htd);
    } catch (TableExistsException e) {
      // table may exist because someone else is creating it at the same
      // time. But it may not be available yet, and opening it might fail.
      Log.info("Creating table '" + new String(tableName) + "' failed with: "
          + e.getMessage() + ".");
      // Wait at most 2 seconds for table to materialize
      long waitAtMost = 5000;
      long giveUpTime = System.currentTimeMillis() + waitAtMost;
      boolean exists = false;
      while (!exists && System.currentTimeMillis() < giveUpTime) {
        if (this.admin.tableExists(tableName)) {
          exists = true;
        } else {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e1) {
            Log.error("Thread interrupted: " + e1.getMessage(), e1);
            break;
          }
        }
      }
      if (exists) {
        Log.info("Table '" + new String(tableName) + "' exists now. Assuming " +
            "that another process concurrently created it. ");
      } else {
        Log.error("Table '" + new String(tableName) + "' does not exist after" +
            " waiting " + waitAtMost + " ms. Giving up. ");
        throw e;
      }
    }
    return new HTable(this.hConf, tableName);
  }

  @Override
  public String getName() {
    return "hbase";
  }

  /**
   * An exception handler that wraps every HBase exception into a runtime exception.
   */
  public static class HBaseIOExceptionHandler implements IOExceptionHandler {
    @Override
    public void handle(IOException e) {
      throw new RuntimeException(e);
    }
  }
}
