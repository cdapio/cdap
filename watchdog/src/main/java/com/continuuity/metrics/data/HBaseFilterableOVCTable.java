/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hbase.HBaseOVCTable;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.table.Scanner;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 *
 */
public class HBaseFilterableOVCTable extends HBaseOVCTable implements FilterableOVCTable {

  public HBaseFilterableOVCTable(CConfiguration cConf, Configuration conf, byte[] tableName, byte[] family,
                                 IOExceptionHandler exceptionHandler) throws OperationException {
    super(cConf, conf, tableName, family, exceptionHandler);
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns, ReadPointer readPointer) {
    try {
      Scan scan =  new Scan();
      if (startRow != null) {
        scan.setStartRow(startRow);
      }
      if (stopRow != null) {
        scan.setStopRow(stopRow);
      }
      if (columns != null) {
        for (int i = 0; i < columns.length; i++) {
          scan.addColumn(family, columns[i]);
        }
      }
      scan.setTimeRange(0, getMaxStamp(readPointer));
      scan.setMaxVersions();
      return new HBaseScanner(this.readTable.getScanner(scan), readPointer){};
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer, Filter filter) {
    try {
      Scan scan =  new Scan();
      if (startRow != null) {
        scan.setStartRow(startRow);
      }
      if (stopRow != null) {
        scan.setStopRow(stopRow);
      }
      if (filter != null) {
        scan.setFilter(filter);
      }
      scan.setTimeRange(0, getMaxStamp(readPointer));
      scan.setMaxVersions();
      return new HBaseScanner(this.readTable.getScanner(scan), readPointer){};
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Scanner scan(ReadPointer readPointer, Filter filter) {
    return scan(null, null, readPointer, filter);
  }

  // Copied from parent class, since it is private
  private synchronized HTable getWriteTable() throws IOException {
    HTable writeTable = this.writeTables.pollFirst();
    return writeTable == null ? new HTable(this.conf, this.tableName) : writeTable;
  }

  // Copied from parent class, since it is private
  private synchronized void returnWriteTable(HTable table) {
    this.writeTables.add(table);
  }
}
