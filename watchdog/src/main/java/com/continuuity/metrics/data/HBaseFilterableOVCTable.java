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
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class HBaseFilterableOVCTable extends HBaseOVCTable implements FilterableOVCTable, TimeToLiveOVCTable {

  private final int ttl;
  private final boolean supportsFuzzyRowFilter;

  public HBaseFilterableOVCTable(CConfiguration cConf, Configuration conf,
                                 byte[] tableName, byte[] family,
                                 IOExceptionHandler exceptionHandler,
                                 int ttl, String hbaseVersion) throws OperationException {
    super(cConf, conf, tableName, family, exceptionHandler);
    this.ttl = ttl;
    // This is a hacky way to detect if FuzzyRowFilter is supported by assuming version >= 0.94.6
    Matcher matcher = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+).*").matcher(hbaseVersion);
    if (matcher.matches()) {
      if (Integer.parseInt(matcher.group(1)) > 0) {
        supportsFuzzyRowFilter = true;
      } else {
        if (Integer.parseInt(matcher.group(2)) > 94) {
          supportsFuzzyRowFilter = true;
        } else {
          supportsFuzzyRowFilter = Integer.parseInt(matcher.group(2)) == 94 && Integer.parseInt(matcher.group(3)) >= 6;
        }
      }
    } else {
      supportsFuzzyRowFilter = false;
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns, ReadPointer readPointer) {
    return scan(startRow, stopRow, columns, readPointer, null);
  }

  @Override
  public boolean isFilterSupported(Class<?> filterClass) {
    // The current logic is hacky as it assumes all filter are supported, except the FuzzyRowFilter is only
    // supported by version >= 0.94.6
    if (FuzzyRowFilter.class.equals(filterClass)) {
      return supportsFuzzyRowFilter;
    }
    return true;
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer, Filter filter) {
    return scan(startRow, stopRow, null, readPointer, filter);
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns, ReadPointer readPointer, Filter filter) {
    try {
      Scan scan =  new Scan();
      if (startRow != null) {
        scan.setStartRow(startRow);
      }
      if (stopRow != null) {
        scan.setStopRow(stopRow);
      }
      if (columns != null) {
        for (byte[] column : columns) {
          scan.addColumn(family, column);
        }
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

  @Override
  public int getTTL() {
    return ttl;
  }
}
