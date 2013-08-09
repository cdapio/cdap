/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.Scanner;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * An OVCTable that supports filtering for scan operations.
 *
 * This class is temporary locate in the metrics module and eventually will move into data-fabric.
 * Also it uses the Hbase Filter interface as filter, which we should have our own Filter interface
 * when we refactor this class into data-fabric.
 */
public interface FilterableOVCTable extends OrderedVersionedColumnarTable {

  /**
   * Tells if the given filter is supported by this table.
   * @param filterClass The class of filter.
   * @return {@code true} if the filter is supported, otherwise {@code false}.
   */
  public boolean isFilterSupported(Class<?> filterClass);

  /**
   * Similar to {@link #scan(byte[], byte[], ReadPointer)} but with filtering applied on rows it scans.
   */
  public Scanner scan(byte[] startRow, byte[] stopRow, ReadPointer readPointer, Filter filter);

  /**
   * Similar to {@link #scan(byte[], byte[], byte[][], com.continuuity.data.operation.executor.ReadPointer)}
   * but with filtering applied on rows it scans.
   */
  public Scanner scan(byte[] startRow, byte[] stopRow, byte[][] columns, ReadPointer readPointer, Filter filter);

  /**
   * Similar to {@link #scan(ReadPointer)} but with filtering applied on rows it scans.
   */
  public Scanner scan(ReadPointer readPointer, Filter filter);
}
