package com.continuuity.metrics.data;

import com.continuuity.data.table.OrderedVersionedColumnarTable;

/**
 * Table for storing aggregated metrics for all time.
 */
public final class AggregatesTable {

  private final OrderedVersionedColumnarTable aggregatesTable;

  public AggregatesTable(EntityTable entityTable, OrderedVersionedColumnarTable aggregatesTable) {
    this.aggregatesTable = aggregatesTable;

  }
}
