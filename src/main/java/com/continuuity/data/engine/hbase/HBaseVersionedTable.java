package com.continuuity.data.engine.hbase;

import org.apache.hadoop.hbase.client.HTable;

import com.continuuity.data.table.OrderedVersionedColumnarTable;

public abstract class HBaseVersionedTable
implements OrderedVersionedColumnarTable {

  public HBaseVersionedTable(HTable table, byte [] family) {}

}
