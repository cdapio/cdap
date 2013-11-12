package com.continuuity.data2.util.hbase;

/**
 * Factory for HBase version-specific {@link HBaseTableUtil} instances.
 */
public class HBaseTableUtilFactory extends HBaseVersionSpecificFactory<HBaseTableUtil> {
  @Override
  protected String getHBase94Classname() {
    return "com.continuuity.data2.util.hbase.HBase94TableUtil";
  }

  @Override
  protected String getHBase96Classname() {
    return "com.continuuity.data2.util.hbase.HBase96TableUtil";
  }
}
