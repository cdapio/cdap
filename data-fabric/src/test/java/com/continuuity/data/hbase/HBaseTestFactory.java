package com.continuuity.data.hbase;

import com.continuuity.data2.util.hbase.HBaseVersionSpecificFactory;

/**
 * Factory class to provide instances of the correct {@link HBaseTestBase} implementation, dependent on the version
 * of HBase that is being used.
 */
public class HBaseTestFactory extends HBaseVersionSpecificFactory<HBaseTestBase> {
  @Override
  protected String getHBase94Classname() {
    return "com.continuuity.data.hbase.HBase94Test";
  }

  @Override
  protected String getHBase96Classname() {
    return "com.continuuity.data.hbase.HBase96Test";
  }
}
