package com.continuuity.hbase.wd;

/**
 *
 */
public class OneByteSimpleHashDistributorTest extends RowKeyDistributorTestBase {
  public OneByteSimpleHashDistributorTest() {
    super(new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(15)));
  }
}
