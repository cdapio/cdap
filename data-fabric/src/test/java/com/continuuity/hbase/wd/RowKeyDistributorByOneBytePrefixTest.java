package com.continuuity.hbase.wd;

/**
 *
 */
public class RowKeyDistributorByOneBytePrefixTest extends RowKeyDistributorTestBase {
  public RowKeyDistributorByOneBytePrefixTest() {
    super(new RowKeyDistributorByOneBytePrefix((byte) 12));
  }
}
