package com.continuuity.hbase.wd;

import com.continuuity.test.XSlowTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@Category(XSlowTests.class)
@RunWith(Suite.class)
@Suite.SuiteClasses({
  IdentityHashDistributorTestRun.class,
  MultiBytesPrefixHashDistributorTestRun.class,
  OneByteSimpleHashDistributorTestRun.class,
  RowKeyDistributorByOneBytePrefixTestRun.class
})
public class DistributorTestsSuite {

  @BeforeClass
  public static void init() throws Exception {
    RowKeyDistributorTestBase.beforeClass();
    RowKeyDistributorTestBase.runBefore = false;
    RowKeyDistributorTestBase.runAfter = false;
  }

  @AfterClass
  public static void finish() throws Exception {
    RowKeyDistributorTestBase.runAfter = true;
    RowKeyDistributorTestBase.afterClass();
  }
}
