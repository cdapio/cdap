package com.continuuity.hive;

import com.continuuity.hive.client.HiveClient;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;

/**
 * Common test performing hive connection and simple queries.
 */
public abstract class HiveServerTest {

  protected abstract HiveClient getHiveClient();

  protected abstract void startServices();

  protected abstract void stopServices();

  @Before
  public void before() {
    startServices();
  }

  @After
  public void after() {
    stopServices();
  }

  @Test
  public void testHive() throws Exception {
    URL loadFileUrl = getClass().getResource("/test_table.dat");
    Assert.assertNotNull(loadFileUrl);

    // Create the table and check that it is there
    getHiveClient().sendCommand("drop table if exists test;", null, null);
    getHiveClient().sendCommand("create table test (first INT, second STRING) ROW FORMAT " +
                                "DELIMITED FIELDS TERMINATED BY '\\t';", null, null);
    HiveClientTestUtils.assertCmdFindPattern(getHiveClient(), "show tables;", "tab_name.*test");
    HiveClientTestUtils.assertCmdFindPattern(getHiveClient(), "describe test;", "first.*int.*second.*string");

    // Load data into test table and assert the data
    getHiveClient().sendCommand("LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() +
                                "' INTO TABLE test;", null, null);
    HiveClientTestUtils.assertCmdFindPattern(getHiveClient(), "select first, second from test;",
                                             "first.*second.*1.*one.*2.*two.*3.*three.*4.*four.*5.*five");

    getHiveClient().sendCommand("drop table test;", null, null);
  }
}
