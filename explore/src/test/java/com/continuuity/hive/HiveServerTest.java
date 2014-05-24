package com.continuuity.hive;

import com.continuuity.hive.client.HiveClient;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;

/**
 * Common test case for hive server.
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

    getHiveClient().sendCommand("drop table if exists test;");
    getHiveClient().sendCommand("create table test (first INT, second STRING) ROW FORMAT " +
                                "DELIMITED FIELDS TERMINATED BY '\\t';");
    getHiveClient().sendCommand("show tables;");
    getHiveClient().sendCommand("describe test;");
    getHiveClient().sendCommand("LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() +
                                "' INTO TABLE test;");
    getHiveClient().sendCommand("select first, second from test;");
    getHiveClient().sendCommand("select * from test;");
    getHiveClient().sendCommand("drop table test;");
    // todo add assertions
  }
}
