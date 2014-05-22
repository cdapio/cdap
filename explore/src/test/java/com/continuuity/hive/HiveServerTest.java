package com.continuuity.hive;

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

  protected abstract HiveCommandExecutor getHiveCommandExecutor();

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

    getHiveCommandExecutor().sendCommand("drop table if exists test;");
    getHiveCommandExecutor().sendCommand("create table test (first INT, second STRING) ROW FORMAT " +
                                         "DELIMITED FIELDS TERMINATED BY '\\t';");
    getHiveCommandExecutor().sendCommand("show tables;");
    getHiveCommandExecutor().sendCommand("describe test;");
    getHiveCommandExecutor().sendCommand("LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() +
                                         "' INTO TABLE test;");
    getHiveCommandExecutor().sendCommand("select first, second from test;");
    getHiveCommandExecutor().sendCommand("select * from test;");
    getHiveCommandExecutor().sendCommand("drop table test;");
    // todo add assertions
  }
}
