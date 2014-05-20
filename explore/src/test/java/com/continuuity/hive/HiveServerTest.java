package com.continuuity.hive;

import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;

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

  protected abstract HiveServer getHiveServer();

  protected abstract HiveCommandExecutor getHiveCommandExecutor();

  protected abstract InMemoryTransactionManager getTransactionManager();

  @Before
  public void before() {
    // Start hive server
    getHiveServer().startAndWait();
    getTransactionManager().startAndWait();
  }

  @After
  public void after() {
    // Stop hive server
    getHiveServer().stopAndWait();
    getTransactionManager().stopAndWait();
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
    getHiveCommandExecutor().sendCommand("drop table test;");
    // todo add assertions
  }
}
