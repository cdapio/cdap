package com.continuuity.hive;

import com.continuuity.hive.guice.InMemoryHiveModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;

/**
 *
 */
public class TestHive {

  private HiveServer embeddedHiveServer;
  private HiveCommandExecutor hiveCommandExecutor;

  @Before
  public void before() {
    Injector injector = Guice.createInjector(new InMemoryHiveModule());
    embeddedHiveServer = injector.getInstance(HiveServer.class);
    hiveCommandExecutor = injector.getInstance(HiveCommandExecutor.class);
    embeddedHiveServer.startAndWait();
  }

  @After
  public void after() {
    embeddedHiveServer.stopAndWait();
  }

  @Test
  public void testHive() throws Exception {
    URL loadFileUrl = getClass().getResource("/test_table.dat");
    Assert.assertNotNull(loadFileUrl);

    hiveCommandExecutor.sendCommand("drop table if exists test;");
    hiveCommandExecutor.sendCommand("create table test (first INT, second STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';");
    hiveCommandExecutor.sendCommand("show tables;");
    hiveCommandExecutor.sendCommand("describe test;");
    hiveCommandExecutor.sendCommand("LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() + "' INTO TABLE test;");
    hiveCommandExecutor.sendCommand("select first, second from test;");
    hiveCommandExecutor.sendCommand("drop table test;");
  }

//  @Test
//  public void testExternalTable() throws Exception {
//    runHiveCommand("drop table if exists employee");
//    runHiveCommand("create external table employee (name STRING, id int) " +
//                     "stored by 'com.continuuity.hive.datasets.DatasetStorageHandler';");
//    runHiveCommand("show tables;");
//    runHiveCommand("describe employee;");
//    runHiveCommand("select name, id from employee;");
//    runHiveCommand("select * from employee;");
//    runHiveCommand("drop table employee");
//  }
}
