package com.continuuity.hive;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.hive.guice.InMemoryHiveModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.net.URL;

/**
 *
 */
public class TestHive {

  private HiveServer hiveServer;
  private HiveCommandExecutor hiveCommandExecutor;

  @Before
  public void before() {

    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Hive.SERVER_ADDRESS, "localhost");
    Configuration hConf = new Configuration();
//    hConf.addResource("mapred-site-local.xml");
//    hConf.reloadConfiguration();

    Injector injector = Guice.createInjector(new ConfigModule(conf, hConf),
                                             new InMemoryHiveModule(),
                                             new DiscoveryRuntimeModule().getInMemoryModules());
    hiveServer = injector.getInstance(HiveServer.class);
    hiveCommandExecutor = injector.getInstance(HiveCommandExecutor.class);

    // Start hive server
    hiveServer.startAndWait();
  }

  @After
  public void after() {
    // Stop hive server
    hiveServer.stopAndWait();
  }

  @Test
  public void testHive() throws Exception {
    URL loadFileUrl = getClass().getResource("/test_table.dat");
    Assert.assertNotNull(loadFileUrl);

    hiveCommandExecutor.sendCommand("drop table if exists test;");
    hiveCommandExecutor.sendCommand("create table test (first INT, second STRING) ROW FORMAT " +
                                    "DELIMITED FIELDS TERMINATED BY '\\t';");
    hiveCommandExecutor.sendCommand("show tables;");
    hiveCommandExecutor.sendCommand("describe test;");
    hiveCommandExecutor.sendCommand("LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() +
                                    "' INTO TABLE test;");
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
