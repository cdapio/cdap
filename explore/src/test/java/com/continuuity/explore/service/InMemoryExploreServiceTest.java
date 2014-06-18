package com.continuuity.explore.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.explore.guice.ExploreRuntimeModule;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class InMemoryExploreServiceTest {
  private static InMemoryTransactionManager transactionManager;
  private static ExploreService exploreService;

  @BeforeClass
  public static void start() throws Exception {
    CConfiguration configuration = CConfiguration.create();
    Configuration hConf = new Configuration();
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.set(Constants.Explore.CFG_LOCAL_DATA_DIR,
                      new File(System.getProperty("java.io.tmpdir"), "hive").getAbsolutePath());

    Injector injector = Guice.createInjector(
        new ConfigModule(configuration, hConf),
        new IOModule(),
        new DiscoveryRuntimeModule().getInMemoryModules(),
        new LocationRuntimeModule().getInMemoryModules(),
        new DataFabricModules().getInMemoryModules(),
        new MetricsClientRuntimeModule().getInMemoryModules(),
        new AuthModule(),
        new ExploreRuntimeModule().getInMemoryModules());
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();

    exploreService = injector.getInstance(ExploreService.class);
    exploreService.startAndWait();
  }

  @AfterClass
  public static void stop() throws Exception {
    exploreService.stop();
    transactionManager.stopAndWait();
  }

  @Test
  public void testHiveIntegration() throws Exception {

    URL loadFileUrl = getClass().getResource("/test_table.dat");
    Assert.assertNotNull(loadFileUrl);


    runCommand("drop table if exists test",
        false,
        ImmutableList.<ColumnDesc>of(),
        ImmutableList.<Row>of());

    runCommand("create table test (first INT, second STRING) ROW FORMAT " +
               "DELIMITED FIELDS TERMINATED BY '\\t'",
        false,
        ImmutableList.<ColumnDesc>of(),
        ImmutableList.<Row>of()
    );

    runCommand("show tables",
        true,
        Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
        Lists.newArrayList(new Row(Lists.<Object>newArrayList("test"))));

    runCommand("describe test",
        true,
        Lists.newArrayList(
            new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
            new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
            new ColumnDesc("comment", "STRING", 3, "from deserializer")
        ),
        Lists.newArrayList(
            new Row(Lists.<Object>newArrayList("first", "int", "")),
            new Row(Lists.<Object>newArrayList("second", "string", ""))
        )
    );

    runCommand("LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() +
               "' INTO TABLE test",
        false,
        ImmutableList.<ColumnDesc>of(),
        ImmutableList.<Row>of()
    );

    runCommand("select first, second from test",
        true,
        Lists.newArrayList(new ColumnDesc("first", "INT", 1, null),
                           new ColumnDesc("second", "STRING", 2, null)),
        Lists.newArrayList(
            new Row(Lists.<Object>newArrayList("1", "one")),
            new Row(Lists.<Object>newArrayList("2", "two")),
            new Row(Lists.<Object>newArrayList("3", "three")),
            new Row(Lists.<Object>newArrayList("4", "four")),
            new Row(Lists.<Object>newArrayList("5", "five")))
    );

    runCommand("select * from test",
        true,
        Lists.newArrayList(new ColumnDesc("test.first", "INT", 1, null),
                           new ColumnDesc("test.second", "STRING", 2, null)),
        Lists.newArrayList(
            new Row(Lists.<Object>newArrayList("1", "one")),
            new Row(Lists.<Object>newArrayList("2", "two")),
            new Row(Lists.<Object>newArrayList("3", "three")),
            new Row(Lists.<Object>newArrayList("4", "four")),
            new Row(Lists.<Object>newArrayList("5", "five"))));

    runCommand("drop table if exists test",
        false,
        ImmutableList.<ColumnDesc>of(),
        ImmutableList.<Row>of());
  }

  private static void runCommand(String command, boolean expectedHasResult,
                                 List<ColumnDesc> expectedColumnDescs, List<Row> expectedRows) throws Exception {
    Handle handle = exploreService.execute(command);

    Status status = waitForCompletionStatus(handle);
    Assert.assertEquals(Status.State.FINISHED, status.getState());
    Assert.assertEquals(expectedHasResult, status.hasResults());

    Assert.assertEquals(expectedColumnDescs, exploreService.getResultSchema(handle));
    Assert.assertEquals(expectedRows.toString(), trimColumnValues(exploreService.nextResults(handle, 100)).toString());

    exploreService.close(handle);
  }

  private static List<Row> trimColumnValues(List<Row> rows) {
    List<Row> newRows = Lists.newArrayList();
    for (Row row : rows) {
      List<Object> newCols = Lists.newArrayList();
      for (Object obj : row.getColumns()) {
        if (obj instanceof String) {
          newCols.add(((String) obj).trim());
        } else {
          newCols.add(obj);
        }
      }
      newRows.add(new Row(newCols));
    }
    return newRows;
  }

  private static Status waitForCompletionStatus(Handle handle) throws Exception {
    Status status;
    do {
      TimeUnit.MILLISECONDS.sleep(200);
      status = exploreService.getStatus(handle);
    } while (status.getState() == Status.State.RUNNING || status.getState() == Status.State.PENDING);
    return status;
  }
}
