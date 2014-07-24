/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.explore.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.explore.guice.ExploreRuntimeModule;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.proto.ColumnDesc;
import com.continuuity.proto.QueryHandle;
import com.continuuity.proto.QueryResult;
import com.continuuity.proto.QueryStatus;
import com.continuuity.tephra.inmemory.InMemoryTransactionManager;
import com.continuuity.test.SlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(SlowTests.class)
public class InMemoryExploreServiceTest {
  private static InMemoryTransactionManager transactionManager;
  private static ExploreService exploreService;

  @BeforeClass
  public static void start() throws Exception {
    CConfiguration configuration = CConfiguration.create();
    Configuration hConf = new Configuration();
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.set(Constants.Explore.LOCAL_DATA_DIR,
                      new File(System.getProperty("java.io.tmpdir"), "hive").getAbsolutePath());

    Injector injector = Guice.createInjector(
        new ConfigModule(configuration, hConf),
        new IOModule(),
        new DiscoveryRuntimeModule().getInMemoryModules(),
        new LocationRuntimeModule().getInMemoryModules(),
        new DataFabricModules().getInMemoryModules(),
        new DataSetsModules().getInMemoryModule(),
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
        ImmutableList.<QueryResult>of());

    runCommand("create table test (first INT, second STRING) ROW FORMAT " +
               "DELIMITED FIELDS TERMINATED BY '\\t'",
        false,
        ImmutableList.<ColumnDesc>of(),
        ImmutableList.<QueryResult>of()
    );

    runCommand("show tables",
        true,
        Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
        Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList("test"))));

    runCommand("describe test",
        true,
        Lists.newArrayList(
            new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
            new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
            new ColumnDesc("comment", "STRING", 3, "from deserializer")
        ),
        Lists.newArrayList(
            new QueryResult(Lists.<Object>newArrayList("first", "int", "")),
            new QueryResult(Lists.<Object>newArrayList("second", "string", ""))
        )
    );

    runCommand("LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() +
               "' INTO TABLE test",
        false,
        ImmutableList.<ColumnDesc>of(),
        ImmutableList.<QueryResult>of()
    );

    runCommand("select first, second from test",
        true,
        Lists.newArrayList(new ColumnDesc("first", "INT", 1, null),
                           new ColumnDesc("second", "STRING", 2, null)),
        Lists.newArrayList(
            new QueryResult(Lists.<Object>newArrayList("1", "one")),
            new QueryResult(Lists.<Object>newArrayList("2", "two")),
            new QueryResult(Lists.<Object>newArrayList("3", "three")),
            new QueryResult(Lists.<Object>newArrayList("4", "four")),
            new QueryResult(Lists.<Object>newArrayList("5", "five")))
    );

    runCommand("select * from test",
        true,
        Lists.newArrayList(new ColumnDesc("test.first", "INT", 1, null),
                           new ColumnDesc("test.second", "STRING", 2, null)),
        Lists.newArrayList(
            new QueryResult(Lists.<Object>newArrayList("1", "one")),
            new QueryResult(Lists.<Object>newArrayList("2", "two")),
            new QueryResult(Lists.<Object>newArrayList("3", "three")),
            new QueryResult(Lists.<Object>newArrayList("4", "four")),
            new QueryResult(Lists.<Object>newArrayList("5", "five"))));

    runCommand("drop table if exists test",
        false,
        ImmutableList.<ColumnDesc>of(),
        ImmutableList.<QueryResult>of());
  }

  private static void runCommand(String command, boolean expectedHasResult,
                                 List<ColumnDesc> expectedColumnDescs,
                                 List<QueryResult> expectedResults) throws Exception {
    QueryHandle handle = exploreService.execute(command);

    QueryStatus status = waitForCompletionStatus(handle);
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, status.getStatus());
    Assert.assertEquals(expectedHasResult, status.hasResults());

    Assert.assertEquals(expectedColumnDescs, exploreService.getResultSchema(handle));
    Assert.assertEquals(expectedResults.toString(),
                        trimColumnValues(exploreService.nextResults(handle, 100)).toString());

    exploreService.close(handle);
  }

  private static List<QueryResult> trimColumnValues(List<QueryResult> results) {
    List<QueryResult> newResults = Lists.newArrayList();
    for (QueryResult result : results) {
      List<Object> newCols = Lists.newArrayList();
      for (Object obj : result.getColumns()) {
        if (obj instanceof String) {
          newCols.add(((String) obj).trim());
        } else {
          newCols.add(obj);
        }
      }
      newResults.add(new QueryResult(newCols));
    }
    return newResults;
  }

  private static QueryStatus waitForCompletionStatus(QueryHandle handle) throws Exception {
    QueryStatus status;
    do {
      TimeUnit.MILLISECONDS.sleep(200);
      status = exploreService.getStatus(handle);
    } while (status.getStatus() == QueryStatus.OpStatus.RUNNING ||
             status.getStatus() == QueryStatus.OpStatus.PENDING);
    return status;
  }
}
