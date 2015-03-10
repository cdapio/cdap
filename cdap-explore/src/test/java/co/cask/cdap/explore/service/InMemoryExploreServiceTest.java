/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.service;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
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
  private static TransactionManager transactionManager;
  private static ExploreService exploreService;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;

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
        new DataSetsModules().getLocalModule(),
        new DataSetServiceModules().getInMemoryModule(),
        new MetricsClientRuntimeModule().getInMemoryModules(),
        new AuthModule(),
        new ExploreRuntimeModule().getInMemoryModules(),
        new ExploreClientModule(),
        new StreamAdminModules().getInMemoryModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            install(new FactoryModuleBuilder()
                      .implement(Store.class, DefaultStore.class)
                      .build(StoreFactory.class)
            );
          }
        },
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);
          }
        });
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();

    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreService = injector.getInstance(ExploreService.class);
    exploreService.startAndWait();
  }

  @AfterClass
  public static void stop() throws Exception {
    exploreService.stop();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
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
    QueryHandle handle = exploreService.execute(Id.Namespace.from(Constants.DEFAULT_NAMESPACE), command);

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
