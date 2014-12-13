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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.discovery.TimeLimitEndpointStrategy;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.executor.ExploreExecutorService;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.tephra.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;

import java.io.File;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for tests that need explore service to be running.
 */
public class BaseHiveExploreServiceTest {
  // Controls for test suite for whether to run BeforeClass/AfterClass
  public static boolean runBefore = true;
  public static boolean runAfter = true;

  protected static TransactionManager transactionManager;
  protected static DatasetFramework datasetFramework;
  protected static DatasetOpExecutor dsOpService;
  protected static DatasetService datasetService;
  protected static ExploreExecutorService exploreExecutorService;
  protected static EndpointStrategy datasetManagerEndpointStrategy;
  protected static ExploreService exploreService;

  protected static ExploreClient exploreClient;

  protected static Injector injector;

  protected static void startServices() throws Exception {
    startServices(CConfiguration.create());
  }

  protected static void startServices(CConfiguration cConf) throws Exception {
    if (!runBefore) {
      return;
    }

    injector = Guice.createInjector(createInMemoryModules(cConf, new Configuration()));
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();

    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();

    datasetFramework = injector.getInstance(DatasetFramework.class);

    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    datasetManagerEndpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_MANAGER)), 1L, TimeUnit.SECONDS);

    exploreClient = injector.getInstance(ExploreClient.class);
    exploreService = injector.getInstance(ExploreService.class);
    Assert.assertTrue(exploreClient.isServiceAvailable());
  }

  @AfterClass
  public static void stopServices() throws Exception {
    if (!runAfter) {
      return;
    }

    exploreClient.close();
    exploreExecutorService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    transactionManager.stopAndWait();
  }

  protected static ExploreClient getExploreClient() {
    return exploreClient;
  }

  protected static QueryStatus waitForCompletionStatus(QueryHandle handle, long sleepTime,
                                                       TimeUnit timeUnit, int maxTries)
    throws ExploreException, HandleNotFoundException, InterruptedException, SQLException {
    QueryStatus status;
    int tries = 0;
    do {
      timeUnit.sleep(sleepTime);
      status = exploreService.getStatus(handle);

      if (++tries > maxTries) {
        break;
      }
    } while (!status.getStatus().isDone());
    return status;
  }

  protected static void runCommand(String command, boolean expectedHasResult,
                                   List<ColumnDesc> expectedColumnDescs, List<QueryResult> expectedResults)
    throws Exception {

    ListenableFuture<ExploreExecutionResult> future = exploreClient.submit(command);
    assertStatementResult(future, expectedHasResult, expectedColumnDescs, expectedResults);
  }

  protected static void assertStatementResult(ListenableFuture<ExploreExecutionResult> future,
                                              boolean expectedHasResult, List<ColumnDesc> expectedColumnDescs,
                                              List<QueryResult> expectedResults)
    throws Exception {
    ExploreExecutionResult results = future.get();

    Assert.assertEquals(expectedHasResult, results.hasNext());

    Assert.assertEquals(expectedColumnDescs, results.getResultSchema());
    Assert.assertEquals(expectedResults, trimColumnValues(results));

    results.close();
  }

  protected static List<QueryResult> trimColumnValues(Iterator<QueryResult> results) {
    int i = 0;
    List<QueryResult> newResults = Lists.newArrayList();
    // Max 100 results
    while (results.hasNext() && i < 100) {
      i++;
      QueryResult result = results.next();
      List<Object> newCols = Lists.newArrayList();
      for (Object obj : result.getColumns()) {
        if (obj instanceof String) {
          newCols.add(((String) obj).trim());
        } else if (obj instanceof Double) {
          // NOTE: this means only use 4 decimals for double and float values in test cases
          newCols.add((double) Math.round(((Double) obj).doubleValue() * 10000) / 10000);
        } else {
          newCols.add(obj);
        }
      }
      newResults.add(new QueryResult(newCols));
    }
    return newResults;
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf) {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    configuration.set(Constants.Explore.LOCAL_DATA_DIR,
                      new File(System.getProperty("java.io.tmpdir"), "hive").getAbsolutePath());

    return ImmutableList.of(
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
      new ExploreClientModule()
    );
  }
}
