package com.continuuity.explore.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.explore.client.DiscoveryExploreClient;
import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.client.ExploreClientUtil;
import com.continuuity.explore.executor.ExploreExecutorService;
import com.continuuity.explore.guice.ExploreRuntimeModule;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for tests that need explore service to be running.
 */
public class BaseHiveExploreServiceTest {
  protected static InMemoryTransactionManager transactionManager;
  protected static DatasetFramework datasetFramework;
  protected static DatasetService datasetService;
  protected static ExploreExecutorService exploreExecutorService;
  protected static ExploreClient exploreClient;
  protected static Injector injector;

  protected static void startServices(CConfiguration cConf) throws Exception {
    injector = Guice.createInjector(createInMemoryModules(cConf, new Configuration()));
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();

    datasetFramework = injector.getInstance(DatasetFramework.class);

    exploreClient = injector.getInstance(DiscoveryExploreClient.class);
    Assert.assertTrue(exploreClient.isAvailable());
  }

  @AfterClass
  public static void stopServices() throws Exception {
    exploreExecutorService.stopAndWait();
    datasetService.stopAndWait();
    transactionManager.stopAndWait();
  }

  protected static void runCommand(String command, boolean expectedHasResult,
                                 List<ColumnDesc> expectedColumnDescs, List<Result> expectedResults) throws Exception {
    Handle handle = exploreClient.execute(command);

    Status status = ExploreClientUtil.waitForCompletionStatus(exploreClient, handle, 200, TimeUnit.MILLISECONDS, 20);
    Assert.assertEquals(Status.OpStatus.FINISHED, status.getStatus());
    Assert.assertEquals(expectedHasResult, status.hasResults());

    Assert.assertEquals(expectedColumnDescs, exploreClient.getResultSchema(handle));
    Assert.assertEquals(expectedResults, trimColumnValues(exploreClient.nextResults(handle, 100)));

    exploreClient.close(handle);
  }

  protected static List<Result> trimColumnValues(List<Result> results) {
    List<Result> newResults = Lists.newArrayList();
    for (Result result : results) {
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
      newResults.add(new Result(newCols));
    }
    return newResults;
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf) {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.setBoolean(Constants.Explore.CFG_EXPLORE_ENABLED, true);
    configuration.set(Constants.Explore.CFG_LOCAL_DATA_DIR,
                      new File(System.getProperty("java.io.tmpdir"), "hive").getAbsolutePath());

    return ImmutableList.of(
      new ConfigModule(configuration, hConf),
      new IOModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataSetServiceModules().getInMemoryModule(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getInMemoryModule(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new AuthModule(),
      new ExploreRuntimeModule().getInMemoryModules()
    );
  }
}
