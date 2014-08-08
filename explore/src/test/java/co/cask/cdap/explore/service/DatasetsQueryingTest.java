/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.discovery.TimeLimitEndpointStrategy;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpRequests;
import co.cask.cdap.common.http.ObjectResponse;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.executor.ExploreExecutorService;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.DatasetMeta;
import com.continuuity.tephra.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.gson.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DatasetsQueryingTest {
  private static TransactionManager transactionManager;
  private static DatasetService dsService;
  private static DatasetOpExecutor dsOpExecutor;
  private static DatasetFramework datasetFramework;
  private static ExploreExecutorService exploreExecutorService;
  private static EndpointStrategy datasetManagerEndpointStrategy;

  private static ExploreClient exploreClient;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(createInMemoryModules(cConf, new Configuration()));
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();

    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();

    dsOpExecutor = injector.getInstance(DatasetOpExecutor.class);
    dsOpExecutor.startAndWait();

    dsService = injector.getInstance(DatasetService.class);
    dsService.startAndWait();

    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    datasetManagerEndpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_MANAGER)), 1L, TimeUnit.SECONDS);

    exploreClient = injector.getInstance(ExploreClient.class);
    Assert.assertTrue(exploreClient.isServiceAvailable());

    datasetFramework = injector.getInstance(DatasetFramework.class);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    exploreClient.close();
    exploreExecutorService.stopAndWait();
    dsService.stopAndWait();
    dsOpExecutor.stopAndWait();
    transactionManager.stopAndWait();
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
      // NOTE: we want real service, but we don't need persistence
      Modules.override(new DataSetServiceModules().getLocalModule()).with(new AbstractModule() {
        @Override
        protected void configure() {
          bind(new TypeLiteral<Map<String, ? extends DatasetModule>>() {
          })
            .annotatedWith(Names.named("defaultDatasetModules"))
            .toInstance(DataSetServiceModules.INMEMORY_DATASET_MODULES);
        }
      }),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new AuthModule(),
      new ExploreRuntimeModule().getInMemoryModules(),
      new ExploreClientModule()
    );
  }

  @Test
  public void getDatasetsTest() throws Exception {
    // record scannable
    datasetFramework.addModule("keyStructValue", new KeyStructValueTableDefinition.KeyStructValueTableModule());
    datasetFramework.addInstance("keyStructValueTable", "my_table", DatasetProperties.EMPTY);

    // not record scannable
    datasetFramework.addModule("module2", new NotRecordScannableTableDefinition.NotRecordScannableTableModule());
    datasetFramework.addInstance("NotRecordScannableTableDef", "my_table_not_record_scannable",
                                 DatasetProperties.EMPTY);

    ObjectResponse<List<?>> datasets;
    HttpRequest request;
    InetSocketAddress address = datasetManagerEndpointStrategy.pick().getSocketAddress();
    URI baseURI = new URI(String.format("http://%s:%d/", address.getHostName(), address.getPort()));

    request = HttpRequest.get(baseURI.resolve("v2/data/datasets?explorable=true").toURL()).build();
    datasets = ObjectResponse.fromJsonBody(HttpRequests.execute(request),
                                           new TypeToken<List<DatasetSpecification>>() { }.getType());
    Assert.assertEquals(1, datasets.getResponseObject().size());
    Assert.assertEquals("my_table", ((DatasetSpecification) datasets.getResponseObject().get(0)).getName());

    request = HttpRequest.get(baseURI.resolve("v2/data/datasets?explorable=false").toURL()).build();
    datasets = ObjectResponse.fromJsonBody(HttpRequests.execute(request), new TypeToken<List<DatasetSpecification>>() {
    }.getType());
    Assert.assertEquals(1, datasets.getResponseObject().size());
    Assert.assertEquals("my_table_not_record_scannable",
                        ((DatasetSpecification) datasets.getResponseObject().get(0)).getName());

    request = HttpRequest.get(baseURI.resolve("v2/data/datasets?meta=true&explorable=true").toURL()).build();
    datasets = ObjectResponse.fromJsonBody(HttpRequests.execute(request), new TypeToken<List<DatasetMeta>>() {
    }.getType());
    Assert.assertEquals(1, datasets.getResponseObject().size());
    Assert.assertEquals("my_table", ((DatasetMeta) datasets.getResponseObject().get(0)).getSpec().getName());

    request = HttpRequest.get(baseURI.resolve("v2/data/datasets?meta=true&explorable=false").toURL()).build();
    datasets = ObjectResponse.fromJsonBody(HttpRequests.execute(request),
                                           new TypeToken<List<DatasetMeta>>() { }.getType());
    Assert.assertEquals(1, datasets.getResponseObject().size());
    Assert.assertEquals("my_table_not_record_scannable",
                        ((DatasetMeta) datasets.getResponseObject().get(0)).getSpec().getName());

    request = HttpRequest.get(baseURI.resolve("v2/data/datasets?meta=true").toURL()).build();
    datasets = ObjectResponse.fromJsonBody(HttpRequests.execute(request),
                                           new TypeToken<List<DatasetMeta>>() { }.getType());
    Assert.assertEquals(2, datasets.getResponseObject().size());

    datasetFramework.deleteInstance("my_table_not_record_scannable");
    datasetFramework.deleteModule("module2");
  }


}
