/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.route.store;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.InMemoryNamespaceClient;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Route Configuration Store Test.
 */
public class LocalRouteStoreTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static TransactionManager txManager;
  private static DatasetFramework datasetFramework;

  @BeforeClass
  public static void beforeClass() throws DatasetManagementException, IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    txManager = new TransactionManager(new Configuration());
    txManager.startAndWait();

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          bind(DatasetFramework.class).to(InMemoryDatasetFramework.class);

          bind(NamespaceQueryAdmin.class).to(InMemoryNamespaceClient.class).in(Scopes.SINGLETON);
        }
      }
    );

    datasetFramework = injector.getInstance(DatasetFramework.class);
  }

  @AfterClass
  public static void afterClass() {
    txManager.stopAndWait();
  }

  @Test
  public void testRouteStorage() throws Exception {
    RouteStore routeStore = new LocalRouteStore(datasetFramework, new InMemoryTxSystemClient(txManager));

    ApplicationId appId = new ApplicationId("n1", "a1");
    ProgramId service1 = appId.service("s1");
    RouteConfig routeConfig = new RouteConfig(ImmutableMap.of("v1", 100));
    routeStore.store(service1, routeConfig);
    Assert.assertEquals(routeConfig.getRoutes(), routeStore.fetch(service1).getRoutes());
    routeStore.delete(service1);
    Assert.assertNotNull(routeStore.fetch(service1));

    try {
      routeStore.delete(service1);
      Assert.fail("Config should have been deleted and thus a NotFoundException must have been thrown.");
    } catch (NotFoundException e) {
      // expected
    }
  }
}
