/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.AuthorizerTest;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests for {@link DatasetBasedAuthorizer}.
 */
public class DatasetBasedAuthorizerTest extends AuthorizerTest {

  private static DatasetBasedAuthorizer datasetAuthorizer;
  private static TransactionManager txManager;

  @BeforeClass
  public static void setUpClass() {
    Injector injector = Guice.createInjector(
      new TransactionInMemoryModule(),
      new ConfigModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));

          MapBinder<String, DatasetModule> mapBinder = MapBinder.newMapBinder(
            binder(), String.class, DatasetModule.class, Names.named("defaultDatasetModules"));
          mapBinder.addBinding("orderedTable-memory").toInstance(new InMemoryTableModule());

          bind(DatasetFramework.class).to(InMemoryDatasetFramework.class);
        }
      }
    );

    txManager = injector.getInstance(TransactionManager.class);
    datasetAuthorizer = injector.getInstance(DatasetBasedAuthorizer.class);

    txManager.startAndWait();
  }

  @AfterClass
  public static void tearDownClass() {
    txManager.stopAndWait();
  }

  @Override
  protected Authorizer get() {
    return datasetAuthorizer;
  }
}
