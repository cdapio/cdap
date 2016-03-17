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

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.internal.app.runtime.DefaultAdmin;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.AuthorizerTest;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

/**
 * Tests for {@link DatasetBasedAuthorizer}.
 */
public class DatasetBasedAuthorizerTest extends AuthorizerTest {

  private static final DatasetBasedAuthorizer datasetAuthorizer = new DatasetBasedAuthorizer();
  private static TransactionManager txManager;

  @BeforeClass
  public static void setUpClass() throws Exception {
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

          bind(DatasetFramework.class).to(InMemoryDatasetFramework.class).in(Scopes.SINGLETON);
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
        }
      }
    );

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    DatasetFramework dsFramework = injector.getInstance(DatasetFramework.class);
    SystemDatasetInstantiator instantiator = new SystemDatasetInstantiator(dsFramework, null, null);
    TransactionSystemClient txClient = injector.getInstance(TransactionSystemClient.class);
    final DynamicDatasetCache dsCache = new SingleThreadDatasetCache(instantiator, txClient, NamespaceId.DEFAULT,
                                                                     ImmutableMap.<String, String>of(), null, null);
    Admin admin = new DefaultAdmin(dsFramework, NamespaceId.DEFAULT);
    Transactional txnl = new Transactional() {
      @Override
      public void execute(TxRunnable runnable) throws TransactionFailureException {
        TransactionContext transactionContext = dsCache.get();
        transactionContext.start();
        try {
          runnable.run(dsCache);
        } catch (TransactionFailureException e) {
          transactionContext.abort(e);
        } catch (Throwable t) {
          transactionContext.abort(new TransactionFailureException("Exception raised from TxRunnable.run()", t));
        }
        transactionContext.finish();
      }
    };
    AuthorizationContext authContext = new DefaultAuthorizationContext(new Properties(), dsCache, admin, txnl);
    datasetAuthorizer.initialize(authContext);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    datasetAuthorizer.destroy();
    txManager.stopAndWait();
  }

  @Override
  protected Authorizer get() {
    return datasetAuthorizer;
  }

  @Override
  @Test(expected = UnsupportedOperationException.class)
  public void testRBAC() throws Exception {
    // DatasetBasedAuthorizer currently does not support role based access control
    super.testRBAC();
  }
}
