/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.test;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.namespace.InMemoryNamespaceClient;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.data.runtime.DynamicTransactionExecutorFactory;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.app.AppFabricDatasetModule;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

/**
 * A lightweight tester for providing {@link DatasetFramework} and {@link TransactionManager} for testing purpose.
 * It is expected to be used with {@link ClassRule} for tests that only need Dataset and transactional operation.
 */
public class AppFabricDatasetTester extends ExternalResource {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private final List<Class<? extends HttpHandler>> handlerClasses;

  private NettyHttpService httpServer;
  private TransactionManager txManager;
  private DatasetFramework datasetFramework;
  private TransactionExecutorFactory txExecutorFactory;
  private TransactionSystemClient txClient;

  /**
   * Creates a testers with an optional set of {@link HttpHandler}. Only dataset related bindings will be available.
   */
  public AppFabricDatasetTester(Class<? extends HttpHandler>...handlerClasses) {
    this.handlerClasses = Arrays.asList(handlerClasses);
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return TEMP_FOLDER.apply(super.apply(base, description), description);
  }

  @Override
  protected void before() throws Throwable {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    txManager = new TransactionManager(new Configuration());
    txManager.startAndWait();

    final Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // Add the app-fabric Dataset module
          MapBinder<String, DatasetModule> datasetModuleBinder = MapBinder.newMapBinder(
            binder(), String.class, DatasetModule.class, Constants.Dataset.Manager.DefaultDatasetModules.class);
          datasetModuleBinder.addBinding("app-fabric").toInstance(new AppFabricDatasetModule());

          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          bind(DatasetFramework.class).to(InMemoryDatasetFramework.class);
          bind(NamespaceQueryAdmin.class).to(InMemoryNamespaceClient.class).in(Scopes.SINGLETON);

          TransactionSystemClient txClient = new InMemoryTxSystemClient(txManager);
          bind(TransactionSystemClient.class).toInstance(txClient);
          bind(TransactionExecutorFactory.class).toInstance(new DynamicTransactionExecutorFactory(txClient));
        }
      }
    );
    httpServer = new CommonNettyHttpServiceBuilder(injector.getInstance(CConfiguration.class), "testService")
      .addHttpHandlers(Iterables.transform(handlerClasses, new Function<Class<? extends HttpHandler>, HttpHandler>() {
        @Override
        public HttpHandler apply(Class<? extends HttpHandler> cls) {
          return injector.getInstance(cls);
        }
      }))
      .build();
    httpServer.startAndWait();

    datasetFramework = injector.getInstance(DatasetFramework.class);
    txClient = injector.getInstance(TransactionSystemClient.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
  }

  @Override
  protected void after() {
    httpServer.stopAndWait();
    txManager.stopAndWait();
  }

  public DatasetFramework getDatasetFramework() {
    return datasetFramework;
  }

  public TransactionExecutorFactory getTxExecutorFactory() {
    return txExecutorFactory;
  }

  public TransactionSystemClient getTxClient() {
    return txClient;
  }

  public URL getEndpointURL(String path) throws MalformedURLException {
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    InetSocketAddress addr = httpServer.getBindAddress();
    return new URL(String.format("http://%s:%d%s", addr.getHostName(), addr.getPort(), path));
  }
}
