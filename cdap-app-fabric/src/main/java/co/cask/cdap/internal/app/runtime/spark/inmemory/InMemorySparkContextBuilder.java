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

package co.cask.cdap.internal.app.runtime.spark.inmemory;

import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.runtime.spark.AbstractSparkContextBuilder;
import co.cask.cdap.internal.app.runtime.spark.BasicSparkContext;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Builds an instance of {@link BasicSparkContext} good for in-memory environment
 */
public class InMemorySparkContextBuilder extends AbstractSparkContextBuilder {
  private final CConfiguration cConf;

  public InMemorySparkContextBuilder(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  protected Injector prepare() {
    // TODO: this logic should go into DataFabricModules. We'll move it once Guice modules are refactored
    Constants.InMemoryPersistenceType persistenceType = Constants.InMemoryPersistenceType.valueOf(
      cConf.get(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.DEFAULT_DATA_INMEMORY_PERSISTENCE));

    if (Constants.InMemoryPersistenceType.MEMORY == persistenceType) {
      return createInMemoryModules();
    } else {
      return createPersistentModules();
    }
  }

  private Injector createInMemoryModules() {
    ImmutableList<Module> inMemoryModules = ImmutableList.of(
      new ConfigModule(cConf),
      new LocalConfigModule(),
      new IOModule(),
      new AuthModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getLocalModule(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new LoggingModules().getInMemoryModules(),
      new StreamAdminModules().getInMemoryModules(),
      new NotificationFeedServiceRuntimeModule().getInMemoryModules()
    );

    return Guice.createInjector(inMemoryModules);
  }

  private Injector createPersistentModules() {
    ImmutableList<Module> standaloneModules = ImmutableList.of(
      new ConfigModule(cConf),
      new LocalConfigModule(),
      new IOModule(),
      new AuthModule(),
      new LocationRuntimeModule().getStandaloneModules(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new DataFabricModules().getStandaloneModules(),
      new DataSetsModules().getLocalModule(),
      new MetricsClientRuntimeModule().getStandaloneModules(),
      new LoggingModules().getStandaloneModules(),
      new StreamAdminModules().getStandaloneModules(),
      new NotificationFeedServiceRuntimeModule().getStandaloneModules()
    );
    return Guice.createInjector(standaloneModules);
  }

  /**
   * Provides bindings to configs needed by other modules binding in this class.
   */
  private static class LocalConfigModule extends AbstractModule {

    @Override
    protected void configure() {
      // No-op
    }

    @Provides
    @Named(Constants.AppFabric.SERVER_ADDRESS)
    public InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.AppFabric.SERVER_ADDRESS),
                              new InetSocketAddress("localhost", 0).getAddress());
    }
  }
}
