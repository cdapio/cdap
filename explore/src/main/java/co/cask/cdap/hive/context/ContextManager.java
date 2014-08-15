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

package co.cask.cdap.hive.context;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.zookeeper.ZKClientService;

import java.io.Closeable;
import java.io.IOException;

/**
 * Stores/creates context for Hive queries to run in MapReduce jobs.
 */
public class ContextManager {
  private static Context savedContext;

  public static void saveContext(DatasetFramework datasetFramework) {
    savedContext = new Context(datasetFramework);
  }

  public static Context getContext(Configuration conf) throws IOException {
    if (savedContext == null) {
      return createContext(conf);
    }

    return savedContext;
  }

  private static Context createContext(Configuration conf) throws IOException {
    // Create context needs to happen only when running in as a MapReduce job.
    // In other cases, ContextManager will be initialized using saveContext method.

    CConfiguration cConf = ConfigurationUtil.get(conf, Constants.Explore.CCONF_KEY, CConfCodec.INSTANCE);
    Configuration hConf = ConfigurationUtil.get(conf, Constants.Explore.HCONF_KEY, HConfCodec.INSTANCE);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);;
        }
      }
    );

    ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    return new Context(datasetFramework, zkClientService);
  }

  /**
   * Contains DatasetFramework object required to run Hive queries in MapReduce jobs.
   */
  public static class Context implements Closeable {
    private final DatasetFramework datasetFramework;
    private final ZKClientService zkClientService;

    public Context(DatasetFramework datasetFramework, ZKClientService zkClientService) {
      this.datasetFramework = datasetFramework;
      this.zkClientService = zkClientService;
    }

    public Context(DatasetFramework datasetFramework) {
      this(datasetFramework, null);
    }

    public DatasetFramework getDatasetFramework() {
      return datasetFramework;
    }

    @Override
    public void close() {
      if (zkClientService != null) {
        zkClientService.stopAndWait();
      }
    }
  }
}
