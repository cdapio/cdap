package com.continuuity.hive.context;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
      new DataSetsModules().getDistributedModule()
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
