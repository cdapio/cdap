package com.continuuity.explore.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.hive.Hive13ExploreService;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Guice runtime module for the explore functionality.
 */
public class ExploreRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new ExploreInMemoryModule(true);
  }

  @Override
  public Module getSingleNodeModules() {
    return null;
  }

  @Override
  public Module getDistributedModules() {
    return new ExploreDistributedModule();
  }

  private static final class ExploreInMemoryModule extends PrivateModule {
    private static final Logger LOG = LoggerFactory.getLogger(ExploreInMemoryModule.class);

    private final boolean isInMemory;

    public ExploreInMemoryModule(boolean isInMemory) {
      this.isInMemory = isInMemory;
    }

    @Override
    protected void configure() {
      // Current version of hive used in Singlenode is Hive 13
      bind(ExploreService.class).annotatedWith(Names.named("explore.service.impl")).to(Hive13ExploreService.class);
      bind(ExploreService.class).toProvider(ExploreServiceProvider.class).in(Scopes.SINGLETON);
      expose(ExploreService.class);

      bind(boolean.class).annotatedWith(Names.named("explore.inmemory")).toInstance(isInMemory);
    }

    @Singleton
    private static final class ExploreServiceProvider implements Provider<ExploreService> {

      private final CConfiguration cConf;
      private final ExploreService exploreService;
      private final boolean isInMemory;

      @Inject
      private ExploreServiceProvider(CConfiguration cConf,
                                     @Named("explore.service.impl") ExploreService exploreService,
                                     @Named("explore.inmemory") boolean isInMemory) {
        this.exploreService = exploreService;
        this.cConf = cConf;
        this.isInMemory = isInMemory;
      }

      private static final long seed = System.currentTimeMillis();
      @Override
      public ExploreService get() {
        File warehouseDir = new File(cConf.get(Constants.Explore.CFG_LOCAL_DATA_DIR), "warehouse");
        File databaseDir = new File(cConf.get(Constants.Explore.CFG_LOCAL_DATA_DIR), "database");

        if (isInMemory) {
          // This seed is required to make all tests pass when launched together, and when several of them
          // start a hive metastore / hive server.
          // TODO try to remove once maven is there
          warehouseDir = new File(warehouseDir, Long.toString(seed));
          databaseDir = new File(databaseDir, Long.toString(seed));
        }

        LOG.debug("Setting {} to {}",
                  HiveConf.ConfVars.METASTOREWAREHOUSE.toString(), warehouseDir.getAbsoluteFile());
        System.setProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.toString(), warehouseDir.getAbsolutePath());

        String connectUrl = String.format("jdbc:derby:;databaseName=%s;create=true", databaseDir.getAbsoluteFile());
        LOG.debug("Setting {} to {}", HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(), connectUrl);
        System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(), connectUrl);

        // Some more local mode settings
        System.setProperty(HiveConf.ConfVars.LOCALMODEAUTO.toString(), "true");
        System.setProperty(HiveConf.ConfVars.SUBMITVIACHILD.toString(), "false");
        System.setProperty(MRConfig.FRAMEWORK_NAME, "local");

        // Disable security
        // TODO: verify if auth=NOSASL is really needed
        System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.toString(), "NOSASL");
        System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.toString(), "false");
        System.setProperty(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.toString(), "false");

        return exploreService;
      }
    }
  }

  private static final class ExploreDistributedModule extends PrivateModule {
    @Override
    protected void configure() {

    }
  }
}
