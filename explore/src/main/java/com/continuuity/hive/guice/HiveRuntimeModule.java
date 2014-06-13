package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.datafabric.dataset.client.DatasetServiceClient;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.explore.service.ExploreHttpHandler;
import com.continuuity.explore.service.ExploreHttpService;
import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.Hive12ExploreService;
import com.continuuity.explore.service.Hive13ExploreService;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.hive.datasets.DatasetStorageHandler;
import com.continuuity.hive.metastore.HiveMetastore;
import com.continuuity.hive.metastore.InMemoryHiveMetastore;
import com.continuuity.hive.metastore.MockHiveMetastore;
import com.continuuity.hive.server.HiveServer;
import com.continuuity.hive.server.MockHiveServer;
import com.continuuity.hive.server.RuntimeHiveServer;
import com.continuuity.http.HttpHandler;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Set;

/**
 * Hive Runtime guice module. HiveConf settings are set as System Properties.
 */
public class HiveRuntimeModule extends RuntimeModule {
  private static final Logger LOG = LoggerFactory.getLogger(HiveRuntimeModule.class);

  /**
   * Sets up modules for running hive in singlenode or in-memory mode.
   */
  private Module getLocalModule(final boolean inMemory) {
    return Modules.combine(
        new HiveServerModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(ExploreService.class).to(Hive13ExploreService.class).in(Scopes.SINGLETON);
            bind(HiveMetastore.class).toProvider(HiveMetastoreProvider.class).in(Scopes.SINGLETON);
            bind(HiveServerProvider.class).to(HiveLocalServerProvider.class).in(Scopes.SINGLETON);
            bind(boolean.class).annotatedWith(Names.named("inmemory")).toInstance(inMemory);
          }

          @Provides
          @Named(Constants.Hive.METASTORE_PORT)
          private int providesHiveMetastorePort() {
            // Select a random free port for hive metastore to run
            final int hiveMetaStorePort = Networks.getRandomPort();
            if (hiveMetaStorePort < 0) {
              throw new IllegalStateException("Unable to find free port to run Hive MetaStore.");
            }

            System.setProperty(HiveConf.ConfVars.METASTOREURIS.toString(), "thrift://localhost:" + hiveMetaStorePort);
            return hiveMetaStorePort;
          }
        }
    );
  }

  @Override
  public Module getInMemoryModules() {
    return getLocalModule(true);
  }

  @Override
  public Module getSingleNodeModules() {
    return getLocalModule(false);
  }

  @Override
  public Module getDistributedModules() {
    return Modules.combine(
        new HiveServerModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(ExploreService.class).to(Hive12ExploreService.class).in(Scopes.SINGLETON);

            Named exploreSeriveName = Names.named(Constants.Service.EXPLORE_HTTP_USER_SERVICE);
            Multibinder<HttpHandler> handlerBinder =
                Multibinder.newSetBinder(binder(), HttpHandler.class, exploreSeriveName);
            handlerBinder.addBinding().to(ExploreHttpHandler.class);
            handlerBinder.addBinding().to(PingHandler.class);

            bind(ExploreHttpService.class).in(Scopes.SINGLETON);

            bind(HiveServerProvider.class).to(HiveDistributedServerProvider.class).in(Scopes.SINGLETON);
          }
        });
  }

  /**
   * Common Hive Module for both singlenode and distributed mode.
   */
  private static final class HiveServerModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(HiveServer.class).toProvider(HiveServerProvider.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Named(Constants.Hive.SERVER_ADDRESS)
    public final InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.Hive.SERVER_ADDRESS),
                                        new InetSocketAddress("localhost", 0).getAddress());
    }
  }

  @Singleton
  private static final class HiveDistributedServerProvider extends HiveServerProvider {

    @Inject
    private HiveDistributedServerProvider(CConfiguration cConf, Injector injector) {
      super(cConf, injector);
    }

    @Override
    protected void setProperties() {
      try {
        String auxJarsPath = generateAuxJarsClasspath();
        System.setProperty(HiveConf.ConfVars.HIVEAUXJARS.toString(), auxJarsPath);
        LOG.debug("Setting {} to {}", HiveConf.ConfVars.HIVEAUXJARS.toString(),
                  System.getProperty(HiveConf.ConfVars.HIVEAUXJARS.toString()));

        // Set local tmp dir to an absolute location in the twill runnable
        System.setProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.toString(),
                           new File(HiveConf.ConfVars.LOCALSCRATCHDIR.defaultVal).getAbsolutePath());
        LOG.info("Setting {} to {}", HiveConf.ConfVars.LOCALSCRATCHDIR.toString(),
                 System.getProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.toString()));
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    private String generateAuxJarsClasspath() throws IOException {
      // Here we find the transitive dependencies and remove all paths that come from the boot class path -
      // those paths are not needed because the new JVM will have them in its boot class path.
      // It could even be wrong to keep them because in the target container, the boot class path may be different
      // (for example, if Hadoop uses a different Java version that Reactor).

      final Set<URL> uris = Sets.newHashSet();

      ImmutableSet.Builder<String> builder = ImmutableSet.builder();
      for (String classpath : Splitter.on(File.pathSeparatorChar).split(System.getProperty("sun.boot.class.path"))) {
        File file = new File(classpath);
        builder.add(file.getAbsolutePath());
        try {
          builder.add(file.getCanonicalPath());
        } catch (IOException e) {
          // Ignore the exception and proceed.
        }
      }


      final Set<String> bootstrapClassPaths = builder.build();
      Dependencies.findClassDependencies(this.getClass().getClassLoader(),
          new Dependencies.ClassAcceptor() {
            @Override
            public boolean accept(String className, URL classUrl, URL classPathUrl) {
              if (bootstrapClassPaths.contains(classPathUrl.getFile())) {
                return false;
              }

              uris.add(classPathUrl);
              return true;
            }
          },
          DatasetServiceClient.class.getCanonicalName(), DatasetStorageHandler.class.getCanonicalName(),
          new HBaseTableUtilFactory().get().getClass().getCanonicalName());

      return Joiner.on(',').join(uris);
    }
  }

  @Singleton
  private static final class HiveLocalServerProvider extends HiveServerProvider {

    @Inject
    private HiveLocalServerProvider(CConfiguration cConf, Injector injector) {
      super(cConf, injector);
    }

    @Override
    protected void setProperties() {
      // Some more local mode settings
      System.setProperty(HiveConf.ConfVars.LOCALMODEAUTO.toString(), "true");
      System.setProperty(HiveConf.ConfVars.SUBMITVIACHILD.toString(), "false");
      System.setProperty(MRConfig.FRAMEWORK_NAME, "local");

      // Disable security
      // TODO: verify if auth=NOSASL is really needed
      System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.toString(), "NOSASL");
      System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.toString(), "false");
      System.setProperty(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.toString(), "false");
    }
  }

  private abstract static class HiveServerProvider implements Provider<HiveServer> {

    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    private HiveServerProvider(CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    protected abstract void setProperties();

    @Override
    public HiveServer get() {
      boolean exploreEnabled = cConf.getBoolean(Constants.Hive.EXPLORE_ENABLED);
      if (!exploreEnabled) {
        return injector.getInstance(MockHiveServer.class);
      }
      // This will throw exceptions if the checks don't pass
      // TODO remove that in distributed mode
      HiveServer.checkHiveVersion(this.getClass().getClassLoader());

      setProperties();
      return injector.getInstance(RuntimeHiveServer.class);
    }
  }

  @Singleton
  private static final class HiveMetastoreProvider implements Provider<HiveMetastore> {

    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    private HiveMetastoreProvider(CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    private static final long seed = System.currentTimeMillis();
    @Override
    public HiveMetastore get() {
      boolean exploreEnabled = cConf.getBoolean(Constants.Hive.EXPLORE_ENABLED);
      if (!exploreEnabled) {
        return injector.getInstance(MockHiveMetastore.class);
      }
      // This will throw exceptions if the checks don't pass
      HiveServer.checkHiveVersion(this.getClass().getClassLoader());

      File warehouseDir = new File(cConf.get(Constants.Hive.CFG_LOCAL_DATA_DIR), "warehouse");
      File databaseDir = new File(cConf.get(Constants.Hive.CFG_LOCAL_DATA_DIR), "database");

      boolean useSeed = injector.getInstance(Key.get(boolean.class, Names.named("inmemory")));
      if (useSeed) {
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

      return injector.getInstance(InMemoryHiveMetastore.class);
    }
  }
}
