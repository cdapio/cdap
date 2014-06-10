package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.datafabric.dataset.client.DatasetServiceClient;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.hive.datasets.DatasetStorageHandler;
import com.continuuity.hive.hooks.TransactionPostHook;
import com.continuuity.hive.hooks.TransactionPreHook;
import com.continuuity.hive.metastore.HiveMetastore;
import com.continuuity.hive.metastore.InMemoryHiveMetastore;
import com.continuuity.hive.metastore.MockHiveMetastore;
import com.continuuity.hive.server.HiveServer;
import com.continuuity.hive.server.MockHiveServer;
import com.continuuity.hive.server.RuntimeHiveServer;

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
  private Module getLocalModule() {
    return Modules.combine(new HiveServerModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(HiveMetastore.class).annotatedWith(Names.named("hive")).
              to(InMemoryHiveMetastore.class).in(Scopes.SINGLETON);
            bind(HiveMetastore.class).toProvider(HiveMetastoreProvider.class).in(Scopes.SINGLETON);
          }

          @Provides
          @Named(Constants.Hive.METASTORE_PORT)
          private int providesHiveMetastorePort(CConfiguration cConf) {
            // Would be better to do it in a local specific HiveServerProvider rather than using this little hack.
            File warehouseDir = new File(cConf.get(Constants.Hive.CFG_LOCAL_DATA_DIR), "warehouse");
            File databaseDir = new File(cConf.get(Constants.Hive.CFG_LOCAL_DATA_DIR), "database");

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
    return getLocalModule();
  }

  @Override
  public Module getSingleNodeModules() {
    return getLocalModule();
  }

  @Override
  public Module getDistributedModules() {
    try {
      String auxJarsPath = generateAuxJarsClasspath();
      System.setProperty(HiveConf.ConfVars.HIVEAUXJARS.toString(), auxJarsPath);
      LOG.debug("Setting {} to {}", HiveConf.ConfVars.HIVEAUXJARS.toString(),
          System.getProperty(HiveConf.ConfVars.HIVEAUXJARS.toString()));

      return new HiveServerModule();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String generateAuxJarsClasspath() throws IOException {
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
  /**
   * Common Hive Module for both singlenode and distributed mode.
   */
  private static final class HiveServerModule extends AbstractModule {

    @Override
    protected void configure() {
      System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.toString(), TransactionPreHook.class.getCanonicalName());
      LOG.debug("Setting {} to {}", HiveConf.ConfVars.PREEXECHOOKS.toString(),
                System.getProperty(HiveConf.ConfVars.PREEXECHOOKS.toString()));

      System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.toString(), TransactionPostHook.class.getCanonicalName());
      LOG.debug("Setting {} to {}", HiveConf.ConfVars.POSTEXECHOOKS.toString(),
                System.getProperty(HiveConf.ConfVars.POSTEXECHOOKS.toString()));


      bind(HiveServer.class).annotatedWith(Names.named("hive")).to(RuntimeHiveServer.class).in(Scopes.SINGLETON);
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
  private static final class HiveServerProvider implements Provider<HiveServer> {

    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    private HiveServerProvider(CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    @Override
    public HiveServer get() {
      boolean exploreEnabled = cConf.getBoolean(Constants.Hive.EXPLORE_ENABLED);
      if (exploreEnabled) {
        // This will throw exceptions if the checks don't pass
        HiveServer.checkHiveVersion();
      }
      return exploreEnabled ?
        injector.getInstance(Key.get(HiveServer.class, Names.named("hive"))) :
        injector.getInstance(MockHiveServer.class);
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

    @Override
    public HiveMetastore get() {
      boolean exploreEnabled = cConf.getBoolean(Constants.Hive.EXPLORE_ENABLED);
      if (exploreEnabled) {
        // This will throw exceptions if the checks don't pass
        HiveServer.checkHiveVersion();
      }
      return exploreEnabled ?
        injector.getInstance(Key.get(HiveMetastore.class, Names.named("hive"))) :
        injector.getInstance(MockHiveMetastore.class);
    }
  }
}
