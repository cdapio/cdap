package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data2.datafabric.dataset.client.DatasetManagerServiceClient;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.hive.datasets.DatasetStorageHandler;
import com.continuuity.hive.hooks.TransactionPostHook;
import com.continuuity.hive.hooks.TransactionPreHook;
import com.continuuity.hive.inmemory.InMemoryHiveMetastore;
import com.continuuity.hive.server.HiveServer;
import com.continuuity.hive.server.MockHiveServer;
import com.continuuity.hive.server.RuntimeHiveServer;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
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

  private final CConfiguration conf;

  public HiveRuntimeModule(CConfiguration conf) {
    this.conf = conf;
  }

  public HiveRuntimeModule() {
    this.conf = CConfiguration.create();
  }

  /**
   * Sets up modules for running hive in singlenode or in-memory mode.
   * @param warehouseDir directory of the metastore files (tables metadata)
   * @param databaseDir directory of the hive tables data.
   */
  private Module getLocalModules(File warehouseDir, File databaseDir) {
    LOG.debug("Setting {} to {}", HiveConf.ConfVars.METASTOREWAREHOUSE.toString(), warehouseDir.getAbsoluteFile());
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

    try {
      // Select a random free port hive metastore to run
      final int hiveMetaStorePort = PortDetector.findFreePort();
      System.setProperty(HiveConf.ConfVars.METASTOREURIS.toString(), "thrift://localhost:" + hiveMetaStorePort);

      return Modules.combine(new HiveModule(),
                             new AbstractModule() {
                               @Override
                               protected void configure() {
                                 bind(int.class).annotatedWith(Names.named(Constants.Hive.METASTORE_PORT))
                                     .toInstance(hiveMetaStorePort);
                                 bind(InMemoryHiveMetastore.class).in(Scopes.SINGLETON);
                               }
                             }
      );
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static long timestamp = System.currentTimeMillis();
  @Override
  public Module getInMemoryModules() {
    File warehouseDir = new File(new File(new File(System.getProperty("java.io.tmpdir"), "hive"), "warehouse"),
                                 Long.toString(timestamp));
    File databaseDir = new File(new File(new File(System.getProperty("java.io.tmpdir"), "hive"), "database"),
                                Long.toString(timestamp));
    return getLocalModules(warehouseDir, databaseDir);
  }

  @Override
  public Module getSingleNodeModules() {
    File warehouseDir = new File(new File(conf.get(Constants.CFG_LOCAL_DATA_DIR), "hive"), "warehouse");
    File databaseDir = new File(new File(conf.get(Constants.CFG_LOCAL_DATA_DIR), "hive"), "database");
    return getLocalModules(warehouseDir, databaseDir);
  }

  @Override
  public Module getDistributedModules() {
    // Hive is optional - if its libraries are not there, reactor still runs
    if (!HiveServer.isHivePresent()) {
      LOG.warn("HiveServer2 not present in classpath, disabling explore functionality.");
      return new AbstractModule() {
        @Override
        protected void configure() {
          bind(HiveServer.class).to(MockHiveServer.class).in(Scopes.SINGLETON);
        }
      };
    } else {
      try {
        String auxJarsPath = generateAuxJarsClasspath();
        System.setProperty(HiveConf.ConfVars.HIVEAUXJARS.toString(), auxJarsPath);
        LOG.debug("Setting {} to {}", HiveConf.ConfVars.HIVEAUXJARS.toString(),
                  System.getProperty(HiveConf.ConfVars.HIVEAUXJARS.toString()));

        return new HiveModule();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
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
     DatasetManagerServiceClient.class.getCanonicalName(), DatasetStorageHandler.class.getCanonicalName(),
     new HBaseTableUtilFactory().get().getClass().getCanonicalName());

    return Joiner.on(',').join(uris);
  }
  /**
   * Common Hive Module for both singlenode and distributed mode.
   */
  private static final class HiveModule extends AbstractModule {

    @Override
    protected void configure() {
      System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.toString(), TransactionPreHook.class.getCanonicalName());
      LOG.debug("Setting {} to {}", HiveConf.ConfVars.PREEXECHOOKS.toString(),
                System.getProperty(HiveConf.ConfVars.PREEXECHOOKS.toString()));

      System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.toString(), TransactionPostHook.class.getCanonicalName());
      LOG.debug("Setting {} to {}", HiveConf.ConfVars.POSTEXECHOOKS.toString(),
                System.getProperty(HiveConf.ConfVars.POSTEXECHOOKS.toString()));

      bind(HiveServer.class).to(RuntimeHiveServer.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Named(Constants.Hive.SERVER_ADDRESS)
    public final InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.Hive.SERVER_ADDRESS),
                                        new InetSocketAddress("localhost", 0).getAddress());
    }
  }
}
