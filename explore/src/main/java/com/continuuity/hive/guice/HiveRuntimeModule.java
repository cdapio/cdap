package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data2.datafabric.dataset.client.DatasetManagerServiceClient;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.hive.inmemory.InMemoryHiveMetastore;
import com.continuuity.hive.server.HiveServer;
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
  private final ClassLoader hiveClassLoader;

  public HiveRuntimeModule(CConfiguration conf, ClassLoader hiveClassLoader) {
    this.conf = conf;
    this.hiveClassLoader = hiveClassLoader;
  }

  public HiveRuntimeModule() {
    this(CConfiguration.create(), HiveRuntimeModule.class.getClassLoader());
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

      return Modules.combine(new HiveModule(hiveClassLoader),
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
    try {
      String auxJarsPath = generateAuxJarsClasspath();
      System.setProperty("hive.aux.jars.path", auxJarsPath);
      LOG.debug("Setting {} to {}", "hive.aux.jars.path",
                System.getProperty("hive.aux.jars.path"));

      return new HiveModule(hiveClassLoader);
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
    Dependencies.findClassDependencies(hiveClassLoader,
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
     DatasetManagerServiceClient.class.getCanonicalName(), "com.continuuity.hive.datasets.DatasetStorageHandler",
     new HBaseTableUtilFactory().get().getClass().getCanonicalName());

    return Joiner.on(',').join(uris);
  }
  /**
   * Common Hive Module for both singlenode and distributed mode.
   */
  private static final class HiveModule extends AbstractModule {

    private HiveModule(ClassLoader hiveClassLoader) {
      this.hiveClassLoader = hiveClassLoader;
    }

    private final ClassLoader hiveClassLoader;

    @Override
    protected void configure() {
      System.setProperty("hive.exec.pre.hooks", "com.continuuity.hive.hooks.TransactionPreHook");
      LOG.debug("Setting {} to {}", "hive.exec.pre.hooks",
                System.getProperty("hive.exec.pre.hooks"));

      System.setProperty("hive.exec.post.hooks", "com.continuuity.hive.hooks.TransactionPostHook");
      LOG.debug("Setting {} to {}", "hive.exec.post.hooks",
                System.getProperty("hive.exec.post.hooks"));

      bind(HiveServer.class).to(RuntimeHiveServer.class).in(Scopes.SINGLETON);
      bind(ClassLoader.class).annotatedWith(Names.named(Constants.Explore.HIVE_CLASSSLOADER))
        .toInstance(hiveClassLoader);
    }

    @Provides
    @Named(Constants.Hive.SERVER_ADDRESS)
    public final InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.Hive.SERVER_ADDRESS),
                                        new InetSocketAddress("localhost", 0).getAddress());
    }
  }
}
