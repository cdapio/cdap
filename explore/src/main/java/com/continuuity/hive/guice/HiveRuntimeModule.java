package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.hive.datasets.DatasetStorageHandler;
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
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hive.conf.HiveConf;
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
 * Hive Runtime guice module.
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
   * The strategy for hive in singlenode is to have a hive-site.xml file in the classpath containing the configuration
   * needed to start an in-memory hive metastore instance, on an in-memory framework (not hadoop).
   * @param warehouseDir directory of the metastore files (tables metadata)
   * @param databaseDir directory of the hive tables data.
   */
  private Module getLocalModules(File warehouseDir, File databaseDir) {
    LOG.debug("Setting {} to {}", Constants.Hive.METASTORE_WAREHOUSE_DIR, warehouseDir.getAbsoluteFile());
    System.setProperty(Constants.Hive.METASTORE_WAREHOUSE_DIR, warehouseDir.getAbsolutePath());

    LOG.debug("Setting {} to {}", Constants.Hive.DATABASE_DIR, databaseDir.getAbsoluteFile());
    System.setProperty(Constants.Hive.DATABASE_DIR, databaseDir.getAbsolutePath());

    try {
      // This will load the hive-site.xml file in the classpath
      final HiveConf hiveConf = new HiveConf();

      // Select a random free port for hive server to run
      final int hiveServerPort = PortDetector.findFreePort();
      hiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

      // Select a random free port hive metastore to run
      final int hiveMetaStorePort = PortDetector.findFreePort();
      hiveConf.set("hive.metastore.uris", "thrift://localhost:" + hiveMetaStorePort);

      return Modules.combine(new HiveModule(hiveConf, hiveServerPort),
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
      LOG.warn("HiveServer2 not present in classpath, disable explore functionality.");
      return new AbstractModule() {
        @Override
        protected void configure() {
          bind(HiveServer.class).to(MockHiveServer.class).in(Scopes.SINGLETON);
        }
      };
    } else {
      try {
        String auxJarsPath = generateAuxJarsClasspath();
        LOG.error("Setting hive.aux.jars.path to {}", auxJarsPath);
        System.setProperty("hive.aux.jars.path", auxJarsPath);

        HiveConf hiveConf = new HiveConf();

        // The port number is a parameter that is directly read from the hiveConf passed to hive server,
        // contrary to most parameters which need to be in hive-site.xml in the classpath.
        final int hiveServerPort = PortDetector.findFreePort();
        hiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

        final HiveConf newHiveConf = new HiveConf();
        newHiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

        return new HiveModule(newHiveConf, hiveServerPort);
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
    Dependencies.findClassDependencies(this.getClass().getClassLoader(), new Dependencies.ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        if (bootstrapClassPaths.contains(classPathUrl.getFile())) {
          return false;
        }

        uris.add(classPathUrl);
        return true;
      }
    }, HBaseClient.class.getCanonicalName(), DatasetStorageHandler.class.getCanonicalName(),
                                       new HBaseTableUtilFactory().get().getClass().getCanonicalName());

    return Joiner.on(',').join(uris);
  }
  /**
   * Common Hive Module for both singlenode and distributed mode.
   */
  private static final class HiveModule extends AbstractModule {

    private final HiveConf hiveConf;

    // Port number where to launch hive server2
    private final int hiveServerPort;

    protected HiveModule(HiveConf hiveConf, int hiveServerPort) {
      this.hiveConf = hiveConf;
      this.hiveServerPort = hiveServerPort;
    }

    @Override
    protected void configure() {
      bind(HiveConf.class).toInstance(hiveConf);
      bind(int.class).annotatedWith(Names.named(Constants.Hive.SERVER_PORT)).toInstance(hiveServerPort);
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
