package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.hive.HiveCommandExecutor;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.inmemory.InMemoryHiveMetastore;
import com.continuuity.test.internal.TempFolder;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;

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

  private Module getLocalModules(File warehouseDir, File databaseDir) {
    LOG.debug("Setting {} to {}", Constants.Hive.METASTORE_WAREHOUSE_DIR, warehouseDir.getAbsoluteFile());
    System.setProperty(Constants.Hive.METASTORE_WAREHOUSE_DIR, warehouseDir.getAbsolutePath());

    LOG.debug("Setting {} to {}", Constants.Hive.DATABASE_DIR, databaseDir.getAbsoluteFile());
    System.setProperty(Constants.Hive.DATABASE_DIR, databaseDir.getAbsolutePath());

    try {
      final HiveConf hiveConf = new HiveConf();

      final int hiveServerPort = PortDetector.findFreePort();
      hiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

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
      Throwables.propagate(e);
      return null;
    }
  }

  @Override
  public Module getInMemoryModules() {
    File warehouseDir = new File(new File(new File(System.getProperty("java.io.tmpdir"), "hive"), "warehouse"),
                                 Long.toString(System.currentTimeMillis()));
    File databaseDir = new File(new File(new File(System.getProperty("java.io.tmpdir"), "hive"), "database"),
                                 Long.toString(System.currentTimeMillis()));
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
      HiveConf hiveConf = new HiveConf();

      // todo figure out for what module exactly we need a hive-site.xml in the classpath
      // The port number is a parameter that is directly read from the hiveConf passed to hive server,
      // Contrary to most parameters which need to be in hive-site.xml in the classpath.
      final int hiveServerPort = PortDetector.findFreePort();
      hiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

      final HiveConf newHiveConf = new HiveConf();
      newHiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

      return new HiveModule(newHiveConf, hiveServerPort);
    } catch (Exception e) {
      Throwables.propagate(e);
      return null;
    }
  }

  private static final class HiveModule extends AbstractModule {

    private final HiveConf hiveConf;
    private final int hiveServerPort;

    protected HiveModule(HiveConf hiveConf, int hiveServerPort) {
      this.hiveConf = hiveConf;
      this.hiveServerPort = hiveServerPort;
    }

    @Override
    protected void configure() {
      bind(HiveConf.class).toInstance(hiveConf);
      bind(HiveCommandExecutor.class);
      bind(HiveServer.class).in(Scopes.SINGLETON);
      bind(int.class).annotatedWith(Names.named(Constants.Hive.SERVER_PORT)).toInstance(hiveServerPort);
    }

    @Provides
    @Named(Constants.Hive.SERVER_ADDRESS)
    public final InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.Hive.SERVER_ADDRESS),
                                        new InetSocketAddress("localhost", 0).getAddress());
    }
  }
}
