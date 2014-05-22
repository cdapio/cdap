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

  private CConfiguration conf = null;

  public HiveRuntimeModule(CConfiguration conf) {
    this.conf = conf;
  }

  public HiveRuntimeModule() {
    // do nothing
  }

  private Module getLocalModules() {
    try {
      final HiveConf hiveConf = new HiveConf();

      final int hiveServerPort = PortDetector.findFreePort();
      hiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

      final int hiveMetaStorePort = PortDetector.findFreePort();
      hiveConf.set("hive.metastore.uris", "thrift://localhost:" + hiveMetaStorePort);

      return Modules.combine(new HiveModule(),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(HiveConf.class).toInstance(hiveConf);
              bind(int.class).annotatedWith(Names.named(Constants.Hive.METASTORE_PORT)).toInstance(hiveMetaStorePort);
              bind(int.class).annotatedWith(Names.named(Constants.Hive.SERVER_PORT)).toInstance(hiveServerPort);
              bind(InMemoryHiveMetastore.class).in(Scopes.SINGLETON);
              bind(HiveServer.class).in(Scopes.SINGLETON);
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
    String warehouseDir = System.getProperty("java.io.tmpdir") +
        System.getProperty("file.separator") +
        "hive" +
        System.getProperty("file.separator") +
        "warehouse" +
        Long.toString(System.currentTimeMillis());
    LOG.debug("Setting {} to {}", Constants.Hive.METASTORE_WAREHOUSE_DIR, warehouseDir);
    System.setProperty(Constants.Hive.METASTORE_WAREHOUSE_DIR, warehouseDir);
    return getLocalModules();
  }

  @Override
  public Module getSingleNodeModules() {
    File warehouseDir = new File(new File(conf.get(Constants.CFG_LOCAL_DATA_DIR), "hive"), "warehouse");
    File databaseDir = new File(new File(conf.get(Constants.CFG_LOCAL_DATA_DIR), "hive"), "database");
    LOG.debug("Setting {} to {}", Constants.Hive.METASTORE_WAREHOUSE_DIR, warehouseDir.getAbsolutePath());
    LOG.debug("Setting {} to {}", Constants.Hive.DATABASE_DIR, databaseDir.getAbsolutePath());
    System.setProperty(Constants.Hive.METASTORE_WAREHOUSE_DIR, warehouseDir.getAbsolutePath());
    System.setProperty(Constants.Hive.DATABASE_DIR, databaseDir.getAbsolutePath());
    return getLocalModules();
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
      newHiveConf.clear();
      newHiveConf.set("hive.metastore.uris", hiveConf.get("hive.metastore.uris"));
      newHiveConf.set("hive.zookeeper.quorum", hiveConf.get("hive.zookeeper.quorum"));
      newHiveConf.set("hive.zookeeper.client.port", hiveConf.get("hive.zookeeper.client.port"));
      newHiveConf.set("hive.lock.manager", hiveConf.get("hive.lock.manager"));
      newHiveConf.setInt("hive.server2.thrift.port", hiveServerPort);
      newHiveConf.set("mapreduce.framework.name", hiveConf.get("mapreduce.framework.name"));

      return Modules.combine(new HiveModule(),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(HiveConf.class).toInstance(newHiveConf);
              bind(int.class).annotatedWith(Names.named(Constants.Hive.SERVER_PORT)).toInstance(hiveServerPort);
              bind(HiveServer.class).in(Scopes.SINGLETON);
            }
          }
      );
    } catch (Exception e) {
      Throwables.propagate(e);
      return null;
    }
  }

  private static final class HiveModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(HiveCommandExecutor.class);
    }

    @Provides
    @Named(Constants.Hive.SERVER_ADDRESS)
    public final InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.Hive.SERVER_ADDRESS),
                                        new InetSocketAddress("localhost", 0).getAddress());
    }
  }
}
