package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.hive.HiveCommandExecutor;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.inmemory.InMemoryHiveMetastore;

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

  /**
   * Use the given file as hive-site.xml, instead of the one automatically retrieved from the classpath.
   */
  private void bypassHiveConfClasspath(Configuration hiveConf) throws Exception {
    // This will call the static code that sets hiveSiteURL.
    // We can reset it after this call, using reflection.
    new HiveConf();
    File confDir = Files.createTempDir();
    File hiveFile = new File(confDir, "hive-site.xml");
    hiveConf.writeXml(new FileOutputStream(hiveFile));
    hiveFile.deleteOnExit();

    Field hiveSiteUrlField = HiveConf.class.getDeclaredField("hiveSiteURL");
    hiveSiteUrlField.setAccessible(true);
    hiveSiteUrlField.set(null, hiveFile.toURI().toURL());
  }

  @Override
  public Module getInMemoryModules() {
    try {
      final Configuration hiveConf = new Configuration();
      hiveConf.clear();
      hiveConf.set("hive.server2.authentication", "NOSASL");
      hiveConf.set("hive.metastore.sasl.enabled", "false");
      hiveConf.set("hive.server2.enable.doAs", "false");
      hiveConf.set("hive.exec.mode.local.auto", "true");
      hiveConf.set("hive.exec.submitviachild", "false");
      hiveConf.set("mapreduce.framework.name", "local");
      // TODO: get local data dir from CConf
      hiveConf.set("hive.metastore.warehouse.dir", "/tmp/hive-warehouse");

      hiveConf.set("hive.exec.pre.hooks", "com.continuuity.hive.hooks.TransactionPreHook");
      hiveConf.set("hive.exec.post.hooks", "com.continuuity.hive.hooks.TransactionPostHook");

      final int hiveServerPort = PortDetector.findFreePort();
      hiveConf.setInt("hive.server2.thrift.port", hiveServerPort);

      final int hiveMetaStorePort = PortDetector.findFreePort();
      hiveConf.set("hive.metastore.uris", "thrift://localhost:" + hiveMetaStorePort);

      bypassHiveConfClasspath(hiveConf);

      // This HiveConf object will load the hive-site.xml that we put in a temp folder
      final HiveConf newHiveConf = new HiveConf();
      return Modules.combine(new HiveModule(),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(HiveConf.class).toInstance(newHiveConf);
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
  public Module getSingleNodeModules() {
    return getInMemoryModules();
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

      HiveConf newHiveConf = new HiveConf();
      newHiveConf.clear();
      newHiveConf.set("hive.metastore.uris", hiveConf.get("hive.metastore.uris"));
      newHiveConf.set("hive.zookeeper.quorum", hiveConf.get("hive.zookeeper.quorum"));
      newHiveConf.set("hive.zookeeper.client.port", hiveConf.get("hive.zookeeper.client.port"));
      newHiveConf.set("hive.lock.manager", hiveConf.get("hive.lock.manager"));
      newHiveConf.setInt("hive.server2.thrift.port", hiveServerPort);
      newHiveConf.set("mapreduce.framework.name", hiveConf.get("mapreduce.framework.name"));
      newHiveConf.set("hive.exec.pre.hooks", "com.continuuity.hive.hooks.TransactionPreHook");
      newHiveConf.set("hive.exec.post.hooks", "com.continuuity.hive.hooks.TransactionPostHook");

      bypassHiveConfClasspath(newHiveConf);

      final HiveConf passedHiveConf = new HiveConf();
      return Modules.combine(new HiveModule(),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(HiveConf.class).toInstance(passedHiveConf);
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
