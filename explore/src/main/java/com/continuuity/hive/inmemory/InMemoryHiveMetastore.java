package com.continuuity.hive.inmemory;

import com.continuuity.common.conf.Constants;
import com.continuuity.hive.server.RuntimeHiveServer;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive Metastore running in memory.
 */
public class InMemoryHiveMetastore extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryHiveMetastore.class);

  private final int hiveMetastorePort;

  @Inject
  public InMemoryHiveMetastore(@Named(Constants.Hive.METASTORE_PORT) int hiveMetastorePort) {
    this.hiveMetastorePort = hiveMetastorePort;
  }

  @Override
  protected void startUp() throws Exception {

    LOG.debug("Starting hive metastore on port {}...", hiveMetastorePort);
    Thread metaStoreRunner = new Thread(
        new Runnable() {
          @Override
          public void run() {
            try {
              // No need to pass the hive conf, hive-site.xml will be read from the classpath directly
              HiveMetaStore.main(new String[]{"-v", "-p", Integer.toString(hiveMetastorePort)});
            } catch (Throwable throwable) {
              LOG.error("Exception while starting Hive MetaStore: ", throwable);
            }
          }
        });
    metaStoreRunner.setDaemon(true);
    metaStoreRunner.start();
    RuntimeHiveServer.waitForPort("localhost", hiveMetastorePort);
  }

  @Override
  protected void shutDown() throws Exception {
    // do nothing since HiveMetaStore DB connection gets closed when its client's connection closes.
  }
}
