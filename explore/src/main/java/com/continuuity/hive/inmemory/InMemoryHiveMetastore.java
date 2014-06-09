package com.continuuity.hive.inmemory;

import com.continuuity.common.conf.Constants;
import com.continuuity.hive.server.RuntimeHiveServer;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Hive Metastore running in memory.
 */
public class InMemoryHiveMetastore extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryHiveMetastore.class);

  private final int hiveMetastorePort;
  private final ExecutorService executorService;

  @Inject
  public InMemoryHiveMetastore(@Named(Constants.Hive.METASTORE_PORT) int hiveMetastorePort) {
    this.hiveMetastorePort = hiveMetastorePort;

    this.executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread t = new Thread(runnable);
        t.setDaemon(true);
        return t;
      }
    });
  }

  @Override
  protected void startUp() throws Exception {

    // Default min threads is 200, we don't need that many in singlenode or tests
    System.setProperty("hive.metastore.server.min.threads", "5");
    System.setProperty("hive.metastore.server.max.threads", "50");

    LOG.debug("Starting hive metastore on port {}...", hiveMetastorePort);

    executorService.execute(new Runnable() {
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
    RuntimeHiveServer.waitForPort("localhost", hiveMetastorePort);
  }

  @Override
  protected void shutDown() throws Exception {
    // TODO this call does not close hive metastore - find a way to do it
    executorService.shutdown();
  }
}
