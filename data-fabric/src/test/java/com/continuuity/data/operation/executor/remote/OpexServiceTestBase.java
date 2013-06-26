package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.util.OperationUtil;
import org.apache.commons.lang.time.StopWatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public abstract class OpexServiceTestBase {

  static OperationExecutor local, remote;
  static InMemoryZookeeper zookeeper;
  static CConfiguration config;
  static OperationExecutorService opexService;

  public static void startService(CConfiguration conf, OperationExecutor opex)
      throws Exception {

    // start an in-memory zookeeper and remember it in a config object
    zookeeper = new InMemoryZookeeper();
    config = conf;
    config.set(Constants.CFG_ZOOKEEPER_ENSEMBLE,
        zookeeper.getConnectionString());

    // find a free port to use for the service
    int port = PortDetector.findFreePort();
    config.setInt(Constants.CFG_DATA_OPEX_SERVER_PORT, port);

    // start an opex service
    opexService = new OperationExecutorService(opex);

    // and start it. Since start is blocking, we have to start async'ly
    new Thread () {
      public void run() {
        try {
          opexService.start(new String[] { }, config);
        } catch (Exception e) {
          System.err.println("Failed to start service: " + e.getMessage());
        }
      }
    }.start();

    // and wait until it has fully initialized
    StopWatch watch = new StopWatch();
    watch.start();
    while(watch.getTime() < 10000) {
      if (opexService.ruok()) break;
    }
    Assert.assertTrue("Operation Executor Service failed to come up within " +
        "10 seconds.", opexService.ruok());

    // now create a remote opex that connects to the service
    remote = new RemoteOperationExecutor(config);
    local = opex;
    // clear data fabric, since it is a singleton now, old tests may have left data there
    local.execute(OperationUtil.DEFAULT, new ClearFabric(ClearFabric.ToClear.ALL));
  }

  @AfterClass
  public static void stopService() throws Exception {

    // shutdown the opex service
    if (opexService != null)
      opexService.stop(true);

    // and shutdown the zookeeper
    if (zookeeper != null) {
      zookeeper.close();
    }
  }
  
  @Before
  public void disableQueuePayloads() {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;
  }
  
  @After
  public void enableQueuePayloads() {
    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  protected static List<WriteOperation> batch(WriteOperation ... ops) {
    return Arrays.asList(ops);
  }
}
