package com.continuuity.data2.transaction.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A transaction state persistor that uses ZooKeeper.
 */
public class ZooKeeperPersistor implements StatePersistor {

  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperPersistor.class);

  private ZKClientService zkClient;

  public static final String DEFAULT_ZK_PREFIX = "/continuuity";
  public static final String CFG_ZK_PREFIX = "data.zk.prefix";

  private final String zkBasePath;

  @Inject
  public ZooKeeperPersistor(CConfiguration conf) {
    String prefix = conf.get(CFG_ZK_PREFIX, DEFAULT_ZK_PREFIX);
    zkBasePath = prefix + "/tx";

    String zkQuorum = conf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE, Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);
    zkClient =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(zkQuorum).build(),
            RetryStrategies.fixDelay(1, TimeUnit.SECONDS))));
    zkClient.startAndWait();

    // ensure the base path exists in ZK
    try {
      if (zkClient.exists(zkBasePath).get() == null) {
        try {
          zkClient.create(zkBasePath, null, CreateMode.PERSISTENT, true).get();
        } catch (ExecutionException e) {
          throw e.getCause();
        }
      }
    } catch (KeeperException.NodeExistsException e) {
      // hmm it exists although exists() returned null? someone else must have created it just now. Ignore.
    } catch (Throwable e) {
      LOG.error("Exception when initializing ZK base path '" + zkBasePath + "':", e);
      throw Throwables.propagate(e);
    }
  }

  private String pathForTag(String tag) {
    return zkBasePath + "/" + tag;
  }

  @Override
  public void persist(String tag, byte[] state) throws IOException {
    try {
      try {
        // attempt to update
        Uninterruptibles.getUninterruptibly(zkClient.setData(pathForTag(tag), state));
      } catch (ExecutionException e) {
        if (e.getCause() instanceof KeeperException.NoNodeException) {
          // does not exist - that means we must create it
          Uninterruptibles.getUninterruptibly(zkClient.create(pathForTag(tag), state, CreateMode.PERSISTENT));
        } else {
          throw e;
        }
      }
    } catch (ExecutionException e) {
      throw new IOException("Unable to persist state for tag '" + tag + "' to path '" + pathForTag(tag) + "':",
        e.getCause());
    }
  }

  @Override
  public void delete(String tag) throws IOException {
    try {
      Uninterruptibles.getUninterruptibly(zkClient.delete(pathForTag(tag)));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof KeeperException.NoNodeException) {
        // node did not exist - ignore
        return;
      }
      throw new IOException("Unable to delete state for tag '" + tag + "' at path '" + pathForTag(tag) + "':",
                            e.getCause());
    }
  }

  @Override
  public byte[] readBack(String tag) throws IOException {
    try {
      NodeData nodeData = Uninterruptibles.getUninterruptibly(zkClient.getData(pathForTag(tag)));
      return nodeData.getData();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof KeeperException.NoNodeException) {
        // node did not exist
        return null;
      }
      throw new IOException("Unable to read state for tag '" + tag + "' at path '" + pathForTag(tag) + "':",
                            e.getCause());
    }
  }
}
