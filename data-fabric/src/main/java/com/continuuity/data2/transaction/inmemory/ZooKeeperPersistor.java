package com.continuuity.data2.transaction.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.continuuity.weave.zookeeper.ZKOperations;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
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
public class ZooKeeperPersistor extends AbstractIdleService implements StatePersistor {

  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperPersistor.class);

  private ZKClientService zkClient;

  public static final String DEFAULT_ZK_PREFIX = "/continuuity";
  public static final String CFG_ZK_PREFIX = "data.zk.prefix";

  private final CConfiguration conf;
  private final String zkBasePath;

  @Inject
  public ZooKeeperPersistor(CConfiguration cconf) {
    String prefix = cconf.get(CFG_ZK_PREFIX, DEFAULT_ZK_PREFIX);
    zkBasePath = prefix + "/tx";
    conf = cconf;
  }

  @Override
  protected void shutDown() throws Exception {
    if (this.zkClient != null) {
      zkClient.stopAndWait();
    }
  }

  @Override
  protected void startUp() throws Exception {
    String zkQuorum = conf.get(Constants.Zookeeper.QUORUM,
                               Constants.Zookeeper.DEFAULT_ZOOKEEPER_ENSEMBLE);
    zkClient =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(zkQuorum).build(),
            RetryStrategies.limit(10, RetryStrategies.fixDelay(1, TimeUnit.SECONDS)))));
    zkClient.startAndWait();

    // ensure the base path exists in ZK
    try {
      ZKOperations.ignoreError(zkClient.create(zkBasePath, null, CreateMode.PERSISTENT, true),
                               KeeperException.NodeExistsException.class, zkBasePath).get();
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
    String path = pathForTag(tag);
    try {
      Uninterruptibles.getUninterruptibly(
        ZKOperations.ignoreError(
          zkClient.delete(path), KeeperException.NoNodeException.class,  path));
    } catch (Throwable e) {
      throw new IOException("Unable to delete state for tag '" + tag + "' at path '" + path + "':", e);
    }
  }

  @Override
  public byte[] readBack(String tag) throws IOException {
    String path = pathForTag(tag);
    try {
      NodeData nodeData = Uninterruptibles.getUninterruptibly(
        ZKOperations.ignoreError(
          zkClient.getData(path), KeeperException.NoNodeException.class, null));
      return nodeData != null ? nodeData.getData() : null;
    } catch (Throwable t) {
      throw new IOException("Unable to read state for tag '" + tag + "' at path '" + path + "':", t);
    }
  }
}
