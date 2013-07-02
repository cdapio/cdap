package com.continuuity.common.zookeeper;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;

/**
 * InMemoryZookeeperMain is an adapter translating ZooKeeperServer and InMemoryZooKeeperServer.
 * Killing of ZK requires some hacks - uses reflection (setAccessible) to get access to internal
 * variables to kill a running ZK instance.
 */
class InMemoryZookeeperMain extends ZooKeeperServerMain implements InMemoryZookeeperMainFacade {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryZookeeperMain.class);
  private final CountDownLatch latch = new CountDownLatch(1);

  /**
   * Runs a ZooKeeperServer with provided {@link QuorumPeerConfig}.
   * @param config Configuration of Peer.
   * @throws Exception
   */
  public void run(QuorumPeerConfig config) throws IOException {
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.readFrom(config);
    latch.countDown();
    super.runFromConfig(serverConfig);
  }

  /**
   * Waits till ZooKeeperServer is started completely.
   * @throws Exception
   */
  public void block() throws InterruptedException {
    latch.await();
    Thread.sleep(30);
  }

  /**
   * Kills a running of Zookeeper by going into the ZooKeeperServerMain
   * and fideling with internals of ZK. There is really no other way of
   * doing this correctly.
   */
  public void kill() {
    try {
      Field connectionFactoryField = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
      connectionFactoryField.setAccessible(true);
      ServerCnxnFactory cnxnFactory = (ServerCnxnFactory) connectionFactoryField.get(this);
      cnxnFactory.closeAll();

      Field ssField = cnxnFactory.getClass().getDeclaredField("ss");
      ssField.setAccessible(true);
      ServerSocketChannel ss = (ServerSocketChannel) ssField.get(cnxnFactory);
      ss.close();
    } catch (Exception e) {
      LOG.error("Failed to kill InMemoryZookeeper " + e.getMessage());
    }
  }

  /**
   * Closes a connection of ZK server.
   * @throws IOException
   */
  public void close() throws IOException {
    shutdown();
  }
}
