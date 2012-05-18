package com.continuuity.common.conflake;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
class ConflakeHandler implements Conflake {
  private static final Logger Log = LoggerFactory.getLogger(ConflakeActor.class);
  private long datacenterId;
  private long workerId;
  private UniqueIDGenerator uniqueIDGenerator;
  private String zkEnsemble;
  private CuratorFramework client;

  private final int RETRY_COUNT = 2;
  private final int INTER_RETRY_TIME = 10 * 1000;

  private final String ZK_CONFLAKE_DATACENTER = "/continuuity/systems/conflake/datacenters";
  private final String ZK_CONFLAKE_WORKER = "/continuuity/systems/conflake/workers";

  public ConflakeHandler(final String zkEnsemble) {
    this.zkEnsemble = zkEnsemble;
    this.datacenterId = 0;
    this.workerId = 0;
  }


  public boolean start() {
    try {
      client = CuratorFrameworkFactory.newClient(zkEnsemble,
        new RetryNTimes(RETRY_COUNT, INTER_RETRY_TIME));
      client.start();

      EnsurePath workerPath = new EnsurePath(ZK_CONFLAKE_WORKER);
      workerPath.ensure(client.getZookeeperClient());

      EnsurePath datacenterPath = new EnsurePath(ZK_CONFLAKE_DATACENTER);
      datacenterPath.ensure(client.getZookeeperClient());

      client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(ZK_CONFLAKE_DATACENTER + "/d");
      client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(ZK_CONFLAKE_WORKER + "/w");
      datacenterId = client.checkExists().forPath(ZK_CONFLAKE_DATACENTER).getNumChildren();
      workerId = client.checkExists().forPath(ZK_CONFLAKE_WORKER).getNumChildren();
      uniqueIDGenerator = new UniqueIDGenerator(datacenterId, workerId);
    } catch (IOException e) {
      Log.error("Failed to start conflake service. Reason : " + e.getMessage());
      return false;
    } catch (Exception e) {
      Log.error("Failed to start conflake service. Reason : " + e.getMessage());
      return false;
    }
    return true;
  }

  public long next() {
    return uniqueIDGenerator.next();
  }


  public void stop() {
    Closeables.closeQuietly(client);
  }
}
