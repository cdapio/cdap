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
 * ConflakeHandler is responsible for generating a unique data center id and worker id.
 * It does so by holding the datacenter id and worker ids within a high available zk
 * ensemble.
 */
class ConflakeHandler implements Conflake {
  private static final Logger Log = LoggerFactory.getLogger(ConflakeActor.class);
  /**
   * Data center ID for id generation.
   */
  private long datacenterId;

  /**
   * Worker ID of the worker responsible for generating ids.
   */
  private long workerId;

  /**
   * UniqueID generator object.
   */
  private UniqueIDGenerator uniqueIDGenerator;

  /**
   * Zookeeper ensemble.
   */
  private transient String zkEnsemble;

  /**
   * Curator client provides zookeeper related services
   */
  private CuratorFramework client;

  /**
   * Number of times ZK connection has to be retried before giving up.
   */
  private final int RETRY_COUNT = 2;

  /**
   * Number of milli-seconds between retries.
   */
  private final int INTER_RETRY_TIME = 10 * 1000;

  /**
   * Path used by conflake for tracking number of data centers.
   */
  private final String ZK_CONFLAKE_DATACENTER = "/continuuity/systems/conflake/datacenters";

  /**
   * Path used by conflake for tracking number of workers for generating id.
   */
  private final String ZK_CONFLAKE_WORKER = "/continuuity/systems/conflake/workers";

  /**
   * Constructs an instance of ConflakeHandler connecting to zk ensemble.
   * @param zkEnsemble
   */
  public ConflakeHandler(final String zkEnsemble) {
    this.zkEnsemble = zkEnsemble;
    this.datacenterId = 0;
    this.workerId = 0;
  }

  /**
   * Starts the Curator framework and also auto-magically assigns datacenter id and workerId.
   * <p>
   * Following is the way the worker ids and data center ids are assigned.
   * <ul>
   *   <li>First, we get the worker ID by connecting to ZK path and getting number of children under that path</li>
   *   <li>If the workerId is greater than 4096, then we add a new datacenterId node.</li>
   *   <li>Overall we can have upto 16,777,216 (16M) possible combinations of datacenter and worker ids. That means
   *       we can have that many instances of Conflake services running. </li>
   * </ul>
   * </p>
   * In order to assign these Ids, we start with workerId. A workerId is picked up first
   * @return true if successful; false otherwise.
   */
  public boolean start() {
    try {
      client = CuratorFrameworkFactory.newClient(zkEnsemble,
        new RetryNTimes(RETRY_COUNT, INTER_RETRY_TIME));
      client.start();

      /** Make sure the path exists, if not create it. This would no-op once created */
      EnsurePath workerPath = new EnsurePath(ZK_CONFLAKE_WORKER);
      workerPath.ensure(client.getZookeeperClient());

      EnsurePath datacenterPath = new EnsurePath(ZK_CONFLAKE_DATACENTER);
      datacenterPath.ensure(client.getZookeeperClient());

      client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(ZK_CONFLAKE_WORKER + "/w");
      workerId = client.checkExists().forPath(ZK_CONFLAKE_WORKER).getNumChildren();
      if(workerId >= 4095) {
        client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(ZK_CONFLAKE_DATACENTER + "/d");
      }
      datacenterId = client.checkExists().forPath(ZK_CONFLAKE_DATACENTER).getNumChildren();

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

  /**
   * Returns the next monotonically increasing id.
   * @return unique id
   */
  public long next() {
    return uniqueIDGenerator.next();
  }

  /**
   * Stop the ZK ensemble client.
   */
  public void stop() {
    if (client == null) {
      Closeables.closeQuietly(client);
    }
  }
}
