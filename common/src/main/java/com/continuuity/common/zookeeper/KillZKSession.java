package com.continuuity.common.zookeeper;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Utility class for killing ZK client to similate failures during testing.
 */
public final class KillZKSession {

  /**
   * Utility classes should have a public constructor or a default constructor
   * hence made it private.
   */
  private KillZKSession() {}

  /**
   * Kills a Zookeeper client to simulate failure scenarious during testing.
   * A default 10seconds is given for client to die.
   *
   * @param client that needs to be killed.
   * @param connectionString of Quorum
   * @throws Exception
   */
  public static void kill(ZooKeeper client, String connectionString) throws IOException, InterruptedException {
    kill(client, connectionString, 10000);
  }

  /**
   * Kills a Zookeeper client to simulate failure scenarious during testing.
   * Callee will provide the amount of time to wait before it's considered failure
   * to kill a client.
   *
   * @param client that needs to be killed.
   * @param connectionString of Quorum
   * @param maxMs time in millisecond specifying the max time to kill a client.
   * @throws Exception
   */
  public static void kill(ZooKeeper client, String connectionString, int maxMs) throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    long sleep = 10;

    ZooKeeper zk = new ZooKeeper(connectionString, maxMs, null, client.getSessionId(), client.getSessionPasswd());

    try {
      try {
        while((System.currentTimeMillis() - start) < maxMs) {
          client.exists("/continuuity", false);
          Thread.sleep(sleep);
        }
      } catch(InterruptedException e) {
          Thread.currentThread().interrupt();
      } catch(Exception expected) {

      }
    } finally {
      zk.close();
    }
  }
}
