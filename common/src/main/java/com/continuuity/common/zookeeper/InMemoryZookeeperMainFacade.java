package com.continuuity.common.zookeeper;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.Closeable;
import java.io.IOException;

/**
 * InMemoryZookeeperMainFacade provides a way to isolate the idiosycracies of
 * things that are not so trivial. There are many ways this can be implemented,
 * hence this is an interface.
 */
interface InMemoryZookeeperMainFacade extends Closeable {
  public void run(QuorumPeerConfig config) throws IOException;
  public void block() throws InterruptedException;
  public void kill();
}
