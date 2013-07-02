package com.continuuity.common.zookeeper;

import com.continuuity.common.utils.DirUtils;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An in memory zookeeper server front that delegates all of the functionality to
 * an implementation of {@link InMemoryZookeeperMainFacade}.
 */
class InMemoryZookeeperServer extends QuorumPeerMain implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryZookeeper.class);
  private final QuorumConfigBuilder configBuilder;
  private final InMemoryZookeeperMainFacade main;
  private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
  private final int instanceIndex;

  /**
   * Defines the states of Zookeeper server.
   */
  private enum State {
    LATENT,
    STARTED,
    STOPPED,
    CLOSED
  }

  /**
   * Basic constructor.
   * @param configBuilder
   */
  public InMemoryZookeeperServer(QuorumConfigBuilder configBuilder) {
    this(configBuilder, 0);
  }

  /**
   * Constructor creates an instance of Zookeeper based on the config.
   * @param configBuilder Quorum config builder.
   * @param instanceIndex Instance ID of this zookeeper
   */
  public InMemoryZookeeperServer(QuorumConfigBuilder configBuilder, int instanceIndex) {
    this.configBuilder = configBuilder;
    this.instanceIndex = instanceIndex;
    this.main = new InMemoryZookeeperMain();
  }


  /**
   * Starts an in-memory instance of Zookeeper.
   * @throws Exception
   */
  public void start() throws IOException, InterruptedException {
    if (!state.compareAndSet(State.LATENT, State.STARTED)) {
      return;
    }

    Thread zkThread = new Thread(new Runnable() {
      public void run() {
        try {
          QuorumPeerConfig config = configBuilder.buildConfig(instanceIndex);
          main.run(config);
        } catch (Exception e) {
          LOG.error("Failed to start InMemoryZookeeper " + e.getMessage());
        }
      }
    });
    zkThread.start();
    // Wait till ZK is up and running.
    main.block();
  }

  /**
   * Stop Zookeeper server.
   * @throws IOException
   */
  public void stop() throws IOException {
    if (state.compareAndSet(State.STARTED, State.STOPPED)) {
      main.close();
    }
  }

  /**
   * Closes connection to Zookeeper, in order to execute close, ZK
   * should be stopped.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    stop();
    if (state.compareAndSet(State.STOPPED, State.CLOSED)) {
      InstanceSpecification spec = configBuilder.getInstanceSpec(instanceIndex);
      if (spec.deleteDataDirectoryOnClose()) {
        DirUtils.deleteRecursively(spec.getDataDirectory());
      }
    }
  }

  /**
   * Kills the In memory instance of Zookeeper.
   */
  public void kill() {
    main.kill();
    state.set(State.STOPPED);
  }
}
