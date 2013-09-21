package com.continuuity.common.zookeeper.election;

import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.zookeeper.NodeChildren;
import com.continuuity.weave.zookeeper.OperationFuture;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Performs leader election as specified in
 * <a href="http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection">Zookeeper recipes</a>.
 */
public final class LeaderElection implements Cancellable {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderElection.class);

  private enum State {
    IN_PROGRESS,
    LEADER,
    FOLLOWER
  }

  private final String guid = UUID.randomUUID().toString();

  private final ZKClient zkClient;
  private final String zkFolderPath;
  private final ElectionHandler handler;
  private final ExecutorService executor;
  private String zkNodePath;
  private boolean cancelled;
  private State state;

  public LeaderElection(ZKClient zkClient, String prefix, ElectionHandler handler) {
    this.zkClient = zkClient;
    this.zkFolderPath = prefix.startsWith("/") ? prefix : "/" + prefix;
    this.executor = Executors.newSingleThreadExecutor(
                                Threads.createDaemonThreadFactory("leader-election-" + prefix.replace('/', '-')));
    this.handler = handler;

    LOG.info("Using guid {}", guid);

    runElection();
    zkClient.addConnectionWatcher(wrapWatcher(new ConnectionWatcher()));
  }

  @Override
  public void cancel() {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        LOG.info("Cancelling {}", zkNodePath == null ? zkFolderPath + "/" + guid : zkNodePath);
        if (!cancelled) {
          cancelled = true;
          deleteNode();
        }
      }
    });
  }

  private byte[] getNodeData() {
    String hostname;
    try {
      hostname = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception e) {
      LOG.warn("Failed to get local hostname.", e);
      hostname = "unknown";
    }
    return hostname.getBytes(Charsets.UTF_8);
  }

  /**
   * Creates an ephemeral, sequential node in zookeeper to represent this participant in the election.
   * This method is only called from {@link #runElection()} (after verifying the node does not exist in zookeeper) to
   * prevent multiple node creation due to race.
   */
  private void register() {
    if (cancelled) {
      return;
    }

    zkNodePath = null;

    // Register for election
    final String path = String.format("%s/%s-", zkFolderPath, guid);
    LOG.debug("Registering for election {} with path {}", zkFolderPath, path);

    // Create needs to be sync operation to prevent multiple nodes getting created due to race condition.
    OperationFuture<String> createFuture = zkClient.create(path, getNodeData(), CreateMode.EPHEMERAL_SEQUENTIAL, true);

    try {
      String result = createFuture.get();
      LOG.debug("Created zk node {}", result);
      zkNodePath = result;

      runElection();

    } catch (InterruptedException e) {
      LOG.error("Got exception during node creation for folder {}", path, e);
      Thread.currentThread().interrupt();

    } catch (ExecutionException e) {
      LOG.error("Got exception during node creation for folder {}", path, e);
      zkNodePath = null;
      runElection();
    }
  }

  private void runElection() {
    if (cancelled) {
      return;
    }

    LOG.debug("Running election for {}", zkNodePath == null ? zkFolderPath + "/" + guid : zkNodePath);
    state = State.IN_PROGRESS;

    OperationFuture<NodeChildren> childrenFuture = zkClient.getChildren(zkFolderPath);
    Futures.addCallback(childrenFuture, wrapCallback(new FutureCallback<NodeChildren>() {
      @Override
      public void onSuccess(NodeChildren result) {
        if (cancelled && state != State.IN_PROGRESS) {
          return;
        }

        NodeFunction nodeFunction = new NodeFunction();
        List<ZkNode> childPaths = Lists.newArrayList(Iterables.transform(result.getChildren(), nodeFunction));
        Collections.sort(childPaths);

        int pathIdx = -1;
        if (nodeFunction.getSelfNode() != null) {
          zkNodePath = zkFolderPath + "/" +  nodeFunction.getSelfNode().getPath();
          pathIdx = Collections.binarySearch(childPaths, nodeFunction.getSelfNode());
        }

        // If cannot find the node supposed to be created by this participant, restart from beginning.
        if (pathIdx < 0) {
          register();
          return;
        }

        if (pathIdx == 0) {
          // This is leader
          becomeLeader();
          return;
        }

        // Watch for deletion of largest node smaller than current node
        watchNode(zkFolderPath + "/" + childPaths.get(pathIdx - 1).getPath(), new LowerNodeWatcher());
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException.NoNodeException) {
          LOG.debug("Got exception during children fetch for {}. Retry.", zkFolderPath, t);
          register();
          return;
        }

        LOG.warn("Got exception during children fetch for {}. Retry.", zkFolderPath, t);
        runElection();
      }
    }));
  }

  private void becomeLeader() {
    state = State.LEADER;
    LOG.debug("Become leader for {}.", zkNodePath);
    handler.leader();
  }

  private void becomeFollower() {
    state = State.FOLLOWER;
    LOG.debug("Become follower for {}", zkNodePath);
    handler.follower();
  }

  private void watchNode(final String nodePath, Watcher watcher) {
    state = State.FOLLOWER;
    OperationFuture<Stat> watchFuture = zkClient.exists(nodePath, watcher);
    Futures.addCallback(watchFuture, wrapCallback(new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        becomeFollower();
        LOG.debug("{} watching {}", zkNodePath, nodePath);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.warn("Exception while setting watch on node {}. Retry.", nodePath, t);
        runElection();
      }
    }));
  }

  private void deleteNode() {
    if (state == State.LEADER) {
      becomeFollower();
    }

    if (zkNodePath != null) {
      Futures.addCallback(zkClient.delete(zkNodePath), wrapCallback(new FutureCallback<String>() {
        @Override
        public void onSuccess(String result) {
          LOG.debug("Node deleted: {}", result);
        }

        @Override
        public void onFailure(Throwable t) {
          LOG.warn("Fail to delete node: {}", zkNodePath);
          if (!(t instanceof KeeperException.NoNodeException)) {
            LOG.debug("Retry delete node: {}", zkNodePath);
            deleteNode();
          }
        }
      }));
    }
  }


  private <V> FutureCallback<V> wrapCallback(final FutureCallback<V> callback) {
    return new FutureCallback<V>() {
      @Override
      public void onSuccess(final V result) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            callback.onSuccess(result);
          }
        });
      }

      @Override
      public void onFailure(final Throwable t) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            callback.onFailure(t);
          }
        });
      }
    };
  }

  private Watcher wrapWatcher(final Watcher watcher) {
    return new Watcher() {
      @Override
      public void process(final WatchedEvent event) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            watcher.process(event);
          }
        });
      }
    };
  }

  /**
   * Watches lower node.
   */
  private class LowerNodeWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == Event.EventType.NodeDeleted) {
        LOG.debug("Lower node deleted {} for election {}", event, zkNodePath);
        runElection();
      }
    }
  }

  /**
   * Watches zookeeper connection.
   */
  private class ConnectionWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      switch (event.getState()) {
        case Disconnected:
          LOG.info("ZK session disconnected: {} for {}", zkClient.getConnectString(), zkFolderPath);
          if (state == State.LEADER) {
            becomeFollower();
          }
        break;
        case SyncConnected:
          LOG.info("ZK session connected: {} for {}", zkClient.getConnectString(), zkFolderPath);
          runElection();
        break;
      }
    }
  }

  /**
   * Represents a zookeeper sequence path with path and sequence id.
   */
  private static final class ZkNode implements Comparable<ZkNode> {
    private final String path;
    private final long seqId;

    private ZkNode(String path) {
      this.path = path;
      this.seqId = Integer.parseInt(path.substring(path.lastIndexOf('-') + 1));
    }

    public long getSeqId() {
      return seqId;
    }

    public String getPath() {
      return path;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ZkNode zkNode = (ZkNode) o;

      return seqId == zkNode.seqId;

    }

    @Override
    public int hashCode() {
      return (int) (seqId ^ (seqId >>> 32));
    }

    @Override
    public int compareTo(ZkNode zkNode) {
      return seqId < zkNode.getSeqId() ? -1 : seqId > zkNode.getSeqId() ? 1 : 0;
    }
  }

  /**
   * Converts zookeeper sequence path into ZkNode, and also remembers the ZkNode for self.
   */
  private final class NodeFunction implements Function<String, ZkNode> {
    private ZkNode selfNode;

    @Override
    public ZkNode apply(String input) {
      ZkNode zkNode = new ZkNode(input);
      if (input.startsWith(guid)) {
        selfNode = zkNode;
      }

      return zkNode;
    }

    public ZkNode getSelfNode() {
      return selfNode;
    }
  }
}
