package com.continuuity.common.zookeeper.election;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Performs leader election as specified in
 * <a href="http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection">Zookeeper recipes</a>.
 *
 * It will enter the leader election process when {@link #start()} is called and leave the process when
 * {@link #stop()} is invoked.
 */
public final class LeaderElection extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderElection.class);

  private enum State {
    IN_PROGRESS,
    LEADER,
    FOLLOWER,
    CANCELLED
  }

  private final String guid;

  private final ZKClient zkClient;
  private final String zkFolderPath;
  private final ElectionHandler handler;

  private ExecutorService executor;
  private String zkNodePath;
  private State state;

  public LeaderElection(ZKClient zkClient, String prefix, ElectionHandler handler) {
    this.guid = UUID.randomUUID().toString();
    this.zkClient = zkClient;
    this.zkFolderPath = prefix.startsWith("/") ? prefix : "/" + prefix;
    this.handler = handler;
  }

  @Override
  protected void doStart() {
    LOG.info("Start leader election on {}{} with guid {}", zkClient.getConnectString(), zkFolderPath, guid);

    executor = Executors.newSingleThreadExecutor(
      Threads.createDaemonThreadFactory("leader-election" + zkFolderPath.replace('/', '-')));

    executor.execute(new Runnable() {
      @Override
      public void run() {
        register();
        LeaderElection.this.zkClient.addConnectionWatcher(wrapWatcher(new ConnectionWatcher()));
      }
    });
    notifyStarted();
  }

  @Override
  protected void doStop() {
    final SettableFuture<String> completion = SettableFuture.create();
    Futures.addCallback(completion, new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        notifyStopped();
      }

      @Override
      public void onFailure(Throwable t) {
        notifyFailed(t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    executor.execute(new Runnable() {
      @Override
      public void run() {
        if (state != State.CANCELLED) {
          // becomeFollower has to be called before deleting node to make sure no two active leader.
          if (state == State.LEADER) {
            becomeFollower();
          }
          state = State.CANCELLED;
          doDeleteNode(completion);
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

  private void register() {
    state = State.IN_PROGRESS;
    zkNodePath = null;

    // Register for election
    final String path = String.format("%s/%s-", zkFolderPath, guid);
    LOG.debug("Registering for election {} with path {}", zkFolderPath, path);

    OperationFuture<String> createFuture = zkClient.create(path, getNodeData(), CreateMode.EPHEMERAL_SEQUENTIAL, true);
    Futures.addCallback(createFuture, new FutureCallback<String>() {

      @Override
      public void onSuccess(String result) {
        LOG.debug("Created zk node {}", result);
        zkNodePath = result;
        if (state == State.CANCELLED) {
          // If cancel was called after create(), but before callback trigger, delete the node created.
          deleteNode();
        } else {
          runElection();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Got exception during node creation for folder {}", path, t);
        // The node may created successfully on server and then server crash,
        // which client might receive failure instead.
        // Not checking for cancel here, as we don't know the zkNodePath.
        // Needs to rely on runElection to handle cancel.
        runElection();
      }
    }, executor);
  }

  private void runElection() {
    LOG.debug("Running election for {}", zkNodePath);

    OperationFuture<NodeChildren> childrenFuture = zkClient.getChildren(zkFolderPath);
    Futures.addCallback(childrenFuture, new FutureCallback<NodeChildren>() {
      @Override
      public void onSuccess(NodeChildren result) {
        Optional<String> nodeToWatch = findNodeToWatch(result.getChildren());

        if (state == State.CANCELLED) {
          deleteNode();
          return;
        }

        if (nodeToWatch == null) {
          // zkNodePath unknown, need to run register.
          register();
          return;
        }

        if (nodeToWatch.isPresent()) {
          // Watch for deletion of largest node smaller than current node
          watchNode(zkFolderPath + "/" + nodeToWatch.get(), new LowerNodeWatcher());
        } else {
          // This is leader
          becomeLeader();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.warn("Got exception during children fetch for {}. Retry.", zkFolderPath, t);
        // If cancel has been called before this callback and the zkNodePath is known, we can simply
        // delete the node. Otherwise, runElection() is needed to determine the zkNodePath if it is cancelled.
        if (state == State.CANCELLED && zkNodePath != null) {
          deleteNode();
        } else {
          runElection();
        }
      }
    }, executor);
  }

  private void becomeLeader() {
    state = State.LEADER;
    LOG.debug("Become leader for {}.", zkNodePath);
    try {
      handler.leader();
    } catch (Throwable t) {
      LOG.warn("Exception thrown when calling leader() method. Withdraw from the leader election process.", t);
      stop();
    }
  }

  private void becomeFollower() {
    state = State.FOLLOWER;
    LOG.debug("Become follower for {}", zkNodePath);
    try {
      handler.follower();
    } catch (Throwable t) {
      LOG.warn("Exception thrown when calling follower() method. Withdraw from the leader election process.", t);
      stop();
    }
  }

  /**
   * Starts watching for the max. of smaller node.
   */
  private void watchNode(final String nodePath, Watcher watcher) {
    OperationFuture<Stat> watchFuture = zkClient.exists(nodePath, watcher);
    Futures.addCallback(watchFuture, new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (state != State.CANCELLED) {
          becomeFollower();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.warn("Exception while setting watch on node {}. Retry.", nodePath, t);
        runElection();
      }
    }, executor);
  }

  private ListenableFuture<String> deleteNode() {
    SettableFuture<String> completion = SettableFuture.create();
    doDeleteNode(completion);
    return completion;
  }

  private void doDeleteNode(final SettableFuture<String> completion) {
    if (zkNodePath == null) {
      completion.set(null);
      return;
    }
    try {
      Futures.addCallback(zkClient.delete(zkNodePath), new FutureCallback<String>() {
        @Override
        public void onSuccess(String result) {
          LOG.debug("Node deleted: {}", result);
          completion.set(result);
        }

        @Override
        public void onFailure(Throwable t) {
          LOG.warn("Fail to delete node: {}", zkNodePath);
          if (!(t instanceof KeeperException.NoNodeException)) {
            LOG.debug("Retry delete node: {}", zkNodePath);
            doDeleteNode(completion);
          } else {
            completion.setException(t);
          }
        }
      }, executor);
    } catch (Throwable t) {
      // If any exception happens when calling delete, treats it as completed with failure.
      completion.setException(t);
    }
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
   * Find the node to watch for and return it in the {@link Optional} value. If this client is
   * the leader, return an {@link Optional#absent()}. This method also tries to set the zkNodePath if
   * it is not set and return {@code null} if the zkNodePath cannot be determined.
   */
  private Optional<String> findNodeToWatch(List<String> nodes) {
    // If this node path is not know, find it first.
    if (zkNodePath == null) {
      for (String node : nodes) {
        if (node.startsWith(guid)) {
          zkNodePath = zkFolderPath + "/" + node;
          break;
        }
      }
    }

    if (zkNodePath == null) {
      // Cannot find the node path, return null
      return null;
    }

    // Find the maximum node that the smaller than node created by this client.
    int currentId = Integer.parseInt(zkNodePath.substring(zkNodePath.indexOf(guid) + guid.length() + 1));
    String nodeToWatch = null;
    int maxOfMins = Integer.MIN_VALUE;
    for (String node : nodes) {
      int nodeId = Integer.parseInt(node.substring(guid.length() + 1));
      if (nodeId < currentId && nodeId > maxOfMins) {
        maxOfMins = nodeId;
        nodeToWatch = node;
      }
    }

    return nodeToWatch == null ? Optional.<String>absent() : Optional.of(nodeToWatch);
  }

  /**
   * Watches lower node.
   */
  private class LowerNodeWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (state != State.CANCELLED && event.getType() == Event.EventType.NodeDeleted) {
        LOG.debug("Lower node deleted {} for election {}.", event, zkNodePath);
        runElection();
      }
    }
  }

  /**
   * Watches zookeeper connection.
   */
  private class ConnectionWatcher implements Watcher {
    private boolean expired;
    private boolean disconnected;

    @Override
    public void process(WatchedEvent event) {
      switch (event.getState()) {
        case Disconnected:
          disconnected = true;
          LOG.info("Disconnected from ZK: {} for {}", zkClient.getConnectString(), zkFolderPath);
          if (state == State.LEADER) {
            // becomeFollower has to be called in disconnect so that no two active leader is possible.
            LOG.info("Stepping down as leader due to disconnect: {} for {}", zkClient.getConnectString(), zkFolderPath);
            becomeFollower();
          }
          break;
        case SyncConnected:
          boolean runElection = disconnected && !expired && state != State.IN_PROGRESS;
          boolean runRegister = disconnected && expired && state != State.IN_PROGRESS;
          disconnected = false;
          expired = false;
          if (runElection) {
            // If the state is cancelled (meaning a cancel happens between disconnect and connect),
            // still runElection() so that it has chance to delete the node (as it's not expired, the node stays
            // after reconnection).
            if (state != State.CANCELLED) {
              state = State.IN_PROGRESS;
            }
            LOG.info("Connected to ZK, running election: {} for {}", zkClient.getConnectString(), zkFolderPath);
            runElection();
          } else if (runRegister && state != State.CANCELLED) {
            LOG.info("Connected to ZK, registering: {} for {}", zkClient.getConnectString(), zkFolderPath);
            register();
          }

          break;
        case Expired:
          LOG.info("ZK session expired: {} for {}", zkClient.getConnectString(), zkFolderPath);
          expired = true;
          break;
      }
    }
  }
}
