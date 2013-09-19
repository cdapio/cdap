package com.continuuity.common.zookeeper.election;

import com.continuuity.common.zookeeper.election.internal.RegisteredElection;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.zookeeper.NodeChildren;
import com.continuuity.weave.zookeeper.OperationFuture;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Performs simple leader election.
 */
public class SimpleLeaderElectionService extends AbstractIdleService implements LeaderElectionService {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleLeaderElectionService.class);
  private static final String NAMESPACE = "/simple_election";

  private final String guid = UUID.randomUUID().toString();

  private final ZKClient zkClient;
  private final Set<RegisteredElection> elections;
  private final Set<RegisteredElection> leaders;

  public SimpleLeaderElectionService(ZKClient zkClient) {
    this(zkClient, NAMESPACE);
  }

  public SimpleLeaderElectionService(ZKClient zkClient, String namespace) {
    this.zkClient = namespace == null ? zkClient : ZKClients.namespace(zkClient, namespace);
    this.elections = new CopyOnWriteArraySet<RegisteredElection>();
    this.leaders = new CopyOnWriteArraySet<RegisteredElection>();

    this.zkClient.addConnectionWatcher(new ConnectionWatcher());
    LOG.info("Using guid {}", guid);
  }

  @Override
  protected void startUp() throws Exception {
    // Nothing to do!
  }

  @Override
  protected void shutDown() throws Exception {
    for (RegisteredElection election : elections) {
      unregister(election);
    }
  }

  @Override
  public Cancellable registerElection(Election election) {
    RegisteredElection registeredElection = null;

    //noinspection SuspiciousMethodCalls
    if (!elections.contains(election)) {
      registeredElection = doRegister(election);
      elections.add(registeredElection);
    } else {
      for (RegisteredElection e : elections) {
        if (e.equals(election)) {
          registeredElection = e;
        }
      }
    }

    final RegisteredElection regElection = registeredElection;
    return new Cancellable() {
      @Override
      public void cancel() {
        unregister(regElection);
      }
    };
  }

  private RegisteredElection doRegister(Election election) {
    String zkPath = null;
    RegisteredElection registeredElection = null;

    try {
      // Register for election
      String path = String.format("/%s/%s-", election.getId(), guid);
      LOG.debug("Registering for election {} with path {}", election, path);

      OperationFuture<String> createFuture =
        zkClient.create(path, null, CreateMode.EPHEMERAL_SEQUENTIAL, true);
      try {
        zkPath = Futures.getUnchecked(createFuture);
      } catch (Throwable e) {
        // Only log the exception, if path is not created then another exception is thrown later.
        LOG.error("Got exception while creating path {} for election {}", path, election);
      }

      // run election
      registeredElection = runElection(election);

      OperationFuture<Stat> watchFuture =
        zkClient.exists(registeredElection.getZkPath(), new SelfWatcher(registeredElection));
      Futures.getUnchecked(watchFuture);

      return registeredElection;
    } catch (Throwable e) {
      LOG.error("Exception while registering for election {}", election.getId(), e);
      try {
        if (zkPath != null) {
          Futures.getUnchecked(zkClient.delete(zkPath));
        } else if (registeredElection != null) {
          Futures.getUnchecked(zkClient.delete(registeredElection.getZkPath()));
        }
      } finally {
        if (registeredElection != null) {
          elections.remove(registeredElection);
        }
      }
      throw Throwables.propagate(e);
    }
  }

  private void unregister(RegisteredElection election) {
    LOG.info("Un-registering {}", election);

    elections.remove(election);

    // Delete node
    try {
      OperationFuture<String> deleteFuture = zkClient.delete(election.getZkPath());
      deleteFuture.get();
    } catch (Throwable e) {
      LOG.error("Got exception while deleting path election {}", election, e);
    }
  }

  private RegisteredElection runElection(Election election) {
    String zkpath = "/" + election.getId();

    LOG.debug("Getting children for {}", election);

    try {
      OperationFuture<NodeChildren> childrenFuture = zkClient.getChildren(zkpath);
      List<String> childPaths = childrenFuture.get().getChildren();

      long selfSeqId = -1;
      TreeMap<Long, String> childrenMap = new TreeMap<Long, String>();
      for (String path : childPaths) {
        long seqId = getSequenceId(path);
        LOG.debug("Got child = {}, seqId = {}", path, seqId);
        childrenMap.put(getSequenceId(path), zkpath + "/" + path);

        if (path.startsWith(guid)) {
          LOG.debug("Self path = {}", path);
          selfSeqId = seqId;
        }
      }

      if (selfSeqId == -1) {
        String message = String.format("Cannot find self path after registration for %s", election);
        LOG.error(message);
        throw new IllegalStateException(message);
      }

      RegisteredElection registeredElection = new RegisteredElection(election, selfSeqId, childrenMap.get(selfSeqId));
      LOG.debug("Registered for election {}", registeredElection);

      LOG.debug("Current leader is {}", childrenMap.firstEntry().getValue());

      if (registeredElection.getSeqId() == childrenMap.firstKey()) {
        // elected leader
        becomeLeader(registeredElection);
      } else {
        // watch previous node
        Map.Entry<Long, String> watchEntry = childrenMap.lowerEntry(registeredElection.getSeqId());
        OperationFuture<Stat> watchFuture =
          zkClient.exists(watchEntry.getValue(), new OtherLeaderWatcher(registeredElection));
        Futures.getUnchecked(watchFuture);
      }


      return registeredElection;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void becomeLeader(RegisteredElection election) {
    leaders.add(election);
    LOG.debug("Became leader for {}", election);

    try {
      election.getElectionHandler().elected(election.getId());
    } catch (Throwable e) {
      LOG.error("Election handler threw exception for election {}: ", election, e);
    }
  }

  private void endLeader(RegisteredElection election) {
    leaders.remove(election);
    LOG.debug("End leader for {}", election);

    try {
      election.getElectionHandler().unelected(election.getId());
    } catch (Throwable e) {
      LOG.error("Election handler threw exception for election {}: ", election, e);
    }
  }

  private static long getSequenceId(String zkPath) {
    int ind = zkPath.lastIndexOf('-');

    if (ind == zkPath.length() - 1 || ind == -1) {
      String message = String.format("No sequence ID found in zkPath %s", zkPath);
      LOG.error(message);
      throw new IllegalStateException(message);
    }

    return Long.parseLong(zkPath.substring(ind + 1));
  }

  /**
   * Watches other node.
   */
  private class OtherLeaderWatcher implements Watcher {
    private final RegisteredElection election;

    private OtherLeaderWatcher(RegisteredElection election) {
      this.election = election;
    }

    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == Event.EventType.NodeDeleted) {
        LOG.debug("Watched node deleted {} for election {}", event, election);
        if (elections.contains(election)) {
          runElection(election);
        }
      }
    }
  }

  /**
   * Watches self node.
   */
  private class SelfWatcher implements Watcher {
    private final RegisteredElection election;

    private SelfWatcher(RegisteredElection election) {
      this.election = election;
    }

    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == Event.EventType.NodeDeleted) {
        LOG.debug("Self node deleted {} for election", event, election);
        if (leaders.contains(election)) {
          endLeader(election);
        }
        if (elections.contains(election)) {
          doRegister(election);
        }
      }
    }
  }

  private class ConnectionWatcher implements Watcher {
    private final AtomicBoolean expired = new AtomicBoolean(false);

    @Override
    public void process(WatchedEvent event) {
      if (event.getState() == Event.KeeperState.Expired) {
        expired.set(true);
        LOG.warn("ZK session expired: {}", zkClient.getConnectString());

        // run end leader
        for (RegisteredElection election : leaders) {
          endLeader(election);
        }
      } else if (event.getState() == Event.KeeperState.SyncConnected && expired.get()) {
        expired.set(false);
        LOG.info("Reconnected after expiration: {}", zkClient.getConnectString());

        for (Election election : elections) {
          doRegister(election);
        }
      }
    }
  }
}
