package com.continuuity.common.zookeeper.election;

import com.continuuity.common.zookeeper.election.internal.RegisteredElection;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.zookeeper.NodeChildren;
import com.continuuity.weave.zookeeper.OperationFuture;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Performs simple leader election.
 */
public class SimpleLeaderElectionService extends AbstractIdleService implements LeaderElectionService {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleLeaderElectionService.class);
  private static final String NAMESPACE = "/simple_election";

  private final String guid = UUID.randomUUID().toString();

  private final ZKClient zkClient;
  private final ConcurrentMap<Election, Boolean> activeElections;
  private final ConcurrentMap<RegisteredElection, Boolean> registeredElections;
  private final ConcurrentMap<RegisteredElection, Boolean> leaders;

  // Used to prevent watches from running during shutdown and vice versa
  private final ReadWriteLock shutdownLock = new ReentrantReadWriteLock();

  public SimpleLeaderElectionService(ZKClient zkClient) {
    this(zkClient, NAMESPACE);
  }

  public SimpleLeaderElectionService(ZKClient zkClient, String namespace) {
    this.zkClient = namespace == null ? zkClient : ZKClients.namespace(zkClient, namespace);
    this.activeElections = Maps.newConcurrentMap();
    this.registeredElections = Maps.newConcurrentMap();
    this.leaders = Maps.newConcurrentMap();

    this.zkClient.addConnectionWatcher(new ConnectionWatcher());
    LOG.info("Using guid {}", guid);
  }

  @Override
  protected void startUp() throws Exception {
    // Nothing to do!
  }

  @Override
  protected void shutDown() throws Exception {
    activeElections.clear();

    shutdownLock.writeLock().lock();
    LOG.info("Shutting down SimpleLeaderElectionService...");
    try {
      for (RegisteredElection election : registeredElections.keySet()) {
        unregister(election);
      }
    } finally {
      shutdownLock.writeLock().unlock();
    }
  }

  @Override
  public Cancellable addElection(final Election election) {
    if (activeElections.putIfAbsent(election, true) == null) {
      try {
        doRegister(election);
      } catch (Throwable e) {
        activeElections.remove(election);
        throw Throwables.propagate(e);
      }
    }

    if (!activeElections.containsKey(election)) {
      LOG.warn("Election {} has been cancelled during registration, un-registering it...");
      unregister(election);
    }

    return new Cancellable() {
      @Override
      public void cancel() {
        unregister(election);
      }
    };
  }

  private void doRegister(Election election) {
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

      registeredElections.put(registeredElection, true);
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
          endLeader(registeredElection);
          registeredElections.remove(registeredElection);
        }
      }
      throw Throwables.propagate(e);
    }
  }

  private void unregister(Election election) {
    activeElections.remove(election);

    final RegisteredElection actualElection = getRegisteredElection(election);
    if (actualElection == null || registeredElections.remove(actualElection) == null) {
      return;
    }

    LOG.info("Un-registering {}", actualElection);

    endLeader(actualElection);

  // Delete node
    OperationFuture<String> deleteFuture = zkClient.delete(actualElection.getZkPath());
    Futures.addCallback(deleteFuture, new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        // Nothing to do
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Got exception while deleting path election {}", actualElection, t);
      }
    });
    Futures.getUnchecked(deleteFuture);
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
          zkClient.exists(watchEntry.getValue(), new OtherWatcher(registeredElection));
        Futures.getUnchecked(watchFuture);
      }

      return registeredElection;
    } catch (Exception e) {
      LOG.error("Got exception while running election {}", election, e);
      throw Throwables.propagate(e);
    }
  }

  private void becomeLeader(RegisteredElection election) {
    if (leaders.putIfAbsent(election, true) != null) {
      return;
    }

    LOG.debug("Became leader for {}", election);
    try {
      election.getElectionHandler().elected(election.getId());
    } catch (Throwable e) {
      LOG.error("Election handler threw exception for election {}: ", election, e);
    }
  }

  private void endLeader(RegisteredElection election) {
    if (leaders.remove(election) == null) {
      return;
    }

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

  private RegisteredElection getRegisteredElection(Election election) {
    for (RegisteredElection e : registeredElections.keySet()) {
      if (e.equals(election)) {
        return e;
      }
    }
    return null;
  }

  /**
   * Watches other node.
   */
  private class OtherWatcher implements Watcher {
    private final RegisteredElection election;

    private OtherWatcher(RegisteredElection election) {
      this.election = election;
    }

    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == Event.EventType.NodeDeleted) {
        LOG.debug("Watched node deleted {} for election {}", event, election);

        if (!shutdownLock.readLock().tryLock()) {
          // Shutdown is in progress
          LOG.info("Shutdown in progress, returning from watch");
          return;
        }

        try {
          if (activeElections.containsKey(election)) {
            runElection(election);
          }
        } finally {
          shutdownLock.readLock().unlock();
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
        LOG.debug("Self node deleted {} for election {}", event, election);

        if (!shutdownLock.readLock().tryLock()) {
          // Shutdown is in progress
          LOG.info("Shutdown in progress, returning from watch");
          return;
        }

        try {
          if (activeElections.containsKey(election)) {
            if (leaders.containsKey(election)) {
              endLeader(election);
            }
            doRegister(election);
          }
        } finally {
          shutdownLock.readLock().unlock();
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
        for (RegisteredElection election : leaders.keySet()) {
          endLeader(election);
        }
      } else if (event.getState() == Event.KeeperState.SyncConnected && expired.get()) {
        expired.set(false);
        LOG.info("Reconnected after expiration: {}", zkClient.getConnectString());

        for (Election election : registeredElections.keySet()) {
          if (activeElections.containsKey(election)) {
            doRegister(election);
          }
        }
      }
    }
  }
}
