/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.zookeeper.election;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Cancellable;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * A service for watching changes in the leader-election participants.
 */
public class LeaderElectionInfoService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionInfoService.class);

  private final ZKClient zkClient;
  private final String leaderElectionPath;
  private final SettableFuture<CountDownLatch> readyFuture;
  private final ConcurrentNavigableMap<Integer, Participant> participants;
  private Cancellable cancellable;

  public LeaderElectionInfoService(ZKClient zkClient, String leaderElectionPath) {
    this.zkClient = zkClient;
    this.leaderElectionPath = leaderElectionPath.startsWith("/") ? leaderElectionPath : "/" + leaderElectionPath;
    this.readyFuture = SettableFuture.create();
    this.participants = new ConcurrentSkipListMap<>();
  }

  /**
   * Gets the map of participants. This method will block until the participants information is fetched
   * for the first time from ZK or timeout happened.
   *
   * @return An immutable {@link SortedMap} ordered by the participant ID with the smallest key in the map
   *         as the current leader. The returned map is thread-safe and will be updated asynchronously
   * @throws InterruptedException if the caller thread is interrupted while waiting for the participants information
   *                              to be available
   * @throws TimeoutException if the wait timed out
   */
  public SortedMap<Integer, Participant> getParticipants(long timeout,
                                                         TimeUnit unit) throws InterruptedException, TimeoutException {
    try {
      Stopwatch stopwatch = new Stopwatch().start();
      CountDownLatch readyLatch = readyFuture.get(timeout, unit);
      long latchTimeout = Math.max(0, stopwatch.elapsedTime(unit) - timeout);
      readyLatch.await(latchTimeout, unit);
    } catch (ExecutionException e) {
      // The ready future never throw on get. If this happen, just return an empty map
      return ImmutableSortedMap.of();
    }
    return Collections.unmodifiableSortedMap(participants);
  }

  /**
   * Fetches the latest participants from ZK. This method will block until it fetched all participants information.
   * Note that the map returned is only a snapshot of the leader election information in ZK, which only reflects
   * the states in ZK at the time when the snapshot was taken.
   *
   * @return An immutable {@link SortedMap} ordered by the participant ID with the smallest key in the map
   *         as the current leader
   * @throws InterruptedException if the caller thread is interrupted while waiting for the participants information
   *                              to be available
   * @throws Exception if failed to fetch information from ZK
   */
  public SortedMap<Integer, Participant> fetchCurrentParticipants() throws Exception {
    try {
      NodeChildren nodeChildren = zkClient.getChildren(leaderElectionPath).get();

      ConcurrentNavigableMap<Integer, Participant> result = new ConcurrentSkipListMap<>();
      SettableFuture<CountDownLatch> completion = SettableFuture.create();
      childrenUpdated(nodeChildren, result, completion);

      completion.get().await();
      return Collections.unmodifiableSortedMap(result);

    } catch (ExecutionException e) {
      // If the election path doesn't exists, that means there is no participant
      Throwable cause = e.getCause();
      if (cause instanceof KeeperException.NoNodeException) {
        return ImmutableSortedMap.of();
      }
      Throwables.propagateIfPossible(cause, Exception.class);
      // Shouldn't reach here as we propagate any Exception/RuntimeException/Error already
      return ImmutableSortedMap.of();
    }
  }

  @Override
  protected void startUp() throws Exception {
    cancellable = ZKOperations.watchChildren(zkClient, leaderElectionPath, new ZKOperations.ChildrenCallback() {
      @Override
      public void updated(NodeChildren nodeChildren) {
        childrenUpdated(nodeChildren, participants, readyFuture);
      }
    });
  }

  @Override
  protected void shutDown() throws Exception {
    if (cancellable != null) {
      cancellable.cancel();
    }
  }

  private void childrenUpdated(NodeChildren nodeChildren,
                               ConcurrentNavigableMap<Integer, Participant> participants,
                               SettableFuture<CountDownLatch> readyFuture) {
    if (!isRunning()) {
      return;
    }

    Map<Integer, String> childIdNodes = new HashMap<>();

    for (String child : nodeChildren.getChildren()) {
      int idx = child.lastIndexOf("-");
      if (idx < 0) {
        // This is not expected as ZK nodes created by LeaderElection always has "-" to separate the node seq no.
        LOG.warn("Ignoring child node {} due to un-recognized format. Expected to be [guid]-[integer]", child);
        continue;
      }

      int id;
      try {
        id = Integer.parseInt(child.substring(idx + 1));
        childIdNodes.put(id, child);
        participants.putIfAbsent(id, new Participant(leaderElectionPath + "/" + child, null));
      } catch (NumberFormatException e) {
        LOG.warn("Ignoring child node {} due to un-recognized format. Expected to be [guid]-[integer]", child);
      }
    }

    // If no participants, just clear the participants map and return
    if (childIdNodes.isEmpty()) {
      participants.clear();
      readyFuture.set(new CountDownLatch(0));
      return;
    }

    // Prepare the ready latch and set it to ready future if it wasn't set before
    CountDownLatch readyLatch = null;
    if (!readyFuture.isDone()) {
      readyLatch = new CountDownLatch(childIdNodes.size());
      readyFuture.set(readyLatch);
    }

    // Remove participants that are gone
    Iterator<Integer> iterator = participants.keySet().iterator();
    while (iterator.hasNext()) {
      if (!childIdNodes.containsKey(iterator.next())) {
        iterator.remove();
      }
    }

    // Fetch each child node
    for (Map.Entry<Integer, String> entry : childIdNodes.entrySet()) {
      fetchParticipant(entry.getValue(), entry.getKey(), participants, readyLatch);
    }
  }

  private void fetchParticipant(String participantNode, final int participantId,
                                final ConcurrentNavigableMap<Integer, Participant> participants,
                                @Nullable final CountDownLatch readyLatch) {
    final String path = leaderElectionPath + "/" + participantNode;
    final Participant oldInfo = participants.get(participantId);

    Futures.addCallback(zkClient.getData(path), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        byte[] data = result.getData();
        String hostname = (data == null) ? null : new String(data, StandardCharsets.UTF_8);
        Participant newInfo = new Participant(path, hostname);
        participants.replace(participantId, oldInfo, newInfo);
        if (readyLatch != null) {
          readyLatch.countDown();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException.NoNodeException) {
          // It's possible the node is gone
          participants.remove(participantId, oldInfo);
        } else {
          // Just log info and replace the participant info with the UNKNOWN_INFO
          LOG.info("Failed in fetching participant information from {}", path);
          participants.replace(participantId, oldInfo, new Participant(path, null));
        }

        if (readyLatch != null) {
          readyLatch.countDown();
        }
      }
    });
  }

  /**
   * Class representing a participant in leader-election.
   */
  public static final class Participant {

    private final String zkPath;
    private final String hostname;

    Participant(String zkPath, @Nullable String hostname) {
      this.zkPath = zkPath;
      this.hostname = hostname;
    }

    public String getZkPath() {
      return zkPath;
    }

    @Nullable
    public String getHostname() {
      return hostname;
    }

    @Override
    public String toString() {
      return "ParticipantInfo{" +
        "zkPath='" + zkPath + '\'' +
        ", hostname='" + hostname + '\'' +
        '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Participant that = (Participant) o;
      return Objects.equals(zkPath, that.zkPath) && Objects.equals(hostname, that.hostname);
    }

    @Override
    public int hashCode() {
      return Objects.hash(zkPath, hostname);
    }
  }
}
