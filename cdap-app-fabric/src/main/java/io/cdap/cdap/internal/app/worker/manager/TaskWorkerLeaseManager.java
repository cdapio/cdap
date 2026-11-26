/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.manager;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lock-free table of task workers and the namespace each is leased to.
 */
class TaskWorkerLeaseManager {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerLeaseManager.class);

  private final Map<String, TaskWorkerStatus> workerRegistry;
  private final int maxConcurrentTasks;
  private final LongSupplier nanoClock;

  /**
   * Creates an empty lease table.
   *
   * @param cConf the configuration to read the per-worker request limit from
   * @param nanoClock the clock that backoffs and activity timestamps are measured against, in the
   *     units of {@link System#nanoTime()}
   */
  public TaskWorkerLeaseManager(CConfiguration cConf, LongSupplier nanoClock) {
    this.workerRegistry = new ConcurrentHashMap<>();
    this.maxConcurrentTasks = cConf.getInt(Constants.TaskWorker.REQUEST_LIMIT);
    this.nanoClock = nanoClock;
  }

  /**
   * Synchronizes the active discovered workers with the routing registry. Registers newly
   * discovered workers and prunes terminated workers.
   */
  void syncDiscovery(Iterable<Discoverable> discoverables) {
    long now = nanoClock.getAsLong();
    Set<String> activeAddresses = new HashSet<>();
    boolean changed = false;
    for (Discoverable d : discoverables) {
      InetSocketAddress address = d.getSocketAddress();
      String workerAddress = address.getHostString() + ":" + address.getPort();
      activeAddresses.add(workerAddress);
      // Runs on every request, so only read the node and allocate for a worker that isn't
      // registered yet.
      if (!workerRegistry.containsKey(workerAddress)) {
        TaskWorkerStatus fresh = new TaskWorkerStatus(null, 0, nodeNameOf(d), now);
        changed |= workerRegistry.putIfAbsent(workerAddress, fresh) == null;
      }
    }

    // Evict workers that discovery no longer reports, even with requests in flight. If a
    // partitioned node heals mid-task, the worker's 429 guard absorbs the over-send.
    changed |= workerRegistry.keySet().retainAll(activeAddresses);

    // Runs on every request, so only log when the worker set changed.
    if (changed) {
      LOG.debug("TaskWorkerLeaseManager worker set changed, leases: [{}]", formatLeases());
    }
  }

  private String formatLeases() {
    return workerRegistry.entrySet().stream()
        .map(e -> {
          TaskWorkerStatus status = e.getValue();
          String lease = status.getLeasedNamespace() == null
              ? "null"
              : status.getLeasedNamespace() + "_" + status.getInflightRequests();
          return e.getKey() + "@" + status.getNodeName() + "=" + lease;
        })
        .collect(Collectors.joining(", "));
  }

  /**
   * Takes a slot on a worker for {@code namespace}: a warm worker already leased to it first, then a
   * fresh worker, then an idle one, preferring the least-loaded node.
   *
   * @return the IP:Port address of the worker, or null if every worker is full or leased elsewhere
   */
  String acquireSlot(String namespace) {
    long now = nanoClock.getAsLong();
    // Counts are snapshotted because concurrent requests change them mid-sort.
    List<Candidate> candidates = snapshotCandidates(namespace, now);

    // STEP 1: Warm match on the least-loaded worker already leased to this namespace.
    List<Candidate> warmWorkers = new ArrayList<>();
    for (Candidate candidate : candidates) {
      if (namespace.equals(candidate.leasedNamespace)
          && candidate.workerInflight < maxConcurrentTasks) {
        warmWorkers.add(candidate);
      }
    }
    warmWorkers.sort(Comparator.<Candidate>comparingInt(c -> c.workerInflight)
        .thenComparingInt(c -> c.nodeInflight));
    for (Candidate candidate : warmWorkers) {
      if (candidate.state.tryAcquireWarmLease(namespace, maxConcurrentTasks, now)) {
        LOG.debug("TaskWorkerLeaseManager: Found warm match for '{}' at {} on {}. Occupancy: {}",
            namespace, candidate.address, candidate.state.getNodeName(),
            candidate.state.getInflightRequests());
        return candidate.address;
      }
    }

    // STEP 2: Fresh claim of a never-leased worker on the least-loaded node.
    List<Candidate> freshWorkers = new ArrayList<>();
    for (Candidate candidate : candidates) {
      if (candidate.leasedNamespace == null && candidate.workerInflight == 0) {
        freshWorkers.add(candidate);
      }
    }
    freshWorkers.sort(Comparator.comparingInt(c -> c.nodeInflight));
    for (Candidate candidate : freshWorkers) {
      if (candidate.state.tryClaimFreshLease(namespace, maxConcurrentTasks, now)) {
        LOG.debug(
            "TaskWorkerLeaseManager: Claimed fresh unleased worker for '{}' at {} on {} "
                + "(node in-flight {})",
            namespace, candidate.address, candidate.state.getNodeName(), candidate.nodeInflight);
        return candidate.address;
      }
    }

    // STEP 3: Idle steal on the least-loaded node, longest-inactive worker (LRU) first.
    List<Candidate> idleWorkers = new ArrayList<>();
    for (Candidate candidate : candidates) {
      if (candidate.workerInflight == 0) {
        idleWorkers.add(candidate);
      }
    }
    idleWorkers.sort(Comparator.<Candidate>comparingInt(c -> c.nodeInflight)
        .thenComparingLong(c -> c.lastActivityTime));
    for (Candidate candidate : idleWorkers) {
      if (candidate.state.tryStealIdleLease(namespace, maxConcurrentTasks, now)) {
        LOG.debug(
            "TaskWorkerLeaseManager: Stealing idle lease for '{}' at {} on {} (node in-flight {})",
            namespace, candidate.address, candidate.state.getNodeName(), candidate.nodeInflight);
        return candidate.address;
      }
    }

    return null;
  }

  /**
   * Snapshots every worker available to {@code namespace} with its own and its node's in-flight
   * counts. Unavailable workers are left out, but still count toward their node.
   */
  private List<Candidate> snapshotCandidates(String namespace, long now) {
    List<Candidate> candidates = new ArrayList<>(workerRegistry.size());
    Map<String, Integer> nodeInflight = new HashMap<>();
    for (Map.Entry<String, TaskWorkerStatus> entry : workerRegistry.entrySet()) {
      TaskWorkerStatusSnapshot snapshot = entry.getValue().snapshot();
      Candidate candidate = new Candidate(entry.getKey(), entry.getValue(), snapshot);
      nodeInflight.merge(candidate.nodeKey, candidate.workerInflight, Integer::sum);
      if (snapshot.isAvailableFor(namespace, now)) {
        candidates.add(candidate);
      }
    }
    for (Candidate candidate : candidates) {
      candidate.nodeInflight = nodeInflight.get(candidate.nodeKey);
    }
    return candidates;
  }

  /** Returns the node name carried in the discoverable payload, or {@code null} if absent. */
  @Nullable
  private static String nodeNameOf(Discoverable discoverable) {
    byte[] payload = discoverable.getPayload();
    if (payload == null || payload.length == 0) {
      return null;
    }
    return new String(payload, StandardCharsets.UTF_8);
  }

  /** Point-in-time view of one worker, so sorting never sees counts change. */
  private static final class Candidate {
    final String address;
    final TaskWorkerStatus state;
    final String leasedNamespace;
    final int workerInflight;
    final long lastActivityTime;
    // A worker on an unknown node counts as its own node.
    final String nodeKey;
    int nodeInflight;

    Candidate(String address, TaskWorkerStatus state, TaskWorkerStatusSnapshot snapshot) {
      this.address = address;
      this.state = state;
      String namespace = snapshot.leasedNamespace;
      this.leasedNamespace = namespace == null || namespace.isEmpty() ? null : namespace;
      this.workerInflight = snapshot.inflightRequests;
      this.lastActivityTime = snapshot.lastActivityTime;
      this.nodeKey = state.getNodeName() == null ? address : state.getNodeName();
    }
  }

  /**
   * Releases one slot on the given worker. The worker stays leased to its namespace; eviction is
   * left to {@link #syncDiscovery}.
   */
  void releaseSlot(String workerAddress) {
    TaskWorkerStatus state = workerRegistry.get(workerAddress);
    if (state != null) {
      state.decrementInflightRequests(nanoClock.getAsLong());
    }
  }

  /**
   * Releases one slot after a worker's 429 to {@code requestNamespace}, adopts the namespace it
   * reports and skips the worker for {@link TaskWorkerStatus#REJECTION_SKIP_NANOS}.
   */
  void markRejected(String workerAddress, @Nullable String leasedNamespace,
      String requestNamespace) {
    TaskWorkerStatus state = workerRegistry.get(workerAddress);
    if (state == null) {
      return;
    }
    TaskWorkerStatusSnapshot updated =
        state.markRejected(leasedNamespace, requestNamespace, nanoClock.getAsLong());
    LOG.debug("Cooling down task worker {} for {}s after it rejected namespace {}. Lease "
            + "attributed to namespace {}, occupancy {}, open to the lease holder: {}.",
        workerAddress, TimeUnit.NANOSECONDS.toSeconds(TaskWorkerStatus.REJECTION_SKIP_NANOS),
        requestNamespace, updated.leasedNamespace, updated.inflightRequests,
        updated.cooldownExemptsHolder);
  }

  /** Marks the worker as recently active when a non-rejection response arrives. */
  void recordActivity(String workerAddress) {
    TaskWorkerStatus state = workerRegistry.get(workerAddress);
    if (state != null) {
      state.recordActivity(nanoClock.getAsLong());
    }
  }

  /**
   * Releases one slot and skips the worker until its backoff expires, since discovery keeps listing
   * a worker that refuses connections, fails the TLS handshake or is terminating.
   */
  void markUnavailable(String workerAddress, String reason) {
    TaskWorkerStatus state = workerRegistry.get(workerAddress);
    if (state == null) {
      return;
    }
    long backoff = state.markUnavailable(nanoClock.getAsLong());
    if (backoff == 0) {
      return;
    }
    LOG.info("Skipping task worker {} for {}s after {}.", workerAddress,
        TimeUnit.NANOSECONDS.toSeconds(backoff), reason);
  }

  /** Clears any backoff on the worker, since a connection (and TLS handshake, if enabled) succeeded. */
  void markReachable(String workerAddress) {
    TaskWorkerStatus state = workerRegistry.get(workerAddress);
    if (state != null) {
      state.markReachable();
    }
  }

  /** Intended primarily for testing. */
  Map<String, TaskWorkerStatus> getRegistry() {
    return workerRegistry;
  }
}
