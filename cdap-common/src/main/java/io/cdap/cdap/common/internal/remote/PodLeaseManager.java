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

package io.cdap.cdap.common.internal.remote;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Lock-free table of task worker pods and the namespace each is leased to.
 */
class PodLeaseManager {

    private static final Logger LOG = LoggerFactory.getLogger(PodLeaseManager.class);

    private final Map<String, PodState> podRegistry;
    private final int maxConcurrentTasks;
    private final LongSupplier nanoClock;

    PodLeaseManager(CConfiguration cConf) {
        this(cConf, System::nanoTime);
    }

    /** Takes the clock that backoffs are measured against, so tests can control it. */
    PodLeaseManager(CConfiguration cConf, LongSupplier nanoClock) {
        this.podRegistry = new ConcurrentHashMap<>();
        this.maxConcurrentTasks = cConf.getInt(Constants.TaskWorker.REQUEST_LIMIT, 10);
        this.nanoClock = nanoClock;
    }

    /**
     * Synchronizes the active discovered pods with the routing registry.
     * Registers newly discovered pods and prunes terminated pods.
     */
    void syncDiscovery(Iterable<Discoverable> discoverables) {
        Map<String, String> activePods = new HashMap<>();
        for (Discoverable d : discoverables) {
            activePods.put(d.getSocketAddress().getHostString() + ":" + d.getSocketAddress().getPort(),
                nodeNameOf(d));
        }

        boolean changed = false;
        for (Map.Entry<String, String> pod : activePods.entrySet()) {
            changed |= podRegistry.putIfAbsent(pod.getKey(), new PodState(null, 0, pod.getValue())) == null;
        }

        // Evict pods that discovery no longer reports, even with requests in flight. If a
        // partitioned node heals mid-task, the worker's 429 guard absorbs the over-send.
        changed |= podRegistry.keySet().retainAll(activePods.keySet());

        // Runs on every request, so only log when the pod set changed.
        if (changed && LOG.isDebugEnabled()) {
            LOG.debug("PodLeaseManager pod set changed, leases: [{}]",
                podRegistry.entrySet().stream()
                    .map(e -> e.getKey() + "@" + e.getValue().getNodeName() + "="
                        + (e.getValue().getLeasedNamespace() == null
                        ? "null" : e.getValue().getLeasedNamespace() + "_" + e.getValue().getInflightRequests()))
                    .collect(Collectors.joining(", ")));
        }
    }

    /**
     * Takes a slot on a pod for {@code namespace}: a warm pod already leased to it first, else it
     * leases a fresh pod, then an idle one. Warm matches break ties on node load; fresh claims and
     * steals prefer the least-loaded node.
     *
     * @return the IP:Port address of the pod, or null if every pod is full or leased elsewhere
     */
    String acquireSlot(String namespace) {
        // Counts are snapshotted because concurrent requests change them mid-sort.
        List<Candidate> candidates = snapshotCandidates();

        // STEP 1: Warm match on the least-loaded pod already leased to this namespace.
        List<Candidate> warmPods = new ArrayList<>();
        for (Candidate candidate : candidates) {
            if (namespace.equals(candidate.leasedNamespace) && candidate.podInflight < maxConcurrentTasks) {
                warmPods.add(candidate);
            }
        }
        warmPods.sort(Comparator.<Candidate>comparingInt(c -> c.podInflight)
            .thenComparingInt(c -> c.nodeInflight));
        for (Candidate candidate : warmPods) {
            if (candidate.state.tryAcquireWarmLease(namespace, maxConcurrentTasks)) {
                LOG.info("PodLeaseManager: Found warm match for '{}' at {} on {}. Occupancy: {}",
                         namespace, candidate.address, candidate.state.getNodeName(),
                         candidate.state.getInflightRequests());
                return candidate.address;
            }
        }

        // STEP 2: Fresh claim of a never-leased pod on the least-loaded node.
        List<Candidate> freshPods = new ArrayList<>();
        for (Candidate candidate : candidates) {
            if (candidate.leasedNamespace == null && candidate.podInflight == 0) {
                freshPods.add(candidate);
            }
        }
        freshPods.sort(Comparator.comparingInt(c -> c.nodeInflight));
        for (Candidate candidate : freshPods) {
            if (candidate.state.tryClaimFreshLease(namespace)) {
                LOG.info("PodLeaseManager: Claimed fresh unleased pod for '{}' at {} on {} (node in-flight {})",
                         namespace, candidate.address, candidate.state.getNodeName(), candidate.nodeInflight);
                return candidate.address;
            }
        }

        // STEP 3: Idle steal on the least-loaded node, longest-inactive pod (LRU) first.
        List<Candidate> idlePods = new ArrayList<>();
        for (Candidate candidate : candidates) {
            if (candidate.podInflight == 0) {
                idlePods.add(candidate);
            }
        }
        idlePods.sort(Comparator.<Candidate>comparingInt(c -> c.nodeInflight)
            .thenComparingLong(c -> c.lastActivityTime));
        for (Candidate candidate : idlePods) {
            if (candidate.state.tryStealIdleLease(namespace)) {
                LOG.info("PodLeaseManager: Stealing idle lease for '{}' at {} on {} (node in-flight {})",
                         namespace, candidate.address, candidate.state.getNodeName(), candidate.nodeInflight);
                return candidate.address;
            }
        }

        return null;
    }

    /**
     * Snapshots every available pod with its own and its node's in-flight counts. Pods that are
     * backing off are left out, but their in-flight tasks still count toward their node.
     */
    private List<Candidate> snapshotCandidates() {
        long now = nanoClock.getAsLong();
        List<Candidate> candidates = new ArrayList<>(podRegistry.size());
        Map<String, Integer> nodeInflight = new HashMap<>();
        for (Map.Entry<String, PodState> entry : podRegistry.entrySet()) {
            Candidate candidate = new Candidate(entry.getKey(), entry.getValue());
            nodeInflight.merge(candidate.nodeKey, candidate.podInflight, Integer::sum);
            if (entry.getValue().isAvailable(now)) {
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

    /** Point-in-time view of one pod, so sorting never sees counts change. */
    private static final class Candidate {
        final String address;
        final PodState state;
        final String leasedNamespace;
        final int podInflight;
        final long lastActivityTime;
        // A pod on an unknown node counts as its own node.
        final String nodeKey;
        int nodeInflight;

        Candidate(String address, PodState state) {
            this.address = address;
            this.state = state;
            String namespace = state.getLeasedNamespace();
            this.leasedNamespace = namespace == null || namespace.isEmpty() ? null : namespace;
            this.podInflight = state.getInflightRequests();
            this.lastActivityTime = state.getLastActivityTime();
            this.nodeKey = state.getNodeName() == null ? address : state.getNodeName();
        }
    }

    /**
     * Releases one slot on the given worker pod. The pod stays leased to its namespace; eviction
     * is left to {@link #syncDiscovery}.
     */
    void releaseSlot(String workerAddress) {
        PodState state = podRegistry.get(workerAddress);
        if (state != null) {
            state.decrementInflightRequests();
        }
    }

    /**
     * Releases one slot and skips the pod until its backoff expires. Used when the pod refuses a
     * connection or is draining to restart, since discovery keeps listing it until it's back.
     */
    void markUnavailable(String workerAddress, String reason) {
        PodState state = podRegistry.get(workerAddress);
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

    /** Clears any backoff on the pod, since a connection to it just succeeded. */
    void markReachable(String workerAddress) {
        PodState state = podRegistry.get(workerAddress);
        if (state != null) {
            state.markReachable();
        }
    }

    /** Intended primarily for testing. */
    Map<String, PodState> getRegistry() {
        return podRegistry;
    }
}
