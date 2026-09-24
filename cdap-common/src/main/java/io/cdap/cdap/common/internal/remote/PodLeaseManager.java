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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * PodLeaseManager handles lock-free Task Worker lease affinity based on logical namespaces.
 * It encapsulates the registry of active Task Worker pods and the logic to allocate or claim pods 
 * for incoming AppFabric requests without exceeding configured concurrency limits.
 */
class PodLeaseManager {

    private static final Logger LOG = LoggerFactory.getLogger(PodLeaseManager.class);

    private final Map<String, PodState> podRegistry;
    private final int maxConcurrentTasks;

    PodLeaseManager(CConfiguration cConf) {
        this.podRegistry = new ConcurrentHashMap<>();
        this.maxConcurrentTasks = cConf.getInt(Constants.TaskWorker.REQUEST_LIMIT, 10);
    }

    /**
     * Synchronizes the active discovered pods with the routing registry.
     * Registers newly discovered pods and prunes terminated pods.
     */
    void syncDiscovery(Iterable<Discoverable> discoverables) {
        Set<String> activePods = new HashSet<>();
        for (Discoverable d : discoverables) {
            activePods.add(d.getSocketAddress().getHostString() + ":" + d.getSocketAddress().getPort());
        }

        for (String podIp : activePods) {
            PodState existing = podRegistry.putIfAbsent(podIp, new PodState(null, 0));
            if (existing != null) {
                // Relist a pod that dropped out of discovery and came back while it was still
                // draining, for example after its node recovered from a partition. Its counts were
                // kept, so it resumes exactly where it left off.
                existing.setListed(true);
            }
        }

        // Pods that discovery no longer reports stop taking new leases immediately, but their
        // entries are only pruned once they have drained.
        //
        // An address leaves discovery when the worker container restarts in place, when the pod
        // starts terminating, or when its node is marked NotReady. The proxy cannot tell these
        // apart, and only the first one kills the requests running on the pod: a terminating pod
        // keeps serving through its grace period, and a partitioned node can come back Ready at
        // the same address with its tasks still running. Dropping the entry while requests are in
        // flight would lose their accounting, since releaseLease finds nothing to decrement, and
        // a returning address would be re-registered at zero while still busy. Waiting costs
        // little: the entry is unroutable while it drains, and after a container restart it is
        // usually already at zero by the time discovery notices, because the dead process's
        // connections reset before the Endpoints update arrives.
        //
        // Unlisting before reading the count is what makes the read safe: from that point no
        // lease path will add to it. The one remaining window is a concurrent sync, working from
        // a discovery view that still includes this address, relisting it between the read and
        // the remove, with a request leased in between. That needs two contradictory views of the
        // same address at the same instant, and costs one request over the pod's limit, which the
        // worker rejects and the proxy self-heals from.
        for (Map.Entry<String, PodState> entry : podRegistry.entrySet()) {
            if (activePods.contains(entry.getKey())) {
                continue;
            }
            PodState state = entry.getValue();
            state.setListed(false);
            if (state.getInflightRequests() == 0) {
                // Two-argument remove for the same reason as in invalidateLease.
                podRegistry.remove(entry.getKey(), state);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("PodLeaseManager synced leases: [{}]",
                podRegistry.entrySet().stream()
                    .map(e -> e.getKey() + "="
                        + (e.getValue().getLeasedNamespace() == null
                        ? "null" : e.getValue().getLeasedNamespace() + "_" + e.getValue().getInflightRequests())
                        + (e.getValue().isListed() ? "" : " (unlisted)"))
                    .collect(Collectors.joining(", ")));
        }
    }

    /**
     * Acquires a lease for the given namespace by first attempting a warm match, 
     * and if full, attempting to claim an idle pod.
     * 
     * @return The IP:Port address of the leased worker pod, or null if the cluster is full.
     */
    String acquireLease(String namespace) {
        // STEP 1: Warm Match Selection
        for (Map.Entry<String, PodState> entry : podRegistry.entrySet()) {
            if (entry.getValue().tryAcquireWarmLease(namespace, maxConcurrentTasks)) {
                LOG.info("PodLeaseManager: Found warm match for '{}' at {}. Occupancy: {}", 
                         namespace, entry.getKey(), entry.getValue().getInflightRequests());
                return entry.getKey();
            }
        }

        // STEP 2: Fresh Claim (Unleased) - Grab any brand new pod efficiently
        for (Map.Entry<String, PodState> entry : podRegistry.entrySet()) {
            if (entry.getValue().tryClaimFreshLease(namespace)) {
                LOG.info("PodLeaseManager: Claimed fresh unleased pod for '{}' at {}", namespace, entry.getKey());
                return entry.getKey();
            }
        }

        // STEP 3: Idle Steal Selection - Prioritize stealing from the longest-inactive pod (LRU)
        List<Map.Entry<String, PodState>> candidates = new ArrayList<>(podRegistry.entrySet());
        candidates.sort(Comparator.comparingLong(e -> e.getValue().getLastActivityTime()));

        for (Map.Entry<String, PodState> entry : candidates) {
            if (entry.getValue().tryStealIdleLease(namespace)) {
                LOG.info("PodLeaseManager: Stealing idle lease for '{}' at {}", namespace, entry.getKey());
                return entry.getKey();
            }
        }
        
        LOG.warn("PodLeaseManager: Cluster full! No available slots for namespace '{}'", namespace);
        return null;
    }

    /**
     * Releases a lease slot on the specified worker pod.
     */
    void releaseLease(String workerAddress) {
        PodState state = podRegistry.get(workerAddress);
        if (state != null) {
            state.decrementInflightRequests();
        }
    }

    /**
     * Gives back the slot this caller was holding on a worker pod that turned out to be unusable,
     * and evicts the pod once nothing is left running on it.
     *
     * <p>This is deliberately not a blanket {@code podRegistry.remove()}. The registry entry is
     * shared by every connection routed to that address, so removing it on one failed connect
     * throws away the occupancy count for requests other channels are still streaming. Those
     * channels later call {@link #releaseLease}, find nothing, and decrement nothing; the pod then
     * comes back through {@link #syncDiscovery} with a zeroed count and gets over-subscribed,
     * which surfaces as a burst of 409s from a worker the proxy believes is idle.
     *
     * <p>The pod is unlisted before its count is read, so no concurrent request can be leased
     * onto it between the check and the removal. If discovery still reports the address, the next
     * {@link #syncDiscovery} registers it again.
     */
    void invalidateLease(String workerAddress) {
        PodState state = podRegistry.get(workerAddress);
        if (state == null) {
            return;
        }
        state.setListed(false);
        state.decrementInflightRequests();
        if (state.getInflightRequests() == 0) {
            // Two-argument remove: between our get() and here, another path may already have
            // removed this entry and a sync may have registered a fresh PodState for the same
            // address. Removing by key alone would delete that fresh entry, along with any lease
            // already taken on it.
            podRegistry.remove(workerAddress, state);
        }
    }
    
    /** Intended primarily for testing. */
    Map<String, PodState> getRegistry() {
        return podRegistry;
    }
}
