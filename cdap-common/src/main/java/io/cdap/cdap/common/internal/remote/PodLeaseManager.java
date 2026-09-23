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

/**
 * PodLeaseManager handles lock-free Task Worker lease affinity based on logical namespaces.
 * It encapsulates the registry of active Task Worker pods and the logic to allocate or claim pods 
 * for incoming AppFabric requests without exceeding configured concurrency limits.
 */
public class PodLeaseManager {

    private static final Logger LOG = LoggerFactory.getLogger(PodLeaseManager.class);

    private final Map<String, PodState> podRegistry;
    private final int maxConcurrentTasks;

    public PodLeaseManager(CConfiguration cConf) {
        this.podRegistry = new ConcurrentHashMap<>();
        this.maxConcurrentTasks = cConf.getInt(Constants.TaskWorker.REQUEST_LIMIT, 10);
    }

    /**
     * Synchronizes the active discovered pods with the routing registry.
     * Registers newly discovered pods and prunes terminated pods.
     */
    public void syncDiscovery(Iterable<Discoverable> discoverables) {
        Set<String> activePods = new HashSet<>();
        for (Discoverable d : discoverables) {
            activePods.add(d.getSocketAddress().getHostString() + ":" + d.getSocketAddress().getPort());
        }

        for (String podIp : activePods) {
            podRegistry.putIfAbsent(podIp, new PodState(null, 0));
        }

        // Prune pods that discovery no longer reports, but only once they have drained.
        //
        // Dropping a PodState while requests are still in flight loses the accounting for them:
        // releaseLease looks the address up and silently does nothing when it is gone, so the
        // decrement never lands. If the pod then reappears - a brief Endpoints flap, a rolling
        // restart, a momentarily empty discovery response - it is re-registered with zero inflight
        // while its earlier requests are still running, and the proxy over-subscribes it until
        // those pile up into 409s and 429s. Keeping the entry until it drains costs one stale map
        // entry; the next sync after the last response removes it.
        podRegistry.entrySet().removeIf(entry ->
            !activePods.contains(entry.getKey()) && entry.getValue().getInflightRequests() == 0);
        
        LOG.debug("PodLeaseManager synced leases: [{}]",
            podRegistry.entrySet().stream()
                .map(e -> e.getKey() + "="
                    + (e.getValue().getLeasedNamespace() == null
                    ? "null" : e.getValue().getLeasedNamespace() + "_" + e.getValue().getInflightRequests()))
                .collect(java.util.stream.Collectors.joining(", ")));
    }

    /**
     * Acquires a lease for the given namespace by first attempting a warm match, 
     * and if full, attempting to claim an idle pod.
     * 
     * @return The IP:Port address of the leased worker pod, or null if the cluster is full.
     */
    public String acquireLease(String namespace) {
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
    public void releaseLease(String workerAddress) {
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
     * <p>Releasing the slot first and only then dropping a pod that has reached zero keeps a
     * genuinely dead pod out of rotation without corrupting anyone else's accounting.
     */
    public void invalidateLease(String workerAddress) {
        PodState state = podRegistry.get(workerAddress);
        if (state == null) {
            return;
        }
        state.decrementInflightRequests();
        if (state.getInflightRequests() == 0) {
            // Two-argument remove so a pod re-registered by a concurrent syncDiscovery is not
            // clobbered: it only removes the mapping if this exact PodState is still installed.
            podRegistry.remove(workerAddress, state);
        }
    }
    
    /** Intended primarily for testing. */
    public Map<String, PodState> getRegistry() {
        return podRegistry;
    }
}
