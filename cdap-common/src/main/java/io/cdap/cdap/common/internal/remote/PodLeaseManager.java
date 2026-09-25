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
            podRegistry.putIfAbsent(podIp, new PodState(null, 0));
        }

        // Evict pods that discovery no longer reports, even with requests in flight. If a
        // partitioned node heals mid-task, the worker's 429 guard absorbs the over-send.
        podRegistry.keySet().retainAll(activePods);

        if (LOG.isDebugEnabled()) {
            LOG.debug("PodLeaseManager synced leases: [{}]",
                podRegistry.entrySet().stream()
                    .map(e -> e.getKey() + "="
                        + (e.getValue().getLeasedNamespace() == null
                        ? "null" : e.getValue().getLeasedNamespace() + "_" + e.getValue().getInflightRequests()))
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
     * Releases one slot on the given worker pod. Eviction is left to {@link #syncDiscovery}.
     */
    void releaseLease(String workerAddress) {
        PodState state = podRegistry.get(workerAddress);
        if (state != null) {
            state.decrementInflightRequests();
        }
    }
    
    /** Intended primarily for testing. */
    Map<String, PodState> getRegistry() {
        return podRegistry;
    }
}
