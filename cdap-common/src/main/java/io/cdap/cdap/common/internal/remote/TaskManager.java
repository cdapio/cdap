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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralized Task Manager for orchestrating Warm Sticky Leases on Task Worker pods.
 * This class coordinates leases, concurrency, and logical resets.
 */
public class TaskManager {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);
  private static final TaskManager INSTANCE = new TaskManager();

  // Concurrency and task limits based on the design doc
  private static final int MAX_CONCURRENT_TASKS_PER_POD = 10;
  static final int MAX_TOTAL_TASKS_BEFORE_RESET = 10;

  private final ReentrantLock lock = new ReentrantLock();

  // Lease state maps
  // Pod IP/Key -> Namespace currently leased
  private final Map<String, String> podLeases = new HashMap<>();
  // Pod IP/Key -> Active concurrent task count
  private final Map<String, Integer> podActiveTaskCounts = new HashMap<>();
  // Pod IP/Key -> Total tasks processed on the current lease
  private final Map<String, Integer> podTotalTaskProcessedCounts = new HashMap<>();

  public static TaskManager getInstance() {
    return INSTANCE;
  }

  private TaskManager() {
    // Singleton
  }

  /**
   * Resolves the target warm pod for a given namespace based on the sticky lease model.
   *
   * @param namespace the namespace requesting execution
   * @param availablePods the list of currently discovered pods
   * @return the selected pod, or null if no pod is available
   */
  @Nullable
  public Discoverable resolvePod(String namespace, List<Discoverable> availablePods) {
    lock.lock();
    try {
      String leasedPodIp = null;
      Discoverable selectedPod = null;

      // 1. Find if a pod is already leased to this namespace and has capacity
      for (Discoverable pod : availablePods) {
        String podIp = getPodKey(pod);
        String currentLease = podLeases.get(podIp);

        if (namespace.equals(currentLease)) {
          int activeTasks = podActiveTaskCounts.getOrDefault(podIp, 0);
          int totalProcessed = podTotalTaskProcessedCounts.getOrDefault(podIp, 0);
          if (activeTasks < MAX_CONCURRENT_TASKS_PER_POD && totalProcessed < MAX_TOTAL_TASKS_BEFORE_RESET) {
            leasedPodIp = podIp;
            selectedPod = pod;
            break;
          }
        }
      }

      // 2. If no active lease exists (or it is at capacity), find an idle/unleased pod
      if (selectedPod == null) {
        for (Discoverable pod : availablePods) {
          String podIp = getPodKey(pod);
          String currentLease = podLeases.get(podIp);

          if (currentLease == null) {
            // Establish a new lease on this idle pod
            podLeases.put(podIp, namespace);
            podActiveTaskCounts.put(podIp, 0);
            podTotalTaskProcessedCounts.put(podIp, 0);
            
            LOG.info("sidhdirenge - TaskManager: Established new lease for namespace '{}' on pod '{}'",
                     namespace, podIp);
            
            leasedPodIp = podIp;
            selectedPod = pod;
            break;
          }
        }
      }

      // 3. Fallback: If all pods are leased to other namespaces, find the pod with the least load
      if (selectedPod == null) {
        LOG.warn("sidhdirenge - TaskManager: All pods are leased. Falling back to least-loaded pod.");
        int minLoad = Integer.MAX_VALUE;
        List<Discoverable> bestPods = new ArrayList<>();
        for (Discoverable pod : availablePods) {
          String podIp = getPodKey(pod);
          int activeTasks = podActiveTaskCounts.getOrDefault(podIp, 0);
          if (activeTasks < minLoad) {
            minLoad = activeTasks;
            bestPods.clear();
            bestPods.add(pod);
          } else if (activeTasks == minLoad) {
            bestPods.add(pod);
          }
        }
        
        if (!bestPods.isEmpty()) {
          int randomIndex = java.util.concurrent.ThreadLocalRandom.current().nextInt(bestPods.size());
          selectedPod = bestPods.get(randomIndex);
          leasedPodIp = getPodKey(selectedPod);
          
          // Force-assign lease to the new namespace
          podLeases.put(leasedPodIp, namespace);
          podActiveTaskCounts.put(leasedPodIp, 0);
          podTotalTaskProcessedCounts.put(leasedPodIp, 0);
        }
      }

      // 4. Increment task counts for the selected pod
      if (selectedPod != null) {
        int activeTasks = podActiveTaskCounts.getOrDefault(leasedPodIp, 0) + 1;
        int totalProcessed = podTotalTaskProcessedCounts.getOrDefault(leasedPodIp, 0) + 1;

        podActiveTaskCounts.put(leasedPodIp, activeTasks);
        podTotalTaskProcessedCounts.put(leasedPodIp, totalProcessed);

        LOG.info("sidhdirenge - TaskManager: Routing task for '{}' to pod '{}' (Active: {}, Total: {})",
                 namespace, leasedPodIp, activeTasks, totalProcessed);
      }

      return selectedPod;
    } finally {
      lock.unlock();
    }
  }

  public void finishTask(String namespace, Discoverable pod, boolean rejected) {
    lock.lock();
    try {
      String podIp = getPodKey(pod);
      int activeTasks = podActiveTaskCounts.getOrDefault(podIp, 0);
      if (activeTasks > 0) {
        activeTasks = activeTasks - 1;
        podActiveTaskCounts.put(podIp, activeTasks);
      }
      
      if (rejected) {
        int totalProcessed = podTotalTaskProcessedCounts.getOrDefault(podIp, 0);
        if (totalProcessed > 0) {
          podTotalTaskProcessedCounts.put(podIp, totalProcessed - 1);
        }
      } else {
        int totalProcessed = podTotalTaskProcessedCounts.getOrDefault(podIp, 0);
        if (totalProcessed >= MAX_TOTAL_TASKS_BEFORE_RESET && activeTasks == 0) {
          LOG.info("sidhdirenge - TaskManager: Pod '{}' finished all active tasks "
                       + "after reaching reset threshold. Reclaiming lease.", podIp);
          releaseLease(podIp);
        }
      }
      
      LOG.info("sidhdirenge - TaskManager: Task finished for '{}' on pod '{}' (Remaining active: {}, Rejected: {})",
               namespace, podIp, activeTasks, rejected);
    } finally {
      lock.unlock();
    }
  }

  private void releaseLease(String podIp) {
    podLeases.remove(podIp);
    podActiveTaskCounts.remove(podIp);
    podTotalTaskProcessedCounts.remove(podIp);
  }

  private String getPodKey(Discoverable pod) {
    return pod.getSocketAddress().getHostString() + ":" + pod.getSocketAddress().getPort();
  }
}
