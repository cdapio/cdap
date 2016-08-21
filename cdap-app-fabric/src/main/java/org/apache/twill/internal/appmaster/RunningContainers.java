/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.appmaster;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.internal.ContainerExitCodes;
import org.apache.twill.internal.ContainerInfo;
import org.apache.twill.internal.ContainerLiveNodeData;
import org.apache.twill.internal.DefaultResourceReport;
import org.apache.twill.internal.DefaultTwillRunResources;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.TwillContainerController;
import org.apache.twill.internal.TwillContainerLauncher;
import org.apache.twill.internal.container.TwillContainerMain;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.yarn.YarnContainerStatus;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A helper class for ApplicationMasterService to keep track of running containers and to interact
 * with them.
 */
final class RunningContainers {
  private static final Logger LOG = LoggerFactory.getLogger(RunningContainers.class);

  /**
   * Function to return cardinality of a given BitSet.
   */
  private static final Function<BitSet, Integer> BITSET_CARDINALITY = new Function<BitSet, Integer>() {
    @Override
    public Integer apply(BitSet input) {
      return input.cardinality();
    }
  };

  // Table of <runnableName, containerId, controller>
  private final Table<String, String, TwillContainerController> containers;

  // Map from runnableName to a BitSet, with the <instanceId> bit turned on for having an instance running.
  private final Map<String, BitSet> runnableInstances;
  private final Map<String, Integer> completedContainerCount;
  private final DefaultResourceReport resourceReport;
  private final Deque<String> startSequence;
  private final Lock containerLock;
  private final Condition containerChange;
  private final ZKClient zkClient;
  private final Multimap<String, ContainerInfo> containerStats;

  RunningContainers(String appId, TwillRunResources appMasterResources, ZKClient zookeeperClient) {
    containers = HashBasedTable.create();
    runnableInstances = Maps.newHashMap();
    completedContainerCount = Maps.newHashMap();
    startSequence = Lists.newLinkedList();
    containerLock = new ReentrantLock();
    containerChange = containerLock.newCondition();
    resourceReport = new DefaultResourceReport(appId, appMasterResources);
    zkClient = zookeeperClient;
    containerStats = HashMultimap.create();
  }

  /**
   * Returns {@code true} if there is no live container.
   */
  boolean isEmpty() {
    containerLock.lock();
    try {
      return runnableInstances.isEmpty();
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Start a container for a runnable.
   */
  void start(String runnableName, ContainerInfo containerInfo, TwillContainerLauncher launcher) {
    containerLock.lock();
    try {
      int instanceId = getStartInstanceId(runnableName);
      RunId runId = getRunId(runnableName, instanceId);
      TwillContainerController controller = launcher.start(runId, instanceId,
                                                           TwillContainerMain.class, "$HADOOP_CONF_DIR");
      containers.put(runnableName, containerInfo.getId(), controller);
      TwillRunResources resources = new DynamicTwillRunResources(instanceId,
                                                                 containerInfo.getId(),
                                                                 containerInfo.getVirtualCores(),
                                                                 containerInfo.getMemoryMB(),
                                                                 containerInfo.getHost().getHostName(),
                                                                 controller,
                                                                 System.getenv(EnvKeys.TWILL_APP_LOG_LEVEL));
      resourceReport.addRunResources(runnableName, resources);
      containerStats.put(runnableName, containerInfo);

      if (startSequence.isEmpty() || !runnableName.equals(startSequence.peekLast())) {
        startSequence.addLast(runnableName);
      }
      containerChange.signalAll();

    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Watch for changes to services under given path.
   * @param path to check for changes.
   */
  void addWatcher(String path) {
    ZKOperations.watchChildren(zkClient, path, new ZKOperations.ChildrenCallback() {
      @Override
      public void updated(NodeChildren nodeChildren) {
        resourceReport.setServices(nodeChildren.getChildren());
      }
    });
  }

  ResourceReport getResourceReport() {
    return resourceReport;
  }

  /**
   * Given a runnable name, returns a list of {@link org.apache.twill.internal.ContainerInfo} for it's instances.
   * @param runnableName name of a runnable.
   * @return a list of {@link org.apache.twill.internal.ContainerInfo} for instances of a runnable.
   */
  Collection<ContainerInfo> getContainerInfo(String runnableName) {
    return containerStats.get(runnableName);
  }

  /**
   * Stops and removes the last running container of the given runnable.
   * This method blocks until handleCompleted() is run for the runnable or a timeout occurs.
   */
  void stopLastAndWait(String runnableName) {
    int maxInstanceId;
    containerLock.lock();
    try {
      maxInstanceId = getMaxInstanceId(runnableName);
      if (maxInstanceId < 0) {
        LOG.warn("No running container found for {}", runnableName);
        return;
      }
    } finally {
      containerLock.unlock();
    }
    stopByIdAndWait(runnableName, maxInstanceId);
  }

  /**
   * Stop and removes a container for a runnable on an id.
   * This method blocks until handleCompleted() is run for the runnable or a timeout occurs.
   * Hence this call should not be made within a containerLock
   */
  void stopByIdAndWait(String runnableName, int instanceId) {
    String containerId = null;
    TwillContainerController controller = null;
    containerLock.lock();
    try {
      // Find the controller with particular instance id.
      for (Map.Entry<String, TwillContainerController> entry : containers.row(runnableName).entrySet()) {
        if (getInstanceId(entry.getValue().getRunId()) == instanceId) {
          containerId = entry.getKey();
          controller = entry.getValue();
          break;
        }
      }

      Preconditions.checkState(containerId != null,
                               "No container found for {} with instanceId = {}", runnableName, instanceId);
      Preconditions.checkState(controller != null,
                               "Null controller found for {} with instanceId = {}", runnableName, instanceId);
    } finally {
      containerLock.unlock();
    }

    LOG.info("Stopping service: {} {}", runnableName, controller.getRunId());
    // This call will block until handleCompleted() method runs or a timeout occurs
    controller.stopAndWait();

    // Remove the stopped container state if it exists (in the case of killing the container due to timeout)
    containerLock.lock();
    try {
      if (removeContainerInfo(containerId)) {
        containers.remove(runnableName, containerId);
        removeInstanceId(runnableName, instanceId);
        resourceReport.removeRunnableResources(runnableName, containerId);
        containerChange.signalAll();
      }
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Blocks until there are changes in running containers.
   */
  void waitForCount(String runnableName, int count) throws InterruptedException {
    containerLock.lock();
    try {
      while (getRunningInstances(runnableName) != count) {
        containerChange.await();
      }
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Returns the number of running instances of the given runnable.
   */
  int count(String runnableName) {
    containerLock.lock();
    try {
      return getRunningInstances(runnableName);
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Returns a Map contains running instances of all runnables.
   */
  Map<String, Integer> countAll() {
    containerLock.lock();
    try {
      return ImmutableMap.copyOf(Maps.transformValues(runnableInstances, BITSET_CARDINALITY));
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Returns a Map containing number of successfully completed containers for all runnables.
   */
  Map<String, Integer> getCompletedContainerCount() {
    containerLock.lock();
    try {
      return ImmutableMap.copyOf(completedContainerCount);
    } finally {
      containerLock.unlock();
    }
  }

  void sendToAll(Message message, Runnable completion) {
    containerLock.lock();
    try {
      if (containers.isEmpty()) {
        completion.run();
      }

      // Sends the command to all running containers
      AtomicInteger count = new AtomicInteger(containers.size());
      for (Map.Entry<String, Map<String, TwillContainerController>> entry : containers.rowMap().entrySet()) {
        for (TwillContainerController controller : entry.getValue().values()) {
          sendMessage(entry.getKey(), message, controller, count, completion);
        }
      }
    } finally {
      containerLock.unlock();
    }
  }

  void sendToRunnable(String runnableName, Message message, Runnable completion) {
    List<TwillContainerController> controllers;
    containerLock.lock();
    try {
      controllers = new ArrayList<>(containers.row(runnableName).values());
    } finally {
      containerLock.unlock();
    }

    if (controllers.isEmpty()) {
      completion.run();
    }

    AtomicInteger count = new AtomicInteger(controllers.size());
    for (TwillContainerController controller : controllers) {
      sendMessage(runnableName, message, controller, count, completion);
    }
  }

  /**
   * Stops all running services. Only called when the AppMaster stops.
   */
  void stopAll() {
    containerLock.lock();
    // Stop the runnables one by one in reverse order of start sequence
    List<String> reverseRunnables = new LinkedList<>();
    try {
      Iterators.addAll(reverseRunnables, startSequence.descendingIterator());
    } finally {
      containerLock.unlock();
    }

    List<ListenableFuture<Service.State>> futures = Lists.newLinkedList();
    for (String runnableName : reverseRunnables) {
      LOG.info("Stopping all instances of " + runnableName);

      futures.clear();
      // Parallel stops all running containers of the current runnable.
      containerLock.lock();
      try {
        for (TwillContainerController controller : containers.row(runnableName).values()) {
          futures.add(controller.stop());
        }
      } finally {
        containerLock.unlock();
      }
      // Wait for containers to stop. Assumes the future returned by Futures.successfulAsList won't throw exception.
      // This will block until handleCompleted() is run for the runnables or a timeout occurs.
      Futures.getUnchecked(Futures.successfulAsList(futures));

      LOG.info("Terminated all instances of " + runnableName);
    }

    // When we acquire this lock, all stopped runnables should have been cleaned up by handleCompleted() method
    containerLock.lock();
    try {
      containers.clear();
      runnableInstances.clear();
      containerStats.clear();
    } finally {
      containerLock.unlock();
    }
  }

  Set<String> getContainerIds() {
    containerLock.lock();
    try {
      return ImmutableSet.copyOf(containers.columnKeySet());
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Handle completion of container.
   *
   * @param status           The completion status.
   * @param restartRunnables Set of runnable names that requires restart.
   */
  void handleCompleted(YarnContainerStatus status, Multiset<String> restartRunnables) {
    containerLock.lock();
    String containerId = status.getContainerId();
    int exitStatus = status.getExitStatus();
    ContainerState state = status.getState();

    try {
      removeContainerInfo(containerId);
      Map<String, TwillContainerController> lookup = containers.column(containerId);
      if (lookup.isEmpty()) {
        // It's OK because if a container is stopped through stopByIdAndWait(), this would be empty.
        return;
      }

      if (lookup.size() != 1) {
        LOG.warn("More than one controller found for container {}", containerId);
      }

      boolean containerStopped = false;
      for (Map.Entry<String, TwillContainerController> completedEntry : lookup.entrySet()) {
        String runnableName = completedEntry.getKey();
        TwillContainerController controller = completedEntry.getValue();

        // TODO: Can there be multiple controllers for a single container?
        // TODO: What is the best way to determine whether to restart container when there are multiple controllers?
        // See if the controller is stopped (this is to decide whether to retry the failed containers or not)
        // In case of multiple controllers, even if one is stopped we will not re-request the container
        containerStopped = containerStopped || isControllerStopped(controller);
        controller.completed(exitStatus);

        if (exitStatus == ContainerExitCodes.SUCCESS) {
          if (!completedContainerCount.containsKey(runnableName)) {
            completedContainerCount.put(runnableName, 0);
          }
          completedContainerCount.put(runnableName, completedContainerCount.get(runnableName) + 1);
        }
        // TODO: should we remove the completed instance from instanceId and resource report even on failures?
        // TODO: won't they get added back when the container is re-requested?
        removeInstanceId(runnableName, getInstanceId(controller.getRunId()));
        resourceReport.removeRunnableResources(runnableName, containerId);
      }

      if (exitStatus != ContainerExitCodes.SUCCESS) {
        LOG.warn("Container {} exited abnormally with state {}, exit code {}.",
                 containerId, state, exitStatus);
        if (!containerStopped && shouldRetry(exitStatus)) {
          LOG.info("Re-request the container {} for exit code {}.", containerId, exitStatus);
          restartRunnables.add(lookup.keySet().iterator().next());
        } else if (containerStopped) {
          LOG.info("Container {} is being stopped, will not re-request", containerId);
        }
      } else {
        LOG.info("Container {} exited normally with state {}", containerId, state);
      }

      lookup.clear();
      containerChange.signalAll();
    } finally {
      containerLock.unlock();
    }
  }

  private boolean shouldRetry(int exitCode) {
    return exitCode != ContainerExitCodes.SUCCESS
      && exitCode != ContainerExitCodes.DISKS_FAILED
      && exitCode != ContainerExitCodes.INIT_FAILED;
  }

  private boolean isControllerStopped(TwillContainerController controller) {
    return controller.state() == Service.State.STOPPING || controller.state() == Service.State.TERMINATED;
  }

  /**
   * Sends a command through the given {@link TwillContainerController} of a runnable. Decrements the count
   * when the sending of command completed. Triggers completion when count reaches zero.
   */
  private void sendMessage(final String runnableName, final Message message,
                           final TwillContainerController controller, final AtomicInteger count,
                           final Runnable completion) {
    Futures.addCallback(controller.sendMessage(message), new FutureCallback<Message>() {
      @Override
      public void onSuccess(Message result) {
        if (count.decrementAndGet() == 0) {
          completion.run();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        try {
          LOG.error("Failed to send message. Runnable: {}, RunId: {}, Message: {}.",
                    runnableName, controller.getRunId(), message, t);
        } finally {
          if (count.decrementAndGet() == 0) {
            completion.run();
          }
        }
      }
    });
  }

  /**
   * Returns the instanceId to start the given runnable.
   */
  private int getStartInstanceId(String runnableName) {
    BitSet instances = runnableInstances.get(runnableName);
    if (instances == null) {
      instances = new BitSet();
      runnableInstances.put(runnableName, instances);
    }
    int instanceId = instances.nextClearBit(0);
    instances.set(instanceId);
    return instanceId;
  }

  /**
   * Remove instance id for a given runnable.
   */
  private void removeInstanceId(String runnableName, int instanceId) {
    BitSet instances = runnableInstances.get(runnableName);
    if (instances == null) {
      return;
    }
    instances.clear(instanceId);
    if (instances.isEmpty()) {
      runnableInstances.remove(runnableName);
    }
  }

  /**
   * Returns the largest instanceId for the given runnable. Returns -1 if no container is running.
   */
  private int getMaxInstanceId(String runnableName) {
    BitSet instances = runnableInstances.get(runnableName);
    if (instances == null || instances.isEmpty()) {
      return -1;
    }
    return instances.length() - 1;
  }

  /**
   * Returns number of running instances for the given runnable.
   */
  private int getRunningInstances(String runableName) {
    BitSet instances = runnableInstances.get(runableName);
    return instances == null ? 0 : instances.cardinality();
  }

  private RunId getRunId(String runnableName, int instanceId) {
    RunId baseId;

    Collection<TwillContainerController> controllers = containers.row(runnableName).values();
    if (controllers.isEmpty()) {
      baseId = RunIds.generate();
    } else {
      String id = controllers.iterator().next().getRunId().getId();
      baseId = RunIds.fromString(id.substring(0, id.lastIndexOf('-')));
    }

    return RunIds.fromString(baseId.getId() + '-' + instanceId);
  }

  private int getInstanceId(RunId runId) {
    String id = runId.getId();
    return Integer.parseInt(id.substring(id.lastIndexOf('-') + 1));
  }

  /**
   * Given the containerId, removes the corresponding containerInfo.
   * @param containerId Id for the container to be removed.
   * @return Returns {@code false} if container with the provided id was not found, {@code true} otherwise.
   */
  private boolean removeContainerInfo(String containerId) {
    for (ContainerInfo containerInfo : this.containerStats.values()) {
      if (containerInfo.getId().equals(containerId)) {
        this.containerStats.values().remove(containerInfo);
        return true;
      }
    }
    return false;
  }

  /**
   * A helper class that overrides the debug port of the resources with the live info from the container controller.
   */
  private static class DynamicTwillRunResources extends DefaultTwillRunResources {

    private final TwillContainerController controller;
    private Integer dynamicDebugPort = null;

    private DynamicTwillRunResources(int instanceId, String containerId,
                                     int cores, int memoryMB, String host,
                                     TwillContainerController controller, String logLevel) {
      super(instanceId, containerId, cores, memoryMB, host, null,
            (logLevel != null) ? LogEntry.Level.valueOf(logLevel) : null);
      this.controller = controller;
    }

    @Override
    public synchronized Integer getDebugPort() {
      if (dynamicDebugPort == null) {
        ContainerLiveNodeData liveData = controller.getLiveNodeData();
        if (liveData != null && liveData.getDebugPort() != null) {
          try {
            dynamicDebugPort = Integer.parseInt(liveData.getDebugPort());
          } catch (NumberFormatException e) {
            LOG.warn("Live data for {} has debug port of '{}' which cannot be parsed as a number",
                     getContainerId(), liveData.getDebugPort());
          }
        }
      }
      return dynamicDebugPort;
    }
  }
}
