/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.yarn;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.appmaster.RunnableProcessLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Abstract base for implementing YarnAMClient for different versions of hadoop.
 *
 * @param <T> Type of container request.
 *
 * TODO: CDAP-8842. Copied from fixes for TWILL-186.
 */
public abstract class AbstractYarnAMClient<T> extends AbstractIdleService implements YarnAMClient {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractYarnAMClient.class);

  // Map from a unique ID to inflight requests
  private final Multimap<String, T> inflightRequests;
  // Map from a unique ID to pending requests that are not yet submitted to YARN
  private final Multimap<String, T> pendingRequests;
  // List of requests pending to remove through allocate call
  private final List<T> pendingRemoves;
  //List of pending blacklist additions for the next allocate call
  private final List<String> blacklistAdditions;
  //List of pending blacklist removals for the next allocate call
  private final List<String> blacklistRemovals;
  //Keep track of blacklisted resources
  private final List<String> blacklistedResources;
  /**
   * Contains a list of known unsupported features.
   */
  protected final Set<String> unsupportedFeatures = Sets.newHashSet();

  protected final ContainerId containerId;
  protected InetSocketAddress trackerAddr;
  protected URL trackerUrl;

  /**
   * Constructs an instance of AMClient.
   *
   * @param containerIdEnvName Name of the environment variable that contains value of the AM container ID.
   */
  protected AbstractYarnAMClient(String containerIdEnvName) {
    String masterContainerId = System.getenv().get(containerIdEnvName);
    Preconditions.checkArgument(masterContainerId != null,
                                "Missing %s from environment", containerIdEnvName);
    this.containerId = ConverterUtils.toContainerId(masterContainerId);
    this.inflightRequests = ArrayListMultimap.create();
    this.pendingRequests = ArrayListMultimap.create();
    this.pendingRemoves = Lists.newLinkedList();
    this.blacklistAdditions = Lists.newArrayList();
    this.blacklistRemovals = Lists.newArrayList();
    this.blacklistedResources = Lists.newArrayList();
  }


  @Override
  public final ContainerId getContainerId() {
    return containerId;
  }

  @Override
  public final void setTracker(InetSocketAddress trackerAddr, URL trackerUrl) {
    this.trackerAddr = trackerAddr;
    this.trackerUrl = trackerUrl;
  }

  @Override
  public final synchronized void allocate(float progress, AllocateHandler handler) throws Exception {
    // In one allocate cycle, either only do new container request or removal of requests.
    // This is a workaround for YARN-314.
    // When remove a container request, AMRMClient will send a container request with size = 0
    // With bug YARN-314, if we mix the allocate call with new container request of the same priority,
    // in some cases the RM would not see the new request (based on sorting of resource capability),
    // but rather only see the one with size = 0.
    if (pendingRemoves.isEmpty()) {
      for (Map.Entry<String, T> entry : pendingRequests.entries()) {
        addContainerRequest(entry.getValue());
      }
      inflightRequests.putAll(pendingRequests);
      pendingRequests.clear();
    } else {
      for (T request : pendingRemoves) {
        removeContainerRequest(request);
      }
      pendingRemoves.clear();
    }

    if (!blacklistAdditions.isEmpty() || !blacklistRemovals.isEmpty()) {
      updateBlacklist(blacklistAdditions, blacklistRemovals);
      blacklistAdditions.clear();
      blacklistRemovals.clear();
    }

    AllocateResult allocateResponse = doAllocate(progress);
    List<RunnableProcessLauncher> launchers = allocateResponse.getLaunchers();

    if (!launchers.isEmpty()) {
      // Only call handler acquire if there is actually inflight requests.
      // This is to workaround the YARN behavior that it can return more containers being asked,
      // such that it causes us to launch process in the pending requests with the wrong container size
      if (!inflightRequests.isEmpty()) {
        handler.acquired(launchers);
      }

      // If no process has been launched through the given launcher, return the container.
      for (ProcessLauncher<YarnContainerInfo> l : launchers) {
        // This cast always works.
        RunnableProcessLauncher launcher = (RunnableProcessLauncher) l;
        if (!launcher.isLaunched()) {
          YarnContainerInfo containerInfo = launcher.getContainerInfo();
          // Casting is needed in Java 8, otherwise it complains about ambiguous method over the info(String, Throwable)
          LOG.info("Nothing to run in container, releasing it: {}", containerInfo.getContainer());
          releaseAssignedContainer(containerInfo);
        }
      }
    }

    List<YarnContainerStatus> completed = allocateResponse.getCompletedStatus();
    if (!completed.isEmpty()) {
      handler.completed(completed);
    }
  }

  @Override
  public final ContainerRequestBuilder addContainerRequest(Resource capability, int count) {
    return new ContainerRequestBuilder(adjustCapability(capability), count) {
      @Override
      public String apply() {
        synchronized (AbstractYarnAMClient.this) {
          String id = UUID.randomUUID().toString();

          String[] hosts = this.hosts.isEmpty() ? null : this.hosts.toArray(new String[this.hosts.size()]);
          String[] racks = this.racks.isEmpty() ? null : this.racks.toArray(new String[this.racks.size()]);

          for (int i = 0; i < count; i++) {
            T request = createContainerRequest(priority, capability, hosts, racks, relaxLocality);
            pendingRequests.put(id, request);
          }

          return id;
        }
      }
    };

  }

  @Override
  public final void addToBlacklist(String resource) {
    if (!blacklistAdditions.contains(resource) && !blacklistedResources.contains(resource)) {
      blacklistAdditions.add(resource);
      blacklistedResources.add(resource);
      blacklistRemovals.remove(resource);
    }
  }

  @Override
  public final void removeFromBlacklist(String resource) {
    if (!blacklistRemovals.contains(resource) && blacklistedResources.contains(resource)) {
      blacklistRemovals.add(resource);
      blacklistedResources.remove(resource);
      blacklistAdditions.remove(resource);
    }
  }

  @Override
  public final void clearBlacklist() {
    blacklistRemovals.addAll(blacklistedResources);
    blacklistedResources.clear();
    blacklistAdditions.clear();
  }

  @Override
  public final synchronized void completeContainerRequest(String id) {
    for (T request : inflightRequests.removeAll(id)) {
      pendingRemoves.add(request);
    }
  }

  /**
   * Records an unsupported feature.
   * @param unsupportedFeature A string identifying an unsupported feature.
   * @return Returns {@code false} if the feature has already been recorded, {@code true} otherwise.
   */
  protected boolean recordUnsupportedFeature(String unsupportedFeature) {
    if (unsupportedFeatures.contains(unsupportedFeature)) {
      return false;
    }
    unsupportedFeatures.add(unsupportedFeature);
    return true;
  }

  /**
   * Adjusts the given resource capability to fit in the cluster limit.
   *
   * @param capability The capability to be adjusted.
   * @return A {@link Resource} instance representing the adjusted result.
   */
  protected abstract Resource adjustCapability(Resource capability);

  /**
   * Creates a container request based on the given requirement.
   *
   * @param priority The priority of the request.
   * @param capability The resource capability.
   * @param hosts Sets of hosts. Could be {@code null}.
   * @param racks Sets of racks. Could be {@code null}.
   * @param relaxLocality If set {@code false}, locality constraints will not be relaxed.
   * @return A container request.
   */
  protected abstract T createContainerRequest(Priority priority, Resource capability,
                                              @Nullable String[] hosts, @Nullable String[] racks,
                                              boolean relaxLocality);

  /**
   * Adds the given request to prepare for next allocate call.
   */
  protected abstract void addContainerRequest(T request);

  /**
   * Removes the given request to prepare for the next allocate call.
   */
  protected abstract void removeContainerRequest(T request);

  /**
   * Send blacklist updates in the next allocate call.
   */
  protected abstract void updateBlacklist(List<String> blacklistAdditions, List<String> blacklistRemovals);

  /**
   * Performs actual allocate call to RM.
   */
  protected abstract AllocateResult doAllocate(float progress) throws Exception;

  /**
   * Releases the given container back to RM.
   */
  protected abstract void releaseAssignedContainer(YarnContainerInfo containerInfo);

  /**
   * Class for carrying results for the {@link #doAllocate(float)} call.
   */
  protected static final class AllocateResult {
    private final List<RunnableProcessLauncher> launchers;
    private final List<YarnContainerStatus> completedStatus;

    public AllocateResult(List<RunnableProcessLauncher> launchers, List<YarnContainerStatus> completedStatus) {
      this.launchers = ImmutableList.copyOf(launchers);
      this.completedStatus = ImmutableList.copyOf(completedStatus);
    }

    public List<RunnableProcessLauncher> getLaunchers() {
      return launchers;
    }

    public List<YarnContainerStatus> getCompletedStatus() {
      return completedStatus;
    }
  }
}
