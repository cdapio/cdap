package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.ApplicationMasterService;
import com.continuuity.common.service.distributed.ContainerGroupSpecification;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages a group of like containers.
 */
public class ContainerGroupHandler {
  private static final Logger Log = LoggerFactory.getLogger(ContainerGroupHandler.class);

  private final ApplicationMasterService amService;

  /**
   * Parameter for the group of containers managed by this class.
   */
  private final ContainerGroupSpecification specification;

  /**
   * Number of instances requested for this group of containers.
   */
  private final int needed;

  /**
   * Flag indicating whether group of containers managed by this class should be stopped.
   */
  private volatile boolean stopping = false;

  /**
   * No of containers requested.
   */
  private int requested = 0;

  /**
   * No of containers that are completed.
   */
  private int completed = 0;

  /**
   * Generates a incremental new unique id.
   */
  private final AtomicInteger requestId = new AtomicInteger();

  /**
   * Factory for container launch context
   */
  private final ContainerLaunchContextFactory containerLaunchContextFactory;

  /**
   * Map of container id to container handler.
   */
  private final Map<ContainerId, ContainerHandler> containerMgrs;

  /**
   * No of containers that have failed. All stage of failures are considered.
   */
  private final AtomicInteger failures = new AtomicInteger();

  /**
   * Creates an instance of manager that is managing this group of containers.
   *
   * @param amService
   * @param specification for this group of containers.
   */
  public ContainerGroupHandler(ApplicationMasterService amService, ContainerGroupSpecification specification) {
    this.amService = amService;
    this.specification = specification;
    this.needed = specification.getNumInstances();
    ImmutablePair<Resource, Resource> clusterResources = amService.getClusterResourcesRange();
    containerLaunchContextFactory =
      new ContainerLaunchContextFactory(clusterResources.getFirst(), clusterResources.getSecond());
    containerMgrs = Maps.newHashMapWithExpectedSize(needed);

  }

  /**
   * Makes a request to allocate a container.
   *
   * @param id
   * @param request
   * @return
   */
  private AMResponse allocate(int id, ResourceRequest request) {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(id);
    req.setApplicationAttemptId(amService.getSpecification().getApplicationAttemptId());
    req.addAsk(request);
    try {
      return amService.getResourceManager().allocate(req).getAMResponse();
    } catch (YarnRemoteException e) {
      Log.warn("There was a problem while requesting resource. Reason : {}", e.getMessage());
      return Records.newRecord(AllocateResponse.class).getAMResponse();
    }
  }

  /**
   * Allocates, monitors and reallocates containers.
   *
   * @return true to keep going; false otherwise.
   */
  public boolean process() {
    if(shouldProceed()) {

      /** Make resource request and request for 'needed' containers. */
      ResourceRequest req = containerLaunchContextFactory.createResourceRequest(specification);

      /** In case there is nothing required, zero request is sent to RM */
      req.setNumContainers(needed - requested);
      if(requested < needed) {
        requested = needed;
      }

      AMResponse response = allocate(requestId.incrementAndGet(), req);
      List<Container> newContainers = response.getAllocatedContainers();
      for(Container container : newContainers) {
        if(! containerMgrs.containsKey(container.getId())) {
          ContainerHandler cm = new ContainerHandler(amService, container, specification);
          containerMgrs.put(container.getId(), cm);
          cm.start();
        } else {
          Log.info("Container {} is already running.", container.getId().getId());
        }
      }

      /**
       * Now, we get the status of all the containers, if there are some failed container, then, we start them
       * again.
       */
      Map<ContainerId, ContainerStatus> containerStatus = Maps.newHashMapWithExpectedSize(needed);
      for(ContainerStatus status : response.getCompletedContainersStatuses()) {
        containerStatus.put(status.getContainerId(), status);
      }

      int complete = 0;
      Set<ContainerId> failed = Sets.newHashSet();
      for(ContainerId containerId : containerMgrs.keySet()) {
        if(containerStatus.containsKey(containerId)) {
          int exitStatus = containerStatus.get(containerId).getExitStatus();
          if (exitStatus == 0) {
            complete++;
          } else {
            Log.debug("Container with id {}, failed. Will be attempted to be started.", containerId);
            failed.add(containerId);
          }
        }
      }

      if (!failed.isEmpty()) {
        failures.addAndGet(failed.size());
        requested -= failed.size();
        for(ContainerId failedId : failed) {
          containerMgrs.put(failedId, null);
        }
      }

      completed = complete;
    }
    return shouldProceed();
  }

  /**
   * Number of completed containers.
   *
   * @return number of completed containers.
   */
  public int getCompleted() {
    return completed;
  }

  /**
   * Number of containers that failed. Inclusive of all stages of failures.
   *
   * @return number of failures.
   */
  public int getFailures() {
    return failures.intValue();
  }

  /**
   * Returns whether group allocation, monitoring or reallocation should continue.
   *
   * @return true to continue; false otherwise.
   */
  private boolean shouldProceed() {
    return !stopping && completed < needed;
  }

  /**
   * Stop all the container managers in this group.
   */
  public void stop() {
    stopping = true;
    for(ContainerHandler cmgr : containerMgrs.values()) {
      if(cmgr != null) {
        cmgr.stop();
      }
    }
  }
}
