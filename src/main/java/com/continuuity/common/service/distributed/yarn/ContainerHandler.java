package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.ApplicationMasterService;
import com.continuuity.common.service.distributed.ContainerGroupSpecification;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 *
 */
public class ContainerHandler extends AbstractScheduledService {
  private static final Logger Log = LoggerFactory.getLogger(ContainerHandler.class);
  private static final int MAX_CHECK_FAILURES = 10;

  private final Container container;
  private final ContainerGroupSpecification specification;
  private final ApplicationMasterService amService;
  private final ContainerLaunchContextFactory containerLaunchContextFactory;
  private ContainerManager containerMgr;
  private ContainerStatus status;
  private int checkFailures = 0;

  public ContainerHandler(ApplicationMasterService amService, Container container, ContainerGroupSpecification specification) {
    this.container = container;
    this.specification = specification;
    this.amService = amService;

    ImmutablePair<Resource, Resource> clusterResources = amService.getClusterResourcesRange();
    this.containerLaunchContextFactory = new ContainerLaunchContextFactory(
      clusterResources.getFirst(), clusterResources.getSecond()
    );
  }

  @Override
  public void startUp() {
    ContainerLaunchContext ctxt = containerLaunchContextFactory.create(specification);
    ctxt.setContainerId(container.getId());
    ctxt.setResource(container.getResource());
    containerMgr = amService.getContainerManagerConnection().connect(container);
    if(containerMgr == null) {
      Log.warn("Failed connecting to container manager for container {}", container.toString());
      stop();
      return;
    }

    StartContainerRequest startRequest = Records.newRecord(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(ctxt);
    Log.debug("Starting container {}", container.getId().toString());
    try {
      containerMgr.startContainer(startRequest);
    } catch (YarnRemoteException e) {
      Log.warn("Failed starting container {}. Reason : {}", container.toString(), e.getMessage());
      stop();
    }
  }

  @Override
  public void shutDown() {
    if (status != null && status.getState() != ContainerState.COMPLETE) {
      Log.info("Stopping container: " + container.getId());
      StopContainerRequest req = Records.newRecord(StopContainerRequest.class);
      req.setContainerId(container.getId());
      try {
        containerMgr.stopContainer(req);
      } catch (YarnRemoteException e) {
        Log.warn("Exception thrown stopping container: " + container, e);
      }
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    GetContainerStatusRequest req = Records.newRecord(GetContainerStatusRequest.class);
    req.setContainerId(container.getId());
    try {
      GetContainerStatusResponse resp = containerMgr.getContainerStatus(req);
      status = resp.getStatus();
      Log.debug("Container {} status {}.", container.toString(), status.toString());
      if (status != null && status.getState() == ContainerState.COMPLETE) {
        stop();
      }
    } catch (YarnRemoteException e) {
      Log.warn("There was problem receiving the status of container {}. Reason : {}",
        container.toString(), e.getMessage());
      checkFailures++;
      if(status == null || checkFailures > MAX_CHECK_FAILURES) {
        Log.warn("Failed after max retry to get status of container {}", container.toString());
        stop();
      }
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(20, 20, TimeUnit.SECONDS);
  }
}
