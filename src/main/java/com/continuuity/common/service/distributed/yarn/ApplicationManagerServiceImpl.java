package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.*;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Application Manager service is base implementation for YARN application manager.
 */
public class ApplicationManagerServiceImpl extends AbstractScheduledService implements ApplicationMasterService {
  private static final Logger Log = LoggerFactory.getLogger(ApplicationManagerServiceImpl.class);

  /**
   * Parameters settings for application master.
   */
  private final ApplicationMasterSpecification specification;

  /**
   * Resource manager connection handler.
   */
  private final ResourceManagerConnectionHandler<AMRMProtocol> rmHandler;

  /**
   * Container manager connection handler.
   */
  private final ContainerManagerConnectionHandler cmHandler;

  /**
   * Collection of container groups managed by this application manager.
   */
  private final List<ContainerGroupHandler> containerGroupHandlers = Lists.newArrayList();

  /**
   * Minimum cluster resources.
   */
  private Resource minClusterResource;

  /**
   * Maximum cluster resources.
   */
  private Resource maxClusterResource;

  /**
   * Generates a new incremental unique id.
   */
  private final AtomicInteger requestId = new AtomicInteger();

  /**
   * Handler to resource manager.
   */
  private AMRMProtocol resourceMgr;

  public ApplicationManagerServiceImpl(ApplicationMasterSpecification specification) {
    this(specification,
      new ResourceManagerConnectionHandlerImpl(specification.getConfiguration()),
      new ContainerManagerConnectionHandlerImpl(specification.getConfiguration()));
  }

  public ApplicationManagerServiceImpl(ApplicationMasterSpecification specification, ResourceManagerConnectionHandler<AMRMProtocol> rmHandler,
                                       ContainerManagerConnectionHandler cmHandler) {
    this.specification = specification;
    this.rmHandler = rmHandler;
    this.cmHandler = cmHandler;
  }
  /**
   * Starts up the application manager service.
   */
  @Override
  protected void startUp() {
    Log.info("Starting the application service.");
    resourceMgr = rmHandler.connect();

    /** Register the application master with the resource manager. */
    RegisterApplicationMasterResponse registration = null;
    try {
      RegisterApplicationMasterRequest request = Records.newRecord(RegisterApplicationMasterRequest.class);
      request.setApplicationAttemptId(specification.getApplicationAttemptId());
      request.setHost(specification.getHostname());
      request.setRpcPort(specification.getClientPort());
      request.setTrackingUrl(specification.getTrackingUrl());
      registration = resourceMgr.registerApplicationMaster(request);
    } catch (YarnRemoteException e) {
      Log.error("There was problem during registering the application master. Reason : {}", e.getMessage());
      stop();
      return;
    }

    minClusterResource = registration.getMaximumResourceCapability();
    maxClusterResource = registration.getMaximumResourceCapability();

    /** Gets all container group parameters*/
    List<ContainerGroupSpecification> containerGroups = specification.getAllContainerGroups();

    if(containerGroups.size() < 1) {
      Log.info("No containers have been configured to be started. Stopping now");
      stop();
    }

    /** Iterate through all container groups, initialize and start them. */
    for(int i = 0; i < containerGroups.size(); ++i) {
      ContainerGroupSpecification clp = containerGroups.get(i);
      containerGroupHandlers.add(new ContainerGroupHandler(clp));
    }
  }

  /**
   * One run of iteration triggered by {@link #scheduler()}.
   *
   * We iterate through container requesting of all failures and each for status of each
   * container group.
   *
   * @throws Exception
   */
  @Override
  protected void runOneIteration() throws Exception {

    /** Iterate and collection total number of failures across all the container groups. */
    int totalFailures = 0;
    for(int i = 0; i < containerGroupHandlers.size(); ++i) {
      totalFailures += containerGroupHandlers.get(i).getFailures();
    }

    /**
     * If total failures across all container groups crosses the threshold for application, then we force
     * fail the application master service.
     */
    if(totalFailures > specification.getAllowedFailures() && specification.getAllowedFailures() != -1) {
      stop();
      return;
    }

    /**
     * Iterate through all container groups.
     */
    boolean keepGoing = false;
    for(ContainerGroupHandler containerGroupHandler : containerGroupHandlers) {
      keepGoing |= containerGroupHandler.process();
    }

    if(! keepGoing) {
      stop();
    }
  }

  /**
   * Shuts down the Application service.
   */
  @Override
  protected void shutDown() {
    Log.info("Shutting down the application service.");

    /** Iterate through all the groups and request them to be stopped. */
    int totalFailures = 0;
    for(ContainerGroupHandler containerGroupHandler : containerGroupHandlers) {
      totalFailures += containerGroupHandler.getFailures();
      containerGroupHandler.stop();
    }

    /** Let resource manager know that you are done. */
    FinishApplicationMasterRequest request = Records.newRecord(FinishApplicationMasterRequest.class);
    request.setAppAttemptId(getApplicationAttemptId());

    if(state() == State.FAILED) {
      request.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
    } else if(totalFailures > specification.getAllowedFailures()) {
      request.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
    } else {
      request.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
    }
    try {
      resourceMgr.finishApplicationMaster(request);
    } catch (YarnRemoteException e) {
      Log.warn("Failed while shutting down application manager service. Reason : {}", e.getMessage());
    }
  }

  /**
   * Schedules {@link #runOneIteration()} runs to happen every 1 second.
   *
   * @return instance of {@link Scheduler}
   */
  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS);
  }

  /**
   * Returns the application service parameters.
   *
   * @return parameters of application parameters.
   */
  @Override
  public ApplicationMasterSpecification getSpecification() {
    return specification;
  }

  /**
   * Application attempt id
   *
   * @return application attempt id.
   */
  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return specification.getApplicationAttemptId();
  }

  /**
   * Makes a request to allocate a container.
   *
   * @param requestId
   * @param request
   * @return
   */
  private AMResponse allocate(int requestId, ResourceRequest request) {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(requestId);
    req.setApplicationAttemptId(getSpecification().getApplicationAttemptId());
    req.addAsk(request);
    try {
      return resourceMgr.allocate(req).getAMResponse();
    } catch (YarnRemoteException e) {
      Log.warn("There was a problem while requesting resource. Reason : {}", e.getMessage());
    }
    return Records.newRecord(AllocateResponse.class).getAMResponse();
  }

  /**
   * Manages a group of like containers.
   */
  public class ContainerGroupHandler {
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
     * @param specification for this group of containers.
     */
    public ContainerGroupHandler(ContainerGroupSpecification specification) {
      this.specification = specification;
      this.needed = specification.getNumInstances();
      containerLaunchContextFactory =
        new ContainerLaunchContextFactory(minClusterResource, maxClusterResource);
      containerMgrs = Maps.newHashMapWithExpectedSize(needed);

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
            ContainerHandler cm = new ContainerHandler(container, specification);
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

  /**
   *
   */
  public class ContainerHandler extends AbstractScheduledService {
    private static final int MAX_CHECK_FAILURES = 10;

    private final Container container;
    private final ContainerGroupSpecification specification;
    private final ContainerLaunchContextFactory containerLaunchContextFactory;
    private ContainerManager containerMgr;
    private ContainerStatus status;
    private int checkFailures = 0;

    public ContainerHandler(Container container, ContainerGroupSpecification specification) {
      this.container = container;
      this.specification = specification;
      this.containerLaunchContextFactory = new ContainerLaunchContextFactory(
        minClusterResource, maxClusterResource
      );
    }

    @Override
    public void startUp() {
      ContainerLaunchContext ctxt = containerLaunchContextFactory.create(specification);
      ctxt.setContainerId(container.getId());
      ctxt.setResource(container.getResource());
      containerMgr = cmHandler.connect(container);
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
}
