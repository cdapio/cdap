package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.*;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Application Manager service is base implementation for YARN application manager.
 */
public class ApplicationMasterServiceImpl extends AbstractScheduledService implements ApplicationMasterService {
  private static final Logger Log = LoggerFactory.getLogger(ApplicationMasterServiceImpl.class);

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
  private TasksHandler tasksHandler;

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

  public ApplicationMasterServiceImpl(ApplicationMasterSpecification specification) {
    this(specification,
      new ResourceManagerConnectionHandlerImpl(specification.getConfiguration()),
      new ContainerManagerConnectionHandlerImpl(specification.getConfiguration()));
  }

  public ApplicationMasterServiceImpl(ApplicationMasterSpecification specification, ResourceManagerConnectionHandler<AMRMProtocol> rmHandler,
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
    Log.info("Starting the flow runner.");
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

    Log.debug("Minimum Cluster Resource {}.", minClusterResource);
    Log.debug("Maximum Cluster Resource {}.", maxClusterResource);

    /** Gets all container group parameters*/
    List<TaskSpecification> tasks = specification.getAllContainerGroups();

    if(tasks.size() < 1) {
      Log.info("No containers have been configured to be started. Stopping now");
      stop();
    }

    /** Iterate through all container groups, initialize and start them. */
    tasksHandler = new TasksHandler(ImmutableList.copyOf(tasks));
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
    int totalFailures = tasksHandler.getFailures();

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
    if(! tasksHandler.process()) {
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
    int totalFailures = tasksHandler.getFailures();

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
   * Adds a task to be executed.
   *
   * @param specification of the task to be added for execution.
   */
  @Override
  public void addTask(TaskSpecification specification) {
    tasksHandler.removeTaskSpecification(specification);
  }

  /**
   * Removes a task from run
   *
   * @param specification of the task to be removed.
   */
  @Override
  public void removeTask(TaskSpecification specification) {
    tasksHandler.removeTaskSpecification(specification);
  }

  /**
   * Makes a request to allocate a container.
   *
   * @param requestId
   * @param requests
   * @return
   */
  private AMResponse allocate(int requestId, List<ResourceRequest> requests, List<ContainerId> releases) {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(requestId);
    req.setApplicationAttemptId(getSpecification().getApplicationAttemptId());
    req.setProgress(10.0f); // We need something sensible here.
    req.addAllAsks(requests);
    if(releases.size() > 0) {
      req.addAllReleases(releases);
    }
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
  public class TasksHandler {

    /**
     * Lists of tasks that are not yet running, but are ready for to be run.
     * When a task is running in container, it's moved to <code>runningTaskQueue</code>
     * Upon failures, a task could be added back to this queue.
     */
    private final Map<String, TaskSpecification> readyToRunQueue = Maps.newConcurrentMap();

    /**
     * Lists of tasks that are currently running within a container.
     */
    private final Map<String, TaskSpecification> runningTaskQueue = Maps.newConcurrentMap();

    /**
     * List of containers to be released.
     */
    private final List<ContainerId> toRelease = new LinkedList<ContainerId>();

    /**
     * Keeps track of containers that was released or not. false if not, true if released.
     */
    private final Map<ContainerId, Boolean> releaseContainerTab = Maps.newConcurrentMap();

    /**
     * Mapping from task specification id to the container they are assigned to.
     * This map is populated only for the running tasks.
     */
    private final Map<String, ContainerId> taskToContainerId = Maps.newConcurrentMap();

    /**
     * Mapping from container ids to task. This mapping is there only for tasks that
     * are running within a container.
     */
    private final Map<ContainerId, String> containerIdToTask = Maps.newConcurrentMap();

    /**
     * Metric of number of times a given task has failed.
     */
    private final Map<String, Integer> failedTaskMetrics = Maps.newHashMap();

    /**
     * Total failed tasks.
     */
    private Integer totalFailedTasks = 0;

    /**
     * Flag indicating whether group of containers managed by this class should be stopped.
     */
    private volatile boolean stopping = false;

    /**
     * Factory for container launch context
     */
    private final ContainerLaunchContextFactory containerLaunchContextFactory;

    /**
     * Map of container id to container handler.
     */
    private final Map<ContainerId, ContainerHandler> containerMgrs = Maps.newConcurrentMap();

    /**
     * Sample Task specification for creating empty requests.
     */
    private final TaskSpecification sampleSpecification;


    /**
     * Creates an instance of manager that is managing this group of containers.
     *
     * @param specifications for this group of containers.
     */
    public TasksHandler(ImmutableList<TaskSpecification> specifications) {
      /** Based on the limitations of the cluster, a launch context factory is created. */
      containerLaunchContextFactory =
        new ContainerLaunchContextFactory(minClusterResource, maxClusterResource);

      sampleSpecification = specifications.get(0);

      /** Add the initial task specification to ready to run queue. */
      for(TaskSpecification specification : specifications) {
        addTaskSpecification(specification);
      }
    }

    public synchronized void addTaskSpecification(TaskSpecification specification) {

    }

    public synchronized void removeTaskSpecification(TaskSpecification specification) {

    }

    /**
     * Allocates, monitors and reallocates containers.
     *
     * @return true to keep going; false otherwise.
     */
    public boolean process() {
      Log.info("Starting TasksHandler process");

      if(shouldProceed()) {

        /**
         * We go through the list of entries in <code>readyToRunQueue</code>, if they are
         * not already in the <code>runningTaskQueue</code> we make the request for them.
         * NOTE: When ever we make a request to YARN they would be the whole request as
         * YARN to override the previous request.
         */
        List<ResourceRequest> resourceRequests = Lists.newArrayList();

        int toBeRequested = 0;
        int toBeReleased = toRelease.size();

        Log.info("Number of tasks in queue waiting to be requested for container is {}", readyToRunQueue.size());
        for(TaskSpecification specification : readyToRunQueue.values()) {
          /** Initialize the failed task counter. */
          if(! failedTaskMetrics.containsKey(specification.getId())) {
            failedTaskMetrics.put(specification.getId(), 0);
          }
          if(runningTaskQueue.containsKey(specification.getId())) {
            Log.warn("Task with id {} already running. This should never happen.", specification.getId());
            continue;
          }
          Log.info("Preparing to request a container for a task with ID {}", specification.getId());
          ResourceRequest req = containerLaunchContextFactory.createResourceRequest(specification);
          req.setNumContainers(specification.getNumInstances());
          resourceRequests.add(req);
          toBeRequested++;
        }

        /**
         * If there are no resources to be requested from resource manager, we need to still
         * make a make a call with not asks. That is don't use <code>setNumContainers</code>.
         */
        if(resourceRequests.isEmpty()) {
          Log.info("Resource request is empty adding an empty resource request.");
          ResourceRequest req = containerLaunchContextFactory.createResourceRequest(sampleSpecification);
          resourceRequests.add(req);
        }

        /**
         * Create a unique request for each allocate request, include containers to be requested and containers
         * to be released.
         */
        Log.info("Requesting {} containers, Releasing {} containers.", toBeRequested, toBeReleased);
        AMResponse response = allocate(requestId.incrementAndGet(), resourceRequests, toRelease);

        /**
         * YARN Returned some containers for us to assign some tasks to it.
         */
        List<Container> newContainers = response.getAllocatedContainers();

        /** Clear up toRelease object, so that new once can be added to it. */
        toRelease.clear();

        Log.info("Received {} new allocated containers, Requested {}.", newContainers.size(), toBeRequested);

        /** Iterate through each container and assign them to a non running task specification */
        for(final Container container : newContainers) {
          Log.info("Assigning a task to the container {}", container.toString());

          /** Add to the tab a the container id that needs to be released. */
          releaseContainerTab.put(container.getId(), false);

          /**
           * We want to make sure that we do not use the container that was already assigned to some task
           * in past. So, we check if it was already assigned. If it was assigned, we request it to released
           * back.
           */
          if(containerMgrs.containsKey(container.getId())) {
            Log.warn("Was allocated a container with id {} that was running a task in past. " +
              "Will not use that container. Requesting it to be released.", container.getId());

            /** If the container is still running, then make sure we do not release that. */
            if(containerMgrs.get(container.getId()) == null) {
              toRelease.add(container.getId());
            }
            continue;
          }

          /**
           * For a given container, we need to find a task specification that matches the container
           * specification. The tasks are picked up from readyToRunQueue to be matched against the
           * container at hand.
           */
          Optional<TaskSpecification> matchingSpec = Iterators.tryFind(readyToRunQueue.values().iterator(),
            new Predicate<TaskSpecification>() {
              @Override
              public boolean apply(@Nullable TaskSpecification input) {
                Resource resource = container.getResource();
                if (resource.getMemory() == input.getMemory()) {
                  Log.info("Found matching task specification for container {}", container.toString());
                  return true;
                }
                return false;
              }
            });

          /**
           * If we have found a specification that matches the container allocated, start it and add that
           * the list of running containers after starting it.
           */

          if(matchingSpec.isPresent()) {
            Log.info("Matching container found. Assigning task to it.");

            /** Create a container handler and assign a task specification to it. */
            TaskSpecification specification = matchingSpec.get();
            ContainerHandler ch = new ContainerHandler(container, specification);
            ch.start();

            /** Add the container to container manager. */
            containerMgrs.put(container.getId(), ch);

            /**
             * Now a task specification is assigned to a container, move the task specification from
             * readyToRunQueue to runningTaskQueue.
             */
            readyToRunQueue.remove(specification.getId());
            runningTaskQueue.put(specification.getId(), specification);

            /**
             * Keep a map from container id to task id and task id to container id.
             */
            taskToContainerId.put(specification.getId(), container.getId());
            containerIdToTask.put(container.getId(), specification.getId());
          } else {
            Log.warn("No matching task specification found for the container. Container ID {}. Requesting release.", container.toString());
            toRelease.add(container.getId());
          }
        }

        /**
         * Now, we get the status of all the containers, if there are some failed container, then, we start them
         * again.
         */
        for(ContainerStatus status : response.getCompletedContainersStatuses()) {
          int exitStatus = status.getExitStatus();
          ContainerId containerId = status.getContainerId();

          /** If the container has not yet completed, then we have nothing to do, we let it run. */
          if(status.getState() != ContainerState.COMPLETE) {
            Log.info("Container id {} is still running or is new. So skipping it.", containerId.toString());
            continue;
          }

          /** Mark this container as released. */
          releaseContainerTab.put(containerId, true);

          if(! containerIdToTask.containsKey(containerId)) {
            Log.warn("Container id {} was not associated with any tasks. This should not be happening. Check logic");
            continue;
          }

          String taskId = containerIdToTask.get(containerId);
          Log.info("Status of container id {}, task id {} is {}", new Object[] { containerId, taskId, exitStatus});

          if(! runningTaskQueue.containsKey(taskId)) {
            Log.warn("Task id {} was supposedly assigned to container with id {}. " +
              "Seems like something is wrong. Check logic.", taskId, containerId);
            continue;
          }

          TaskSpecification specification = runningTaskQueue.get(taskId);

          /**
           * Now if task within the container has exited with non-zero code, then we need to move the task
           * from runningTaskQueue to readyToRunQueue.
           */
          runningTaskQueue.remove(taskId);
          if(exitStatus != 0) {
            readyToRunQueue.put(taskId, specification);
            int taskFailure = failedTaskMetrics.get(taskId);
            totalFailedTasks++;
            failedTaskMetrics.put(taskId, taskFailure++);
          } else {
            /**
             * Container and task within the container has terminated with exit code of zero, so just remove
             * if from running state, but do not add it to readyToRunQueue. This might be the case when user
             * of other system has requested this container to be stopped.
             *
             * Make sure the task associated with that container is removed from runningTaskQueue and also
             * make sure there is nothing in readyToRunQueue.
             */
            if(readyToRunQueue.containsKey(taskId)) {
              readyToRunQueue.remove(taskId);
            }
          }
        }
      }
      return shouldProceed();
    }

    /**
     * Number of containers that failed. Inclusive of all stages of failures.
     *
     * @return number of failures.
     */
    public int getFailures() {
      return totalFailedTasks;
    }

    /**
     * Returns whether group allocation, monitoring or reallocation should continue.
     *
     * @return true to continue; false otherwise.
     */
    private boolean shouldProceed() {
      return !stopping;
    }

    /**
     * Stop all the container managers in this group.
     */
    public void stop() {
      stopping = true;

      /**
       * Till all the containers are released or we hit a timeout we don't exit this loop
       */
      StopWatch releaseTimer = new StopWatch();
      boolean keepRunning = true;

      Log.info("Application Master service has been requested to be stopped.");

      while(keepRunning) {

        /** We wait for 120 seconds to stop. */
        if(releaseTimer.getTime() > 120*1000 )  {
          keepRunning = false;
          /** We let it run one last time. */
        }

        /** We pickup all the containers that are currently running. */
        boolean foundNoContainersToBeReleased = true;
        for(Map.Entry<ContainerId, Boolean> container : releaseContainerTab.entrySet()) {
          ContainerId containerId = container.getKey();

          /** The container is not released yet. So, check if there is task running within it. */
          if(releaseContainerTab.get(containerId) == false) {
            foundNoContainersToBeReleased = false;
            if(containerMgrs.containsKey(containerId) && containerMgrs.get(containerId) != null) {
              Log.info("Attempting to stop container {}", containerId);
              /** Stop the container. */
              containerMgrs.get(containerId).stopAndWait();
            }
            Log.info("Adding the container {} to be released.", containerId);
            /** Add the container to be released. */
            toRelease.add(containerId);
          }
        }

        /** We found no containers that are yet to be released and there is nothing in release list.*/
        if(foundNoContainersToBeReleased && toRelease.isEmpty()) {
          keepRunning = true;
        }

        /** Create a zero allocation request with all the release requests to RM */
        List<ResourceRequest> resourceRequests = Lists.newArrayList();
        ResourceRequest req = containerLaunchContextFactory.createResourceRequest(sampleSpecification);
        req.setNumContainers(0);
        resourceRequests.add(req);

        /** Make a request to RM */
        AMResponse response = allocate(requestId.incrementAndGet(), resourceRequests, toRelease);
        List<Container> newContainers = response.getAllocatedContainers();

        /** Clear the list */
        toRelease.clear();

        /** If there were any new containers allocated from the previous request, release them immediately. */
        if(! newContainers.isEmpty()) {
          for(Container container : newContainers) {
            releaseContainerTab.put(container.getId(), false);
            toRelease.add(container.getId());
          }
        }

        /** Check the status of each containers */
        List<ContainerStatus> containerStatuses = response.getCompletedContainersStatuses();
        for(ContainerStatus status : containerStatuses) {
          if(status.getState() != ContainerState.COMPLETE) {
            toRelease.add(status.getContainerId());
            continue;
          }
          releaseContainerTab.put(status.getContainerId(), true);
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
    private final TaskSpecification specification;
    private final ContainerLaunchContextFactory containerLaunchContextFactory;
    private ContainerManager containerMgr;
    private ContainerStatus status;
    private int checkFailures = 0;

    public ContainerHandler(Container container, TaskSpecification specification) {
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
