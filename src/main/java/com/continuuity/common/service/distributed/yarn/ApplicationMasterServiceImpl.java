package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.*;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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

    // Register the application master with the resource manager.
    RegisterApplicationMasterResponse registration;

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

    minClusterResource = registration.getMinimumResourceCapability();
    maxClusterResource = registration.getMaximumResourceCapability();

    Log.debug("Minimum Cluster Resource {}.", minClusterResource);
    Log.debug("Maximum Cluster Resource {}.", maxClusterResource);

    // Gets all container group parameters
    List<TaskSpecification> tasks = specification.getTaskSpecifications();

    if(tasks.size() < 1) {
      Log.info("No containers have been configured to be started. Stopping now");
      stop();
    }

    // Initial list of tasks provided.
    tasksHandler = new TasksHandler(ImmutableList.copyOf(tasks));
    Log.info("Application Master service started.");
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
    /**
     * Iterate through all container groups.
     */
    if(! tasksHandler.process()) {
      Function<Void, Void> hook = specification.getOnShutdownHook();
      if(hook != null) {
        hook.apply(null);
      }
      stop();
    }
  }

  /**
   * Shuts down the Application service.
   */
  @Override
  protected void shutDown() {
    Log.info("Shutting down the application service.");

    // Ask the tasks handler to shutdown.
    tasksHandler.stop();

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
   * @return An instance of
   * {@link com.google.common.util.concurrent.AbstractScheduledService.Scheduler}
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
  public void addTask(List<TaskSpecification> specification) {
    tasksHandler.addTaskSpecification(specification);
  }

  /**
   * Removes a task from run
   *
   * @param specification of the task to be removed.
   */
  @Override
  public void removeTask(List<TaskSpecification> specification) {
    tasksHandler.removeTaskSpecification(specification);
  }

  /**
   * Number of tasks to be yet allocated containers to run.
   *
   * @return number of tasks that are still waiting to be allocated container.
   */
  @Override
  public int getPendingTasks() {
    return tasksHandler.getPendingTasks();
  }

  /**
   * Returns number of tasks that are pending release.
   *
   * @return number of tasks that are still pending release.
   */
  @Override
  public int getPendingReleases() {
    return tasksHandler.getPendingReleases();
  }

  /**
   * Makes a request to allocate a container.
   *
   * @param requestId The id of the request
   * @param requests  A List of ResourceRequests
   * @return The Application Master reponse
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
    private final Map<ContainerId, TaskHandler> containerMgrs = Maps.newConcurrentMap();

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
        readyToRunQueue.put(specification.getId(), specification);
      }
    }

    /**
     * Returns number of pending tasks - tasks that waiting for container to be allocated.
     *
     * @return number of tasks waiting for container to be allocated.
     */
    public int getPendingTasks() {
      return readyToRunQueue.size();
    }

    /**
     * Adds task specification to readyToRunQueue.
     *
     * @param specifications of tasks of be added.
     */
    public synchronized void addTaskSpecification(List<TaskSpecification> specifications) {
      for(TaskSpecification addTaskSpecification : specifications) {
        readyToRunQueue.put(addTaskSpecification.getId(), addTaskSpecification);
      }
    }

    /**
     * Puts the containers in toRelease list for them to released next iterations.
     *
     * @param specifications of tasks to be removed.
     */
    public synchronized void removeTaskSpecification(List<TaskSpecification> specifications) {
      for(TaskSpecification removeTaskSpecification : specifications) {
        if(taskToContainerId.containsKey(removeTaskSpecification.getId())) {
          ContainerId removeContainerId = taskToContainerId.get(removeTaskSpecification.getId());
          toRelease.add(removeContainerId);
        }
      }
    }

    /**
     * Allocates, monitors and reallocates containers.
     *
     * @return true to keep going; false otherwise.
     */
    public boolean process() {

      if(shouldProceed()) {

        /**
         * We go through the list of entries in <code>readyToRunQueue</code>, if they are
         * not already in the <code>runningTaskQueue</code> we make the request for them.
         * NOTE: When ever we make a request to YARN they would be the whole request as
         * YARN would override the previous request.
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
          Log.info("Request a container for a task with ID {} with resource capacity : Memory {} MB",
            specification.getId(), specification.getMemory());
          ResourceRequest req = containerLaunchContextFactory.createResourceRequest(specification);
          req.setNumContainers(specification.getNumInstances());
          resourceRequests.add(req);
          toBeRequested++;
        }

        /**
         * Create a unique request for each allocate request, include containers to be requested and containers
         * to be released.
         */
        Log.info("Requesting {} containers, Releasing {} container(s).", toBeRequested, toBeReleased);
        AMResponse response = allocate(requestId.incrementAndGet(), resourceRequests, toRelease);

        /**
         * YARN Returned some containers for us to assign some tasks to it.
         */
        List<Container> newContainers = response.getAllocatedContainers();

        /** Clear up toRelease object, so that new once can be added to it. */
        toRelease.clear();

        Log.info("{} out of {} new containers were allocated.", newContainers.size(), toBeRequested);

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
            TaskHandler ch =
              new TaskHandler(cmHandler, minClusterResource, maxClusterResource, container, specification);
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
        Log.info("Checking the statuses of containers.");
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

          if(! containerMgrs.containsKey(containerId)) {
            Log.info("Container {} was allocated, but was not assigned any task.", containerId.getId());
            continue;
          } else {
            Log.info("Waiting for container '{}' to stop as it has either failed or completed.", containerId);
            containerMgrs.get(containerId).stopAndWait();
            containerMgrs.put(containerId, null);
          }

          String taskId = containerIdToTask.get(containerId);
          Log.info("Status of container id {}, task id {} is {}", new Object[] { containerId, taskId, exitStatus});

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
      releaseTimer.start();

      boolean keepRunning = true;

      Log.info("Tasks handler has been requested to be stopped.");

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
          if (!releaseContainerTab.get(containerId)) {
            foundNoContainersToBeReleased = false;
            if(containerMgrs.containsKey(containerId) && containerMgrs.get(containerId) != null) {
              Log.trace("Attempting to stop container {}", containerId);
              /** Stop the container. */
              containerMgrs.get(containerId).stopAndWait();
            }
            Log.trace("Adding the container {} to be released.", containerId);
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

    /**
     * Number of tasks to be released.
     *
     * @return number of tasks to be released.
     */
    public int getPendingReleases() {
      return toRelease.size();
    }
  }
}
