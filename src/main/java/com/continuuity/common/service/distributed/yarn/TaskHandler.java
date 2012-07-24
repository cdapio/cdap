package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.ContainerManagerConnectionHandler;
import com.continuuity.common.service.distributed.TaskSpecification;
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
 * Responsible for managing a single Task running within a container.
 *
 * <p>
 *   A task is created off the specification provided by the {@code ApplicationMasterService}
 *   It's responsible for starting, stopping and monitoring the container.
 * </p>
 */
public class TaskHandler extends AbstractScheduledService {
  private static final Logger Log = LoggerFactory.getLogger(TaskHandler.class);

  /**
   * Container under which the task runs.
   */
  private final Container container;

  /**
   * Specification of the task.
   */
  private final TaskSpecification specification;

  /**
   * Handler for create container launch context from the task specification.
   */
  private final ContainerLaunchContextFactory containerLaunchContextFactory;

  /**
   * Handler to container manager.
   */
  private ContainerManagerConnectionHandler cmHandler;

  /**
   * Instance of container manager as returned by the {@code ContainerManagerConnectionHandler}.
   */
  private ContainerManager containerMgr;

  /**
   * Status of the container.
   */
  private ContainerStatus status;

  /**
   * Maximum number of times the check for status of container can be sustained before declaring
   * it as a failed task.
   */
  private static final int MAX_CHECK_FAILURES = 100;

  /**
   * Counts the number of check failures.
   */
  private int checkFailures = 0;

  /**
   * Creates an instance of Task Handler.
   *
   * @param cmHandler Container Manager.
   * @param minClusterResource Minimum cluster resource.
   * @param maxClusterResource Maximum cluster resource.
   * @param container Container
   * @param specification Specification of a task.
   */
  public TaskHandler(ContainerManagerConnectionHandler cmHandler,
                     Resource minClusterResource, Resource maxClusterResource,
                     Container container, TaskSpecification specification) {
    this.container = container;
    this.specification = specification;
    this.cmHandler = cmHandler;
    this.containerLaunchContextFactory = new ContainerLaunchContextFactory (
      minClusterResource, maxClusterResource
    );
  }

  @Override
  public void startUp() {

    // Connect to container manager (essentially a node manager).
    containerMgr = cmHandler.connect(container);

    // If we did not get an instance of container manager, then the task cannot be started.
    // So, stop the task.
    if(containerMgr == null) {
      Log.warn("Failed connecting to container manager for container {}", container.toString());
      stop();
      return;
    }

    // Create the task specification to be launched within a container.
    ContainerLaunchContext launchContext = containerLaunchContextFactory.create(specification);
    launchContext.setContainerId(container.getId());

    // Create an start container request.
    StartContainerRequest startRequest = Records.newRecord(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(launchContext);

    Log.info("Starting task {} in container {}.", specification.getId(), container);
    Log.info("Task {} run start request : {}", specification.getId(), startRequest.toString());

    try {
      containerMgr.startContainer(startRequest);
      Log.info("Started task {}", specification.getId());
    } catch (YarnRemoteException e) {
      e.printStackTrace();
      Log.info("Failed starting container {}. Reason : {}", container.toString(), e.getMessage());
      stop();
    } catch (Throwable e) {
      e.printStackTrace();
      Log.info("Horrible happened. Reason : {}", e.getMessage());
      stop();
    }
  }

  @Override
  public void shutDown() {
    Log.info("Stopping task {} running in container {}", specification.getId(), container);

    StopContainerRequest req = Records.newRecord(StopContainerRequest.class);
    req.setContainerId(container.getId());
    try {
      containerMgr.stopContainer(req);
    } catch (YarnRemoteException e) {
      Log.warn("Exception thrown stopping container: " + container, e);
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    Log.info("Checking status of task {}, Container {}", specification.getId(), container.getId().toString());

    GetContainerStatusRequest req = Records.newRecord(GetContainerStatusRequest.class);
    req.setContainerId(container.getId());

    try {
      GetContainerStatusResponse resp = containerMgr.getContainerStatus(req);

      status = resp.getStatus();
      Log.info("Container {} status {}, Diganostics {}.", new Object[] {
        container.getId().toString(), status.toString(), status.getDiagnostics()});

      if (status != null && status.getState() == ContainerState.COMPLETE) {
        Log.info("Container {} has completed. Stopping the container.", container.getId());
        stop();
      }
    } catch (YarnRemoteException e) {
      Log.info("There was problem receiving the status of container {}. Container might be shutting down.",
        container.toString());
      checkFailures++;

    } catch (Throwable e) {
      Log.info("An error was thrown while checking the status of a container. Reason : {}", e.getMessage());
    }

    if(checkFailures > MAX_CHECK_FAILURES) {
      Log.info("Failed after max retry to get status of container {}", container.toString());
      stop();
    }

  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS);
  }
}