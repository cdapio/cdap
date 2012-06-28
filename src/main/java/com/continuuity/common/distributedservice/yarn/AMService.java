package com.continuuity.common.distributedservice.yarn;

import com.continuuity.common.distributedservice.*;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Application Manager service is base implementation for YARN application manager.
 */
public class AMService extends AbstractScheduledService implements ApplicationMasterService {
  private static final Logger Log = LoggerFactory.getLogger(AMService.class);

  /**
   * Parameters settings for application master.
   */
  private final ApplicationMasterSpecification specification;

  /**
   * Resource manager connection handler.
   */
  private final MasterConnectionHandler<AMRMProtocol> rmHandler;

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
   * Handler to resource manager.
   */
  private AMRMProtocol resourceMgr;

  public AMService(ApplicationMasterSpecification specification) {
    this(specification,
      new RMConnectionHandler(specification.getConfiguration()),
      new CMConnectionHandler(specification.getConfiguration()));
  }

  public AMService(ApplicationMasterSpecification specification, MasterConnectionHandler<AMRMProtocol> rmHandler,
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
      containerGroupHandlers.add(new ContainerGroupHandler(this, clp));
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
   * Returns an instance to resource manager.
   *
   * @return instance of resource manager.
   */
  @Override
  public AMRMProtocol getResourceManager() {
    return resourceMgr;
  }

  /**
   * Pair of min and max cluster resources.
   *
   * @return pair of min & max cluster resources.
   */
  @Override
  public ImmutablePair<Resource, Resource> getClusterResourcesRange() {
    return new ImmutablePair<Resource, Resource>(minClusterResource, maxClusterResource);
  }

  /**
   * Return CM connection handler.
   *
   * @return handler to connection manager.
   */
  @Override
  public ContainerManagerConnectionHandler getContainerManagerConnection() {
    return cmHandler;
  }

  /**
   * Return RM connection handler.
   *
   * @return handler to resource manager.
   */
  @Override
  public MasterConnectionHandler getMasterConnection() {
    return rmHandler;
  }
}
