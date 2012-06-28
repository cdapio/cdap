package com.continuuity.common.service.distributed.yarn;

import com.continuuity.common.service.distributed.ClientService;
import com.continuuity.common.service.distributed.ClientSpecification;
import com.continuuity.common.service.distributed.ContainerGroupSpecification;
import com.continuuity.common.service.distributed.MasterConnectionHandler;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ClientServiceImpl extends AbstractScheduledService implements ClientService {
  private static Logger Log = LoggerFactory.getLogger(ClientServiceImpl.class);

  private final ClientSpecification specification;
  private final MasterConnectionHandler<ClientRMProtocol> appMgrHandler;
  private ClientRMProtocol appMgr;
  private ApplicationId applicationId;
  private ApplicationReport applicationReport;
  private static final Set<YarnApplicationState> FAILED
    = EnumSet.of(YarnApplicationState.FAILED, YarnApplicationState.KILLED);
  private volatile boolean stopped = true;
  private volatile boolean failed = false;

  /**
   * Handler for managing connection to Resource manager.
   */
  private static class ApplicationManagerConnectionHandler implements MasterConnectionHandler<ClientRMProtocol> {
    private static final Logger Log = LoggerFactory.getLogger(ApplicationManagerConnectionHandler.class);
    private final Configuration configuration;
    private final YarnRPC rpc;

    public ApplicationManagerConnectionHandler(Configuration configuration) {
      this.configuration = configuration;
      this.rpc = YarnRPC.create(configuration);
    }

    /**
     * Connects to RM.
     *
     * @return a connection handler.
     */
    @Override
    public ClientRMProtocol connect() {
      YarnConfiguration yarnConfiguration = new YarnConfiguration(configuration);
      InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConfiguration.get(
        YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS
      ));
      return (ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, configuration);
    }
  }

  public ClientServiceImpl(ClientSpecification specification) {
    this(specification, new ApplicationManagerConnectionHandler(specification.getConfiguration()));
  }

  public ClientServiceImpl(ClientSpecification specification, ApplicationManagerConnectionHandler appMgrHandler) {
    Preconditions.checkNotNull(specification);
    this.specification = specification;
    this.appMgrHandler = appMgrHandler;
  }

  @Override
  public void startUp() {
    Log.info("Connecting to resource manager ...");
    appMgr = appMgrHandler.connect();

    /** Get a new application id. */
    GetNewApplicationResponse response = null;
    try {
      response = appMgr.getNewApplication(Records.newRecord(GetNewApplicationRequest.class));
    } catch (YarnRemoteException e) {
      Log.error("Failed to acquire application id from resource manager. Reason : {}", e.getMessage());
      stop();
      return;
    }

    /** Retrieve application id from the response from connecting to resource manager. */
    applicationId = response.getApplicationId();

    /** Build a container launch context to create the AM container. */
    ContainerLaunchContextFactory containerLaunchContextFactory =
      new ContainerLaunchContextFactory(response.getMinimumResourceCapability(), response.getMaximumResourceCapability());

    ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
    context.setApplicationId(applicationId);
    context.setApplicationName(specification.getApplicationName());

    try {
      String user = UserGroupInformation.getCurrentUser().getShortUserName();

      /** Build the container specification for launching application manager. */
      ContainerGroupSpecification.Builder builder = new ContainerGroupSpecification.Builder();
      builder.setMemory(specification.getMemory());
      for(String command : specification.getCommands()) {
        builder.addCommand(command);
      }
      for(Map.Entry<String, String> entry : specification.getEnvironment().entrySet()) {
        builder.addEnv(entry.getKey(), entry.getValue());
      }
      builder.setUser(user);
      builder.setPriority(0);
      ContainerGroupSpecification appMasterSpecs = builder.create();

      /** Create and populate container launch context. */
      ContainerLaunchContext clc = containerLaunchContextFactory.create(appMasterSpecs);
      context.setAMContainerSpec(clc);
      context.setUser(appMasterSpecs.getUser());
      context.setQueue(specification.getQueue());
      context.setPriority(ContainerLaunchContextFactory.createPriority(appMasterSpecs.getPriority()));

      /** Create application submission request and submit the application */
      SubmitApplicationRequest request = Records.newRecord(SubmitApplicationRequest.class);
      request.setApplicationSubmissionContext(context);
      appMgr.submitApplication(request);
      stopped = false;
    } catch (YarnRemoteException e) {
      Log.error("Unable to submit the application to YARN for running. Reason : {}", e.getMessage());
      stop();
    } catch (IOException e) {
      Log.error("Unable to retrieve user for set context for running application master. Reason : {}", e.getMessage());
      stop();
    }
  }

  @Override
  public void shutDown() {
    if(! hasStopped()) {
      if(applicationReport == null) {
        if(applicationId != null) {
          KillApplicationRequest request = Records.newRecord(KillApplicationRequest.class);
          request.setApplicationId(applicationId);
          try {
            appMgr.forceKillApplication(request);
            Log.info("Application {} was killed.", applicationId.toString());
          } catch (YarnRemoteException e) {
            Log.error("There was issue killing the application. Reason : {}", e.getMessage());
          }
        }
      }
    }
  }

  /**
   * Run one iteration of the scheduled task. If any invocation of this method throws an exception,
   * the service will transition to the {@link com.google.common.util.concurrent.Service.State#FAILED} state and
   * this method will no longer be called.
   */
  @Override
  protected void runOneIteration() throws Exception {
    if(! isRunning()) {
      return;
    }

    /** Get the application status */
    try {
      GetApplicationReportRequest request = Records.newRecord(GetApplicationReportRequest.class);
      request.setApplicationId(applicationId);
      GetApplicationReportResponse response = appMgr.getApplicationReport(request);
      applicationReport = response.getApplicationReport();
      if(applicationReport == null) {
        Log.warn("Have not received application report.");
      } else if(FAILED.contains(applicationReport.getYarnApplicationState())) {
        stopped = true;
        failed = true;
      } else if(applicationReport.getYarnApplicationState() == YarnApplicationState.FINISHED) {
        stopped = true;
        failed = false;
      }
    } catch (YarnRemoteException e) {
      Log.warn("Unable to get status of application {}. Reason : {}", applicationId.toString(), e.getMessage());
    }
  }

  /**
   * Returns the {@link com.google.common.util.concurrent.AbstractScheduledService.Scheduler} object used to configure this service.  This method will only be
   * called once.
   */
  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS);
  }

  /**
   * Returns the specification used to configure this service.
   *
   * @return specification to configure this service.
   */
  @Override
  public ClientSpecification getSpecification() {
    return specification;
  }

  /**
   * Returns the id associated with this application.
   *
   * @return id of this application.
   */
  @Override
  public ApplicationId getApplicationId() {
    return applicationId;
  }

  /**
   * Returns a report of the current application.
   *
   * @return report of current application if successful; else null.
   */
  @Override
  public ApplicationReport getReport() {
    return applicationReport;
  }

  /**
   * Returns the state of the application.
   *
   * @return true if running; false otherwise.
   */
  @Override
  public boolean hasStopped() {
    return stopped;
  }

  /**
   * Returns true if failed, else false.
   *
   * @return true if failed; false otherwise.
   */
  @Override
  public boolean hasFailed() {
    return failed;
  }
}
