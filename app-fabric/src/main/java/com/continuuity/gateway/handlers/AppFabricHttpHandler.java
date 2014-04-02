package com.continuuity.gateway.handlers;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramStatus;
import com.continuuity.app.services.RunIdentifier;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.ServerException;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;
import com.continuuity.internal.app.deploy.SessionInfo;
import com.continuuity.internal.app.runtime.schedule.Scheduler;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpRequest;
import com.continuuity.http.NettyHttpService;
import com.continuuity.app.services.EntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AppFabricHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricHttpHandler.class);
  /**
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

  /**
   * Number of seconds for timing out a service endpoint discovery.
   */
  private static final long DISCOVERY_TIMEOUT_SECONDS = 3;

  /**
   * Maintains a mapping of transient session state. The state is stored in memory,
   * in case of failure, all the current running sessions will be terminated. As
   * per the current implementation only connection per account is allowed to upload.
   */
  private final Map<String, SessionInfo> sessions = Maps.newConcurrentMap();

  /**
   * Used to manage datasets. TODO: implement and use DataSetService instead
   */
  private final DataSetAccessor dataSetAccessor;
  /**
   * Configuration object passed from higher up.
   */
  private final CConfiguration configuration;

  /**
   * Factory for handling the location - can do both in either Distributed or Local mode.
   */
  private final LocationFactory locationFactory;

  /**
   * DeploymentManager responsible for running pipeline.
   */
  private final ManagerFactory managerFactory;

  /**
   * Authorization Factory used to create handler used for authroizing use of endpoints.
   */
  private final AuthorizationFactory authFactory;

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  /**
   * Discovery service client to discover other services.
   */
  private final DiscoveryServiceClient discoveryServiceClient;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  /**
   * The directory where the uploaded files would be placed.
   */
  private final String archiveDir;

  /**
   * App fabric output directory.
   */
  private final String appFabricDir;

  // We need it here now to be able to reset queues data
  private final QueueAdmin queueAdmin;
  // We need it here now to be able to reset queues data
  private final StreamAdmin streamAdmin;

  /**
   * Timeout to upload to remote app fabric.
   */
  private static final long UPLOAD_TIMEOUT = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

  /**
   * Timeout to get response from metrics system.
   */
  private static final long METRICS_SERVER_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  //TODO: GatewayAuthenticator, Scheduler
  @Inject
  public AppFabricHttpHandler(CConfiguration configuration, DataSetAccessor dataSetAccessor,
                              LocationFactory locationFactory,
                              ManagerFactory managerFactory, AuthorizationFactory authFactory,
                              StoreFactory storeFactory, ProgramRuntimeService runtimeService,
                              DiscoveryServiceClient discoveryServiceClient,
                              QueueAdmin queueAdmin, StreamAdmin streamAdmin
                              ) {
    //super(authenticator);
    this.dataSetAccessor = dataSetAccessor;
    this.locationFactory = locationFactory;
    this.configuration = configuration;
    this.managerFactory = managerFactory;
    this.authFactory = authFactory;
    this.runtimeService = runtimeService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.store = storeFactory.create();
    this.appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR,
                                          System.getProperty("java.io.tmpdir"));
    this.archiveDir = this.appFabricDir + "/archive";
  }

  @Path("/trial/ping")
  @GET
  public void Get(HttpRequest request, HttpResponder response){
    response.sendString(HttpResponseStatus.OK, "hello");
  }
  /**
   * Returns status of a flow.
   */

  @Path("/apps/{app-id}/flows/{flow-id}/status")
  @GET
  public void flowStatus(HttpRequest request, HttpResponder responder,
                         @PathParam("app-id") final String appId,
                         @PathParam("flow-id") final String flowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(flowId);
    id.setType(EntityType.FLOW);
    runnableStatus(request, responder, id);
  }

  private void runnableStatus(HttpRequest request, HttpResponder responder, ProgramId id) {
    String accountId = "developer"; //TODO: getAuthenticatedAccountId(request);
    id.setAccountId(accountId);
    try {
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
      ProgramStatus status = getProgramStatus(token, id);
      if (status.getStatus().equals("NOT_FOUND")){
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        JsonObject o = new JsonObject();
        o.addProperty("status", status.getStatus());
        responder.sendJson(HttpResponseStatus.OK, o);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private ProgramStatus getProgramStatus(AuthToken token, ProgramId id)
    throws ServerException, AppFabricServiceException {

      return status(token, id);

  }

  public synchronized ProgramStatus status(AuthToken token, ProgramId id)
    throws AppFabricServiceException {

    try {
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(id);

      if (runtimeInfo == null) {
        if (id.getType() != EntityType.WEBAPP) {
          //Runtime info not found. Check to see if the program exists.
          String spec = getSpecification(id);
          if (spec == null || spec.isEmpty()) {
            // program doesn't exist
            return new ProgramStatus(id.getApplicationId(), id.getFlowId(), null, "NOT_FOUND");
          } else {
            // program exists and not running. so return stopped.
            return new ProgramStatus(id.getApplicationId(), id.getFlowId(), null,
                                     ProgramController.State.STOPPED.toString());
          }
        } else {
          // TODO: Fetching webapp status is a hack. This will be fixed when webapp spec is added.
          Location webappLoc = null;
          try {
            Id.Program programId = Id.Program.from(id.getAccountId(), id.getApplicationId(), id.getFlowId());
            webappLoc = Programs.programLocation(locationFactory, appFabricDir, programId, Type.WEBAPP);
          } catch (FileNotFoundException e) {
            // No location found for webapp, no need to log this exception
          }

          if (webappLoc != null && webappLoc.exists()) {
            // webapp exists and not running. so return stopped.
            return new ProgramStatus(id.getApplicationId(), id.getFlowId(), null,
                                     ProgramController.State.STOPPED.toString());
          } else {
            // webapp doesn't exist
            return new ProgramStatus(id.getApplicationId(), id.getFlowId(), null, "NOT_FOUND");
          }
        }
      }

      Id.Program programId = runtimeInfo.getProgramId();
      RunIdentifier runId = new RunIdentifier(runtimeInfo.getController().getRunId().getId());

      // NOTE: This was a temporary hack done to map the status to something that is
      // UI friendly. Internal states of program controller are reasonable and hence
      // no point in changing them.
      String status = controllerStateToString(runtimeInfo.getController().getState());
      return new ProgramStatus(programId.getApplicationId(), programId.getId(), runId, status);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  private String controllerStateToString(ProgramController.State state) {
    if (state == ProgramController.State.ALIVE) {
      return "RUNNING";
    }
    if (state == ProgramController.State.ERROR) {
      return "FAILED";
    }
    return state.toString();
  }


  private String getSpecification(ProgramId id)
    throws AppFabricServiceException {

    ApplicationSpecification appSpec;
    try {
      appSpec = store.getApplication(new Id.Application(new Id.Account(id.getAccountId()), id.getApplicationId()));
      if (appSpec == null) {
        return "";
      }

      String runnableId = id.getFlowId();
      if (id.getType() == EntityType.FLOW && appSpec.getFlows().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getFlows().get(id.getFlowId()));
      } else if (id.getType() == EntityType.PROCEDURE && appSpec.getProcedures().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getProcedures().get(id.getFlowId()));
      } else if (id.getType() == EntityType.MAPREDUCE && appSpec.getMapReduce().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getMapReduce().get(id.getFlowId()));
      } else if (id.getType() == EntityType.WORKFLOW && appSpec.getWorkflows().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getWorkflows().get(id.getFlowId()));
      } else if (id.getType() == EntityType.APP) {
        return GSON.toJson(makeAppRecord(appSpec));
      }
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw  new AppFabricServiceException("Could not retrieve application spec for " +
                                             id.toString() + ", reason: " + e.getMessage());
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
    return "";
  }

  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(ProgramId identifier) {
    Type type = Type.valueOf(identifier.getType().name());
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               identifier.getAccountId(), identifier.getFlowId());

    Id.Program programId = Id.Program.from(identifier.getAccountId(),
                                           identifier.getApplicationId(),
                                           identifier.getFlowId());

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

   /* -----------------  helpers to return Jsion consistently -------------- */

  private static Map<String, String> makeDataSetRecord(String name, String classname,
                                                       DataSetSpecification specification) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Dataset");
    builder.put("id", name);
    builder.put("name", name);
    if (classname != null) {
      builder.put("classname", classname);
    }
    if (specification != null) {
      builder.put("specification", GSON.toJson(specification));
    }
    return builder.build();
  }

  private static Map<String, String> makeStreamRecord(String name, StreamSpecification specification) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Stream");
    builder.put("id", name);
    builder.put("name", name);
    if (specification != null) {
      builder.put("specification", GSON.toJson(specification));
    }
    return builder.build();
  }

  private static Map<String, String> makeAppRecord(ApplicationSpecification spec) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "App");
    builder.put("id", spec.getName());
    builder.put("name", spec.getName());
    if (spec.getDescription() != null) {
      builder.put("description", spec.getDescription());
    }
    return builder.build();
  }

  private static Map<String, String> makeFlowRecord(String app, FlowSpecification spec) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Flow");
    builder.put("app", app);
    builder.put("id", spec.getName());
    builder.put("name", spec.getName());
    if (spec.getDescription() != null) {
      builder.put("description", spec.getDescription());
    }
    return builder.build();
  }

  private static Map<String, String> makeProcedureRecord(String app, ProcedureSpecification spec) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Procedure");
    builder.put("app", app);
    builder.put("id", spec.getName());
    builder.put("name", spec.getName());
    if (spec.getDescription() != null) {
      builder.put("description", spec.getDescription());
    }
    return builder.build();
  }

  private static Map<String, String> makeMapReduceRecord(String app, MapReduceSpecification spec) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Mapreduce");
    builder.put("app", app);
    builder.put("id", spec.getName());
    builder.put("name", spec.getName());
    if (spec.getDescription() != null) {
      builder.put("description", spec.getDescription());
    }
    return builder.build();
  }

  private static Map<String, String> makeWorkflowRecord(String app, WorkflowSpecification spec) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Workflow");
    builder.put("app", app);
    builder.put("id", spec.getName());
    builder.put("name", spec.getName());
    if (spec.getDescription() != null) {
      builder.put("description", spec.getDescription());
    }
    return builder.build();
  }

}
