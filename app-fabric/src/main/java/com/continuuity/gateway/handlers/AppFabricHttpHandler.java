package com.continuuity.gateway.handlers;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramStatus;
import com.continuuity.app.services.RunIdentifier;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.ServerException;
import com.continuuity.data2.OperationException;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;
import com.continuuity.internal.app.deploy.SessionInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Map;

/**
 *  HttpHandler class for app-fabric requests.
 */
@Path(Constants.Gateway.GATEWAY_VERSION) //this will be removed/changed when gateway goes.
public class AppFabricHttpHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricHttpHandler.class);
  /**
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

  /**
   * Maintains a mapping of transient session state. The state is stored in memory,
   * in case of failure, all the current running sessions will be terminated. As
   * per the current implementation only connection per account is allowed to upload.
   */
  private final Map<String, SessionInfo> sessions = Maps.newConcurrentMap();

  /**
   * Configuration object passed from higher up.
   */
  private final CConfiguration configuration;

  /**
   * Factory for handling the location - can do both in either Distributed or Local mode.
   */
  private final LocationFactory locationFactory;


  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  /**
   * App fabric output directory.
   */
  private final String appFabricDir;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final WorkflowClient workflowClient;

  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  @Inject
  public AppFabricHttpHandler(Authenticator authenticator, CConfiguration configuration,
                              LocationFactory locationFactory,
                              StoreFactory storeFactory,
                              ProgramRuntimeService runtimeService,
                              WorkflowClient workflowClient) {
    super(authenticator);
    this.locationFactory = locationFactory;
    this.configuration = configuration;
    this.runtimeService = runtimeService;
    this.appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR,
                                          System.getProperty("java.io.tmpdir"));

    this.store = storeFactory.create();
    this.workflowClient = workflowClient;
  }

  /**
   * Ping: responds with an OK message.
   */
  @Path("/ping")
  @GET
  public void Get(HttpRequest request, HttpResponder response){
    response.sendString(HttpResponseStatus.OK, "OK");
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
    LOG.info("Status call from AppFabricHttpHandler for app {} flow {}", appId, flowId);
    runnableStatus(request, responder, id);
  }

  /**
   * Returns status of a procedure.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/status")
  public void procedureStatus(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId,
                              @PathParam("procedure-id") final String procedureId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(procedureId);
    id.setType(EntityType.PROCEDURE);
    LOG.info("Status call from AppFabricHttpHandler for app {} procedure {}", appId, procedureId);
    runnableStatus(request, responder, id);
  }

  /**
   * Returns status of a mapreduce.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}/status")
  public void mapreduceStatus(final HttpRequest request, final HttpResponder responder,
                              @PathParam("app-id") final String appId,
                              @PathParam("mapreduce-id") final String mapreduceId) {

    // Get the runnable status
    // If runnable is not running
    //   - Get the status from workflow

    AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
    String accountId = getAuthenticatedAccountId(request);
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(mapreduceId);
    id.setType(EntityType.MAPREDUCE);
    id.setAccountId(accountId);

    try {

      ProgramStatus status = getProgramStatus(token, id);
      if (status.getStatus().equals("NOT_FOUND")) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else if (!status.getStatus().equals("RUNNING")) {
        //Program status is not running, check if it is running as a part of workflow
        String workflowName = getWorkflowName(id.getFlowId());
        workflowClient.getWorkflowStatus(id.getAccountId(), id.getApplicationId(), workflowName,
                                         new WorkflowClient.Callback() {
                                           @Override
                                           public void handle(WorkflowClient.Status status) {
                                             JsonObject o = new JsonObject();
                                             if (status.getCode().equals(WorkflowClient.Status.Code.OK)) {
                                               o.addProperty("status", "RUNNING");
                                             } else {
                                               o.addProperty("status", "STOPPED");
                                             }
                                             responder.sendJson(HttpResponseStatus.OK, o);
                                           }
                                         });
      } else {
        JsonObject o = new JsonObject();
        o.addProperty("status", status.getStatus());
        responder.sendJson(HttpResponseStatus.OK, o);
      }
      LOG.info("Status call from AppFabricHttpHandler for app {} mapreduce {}", appId, mapreduceId);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get workflow name from mapreduceId.
   * Format of mapreduceId: WorkflowName_mapreduceName, if the mapreduce is a part of workflow.
   *
   * @param mapreduceId id of the mapreduce job in reactor.
   * @return workflow name if exists null otherwise
   */
  private String getWorkflowName(String mapreduceId) {
    String [] splits = mapreduceId.split("_");
    if (splits.length > 1) {
      return splits[0];
    } else {
      return null;
    }
  }

  /**
   * Get the workflow status
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/status")
  public void workflowStatus(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId,
                             @PathParam("workflow-id") final String workflowId) {

    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(workflowId);
    id.setType(EntityType.WORKFLOW);
    LOG.info("Status call from AppFabricHttpHandler for app {}  workflow id {}", appId, workflowId);
    runnableStatus(request, responder, id);
  }


  private void runnableStatus(HttpRequest request, HttpResponder responder, ProgramId id) {
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);
    //id.setAccountId("default");
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

  /**
   * checks the status of the program
   */
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

   /* -----------------  helpers to return Json consistently -------------- */

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

}
