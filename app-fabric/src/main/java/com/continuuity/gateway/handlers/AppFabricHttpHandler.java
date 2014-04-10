package com.continuuity.gateway.handlers;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.*;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.OperationException;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *  HttpHandler class for app-fabric requests.
 */
@Path(Constants.Gateway.GATEWAY_VERSION) //this will be removed/changed when gateway goes.
public class AppFabricHttpHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricHttpHandler.class);

  private static final java.lang.reflect.Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>(){}.getType();

  /**
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

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

  private static final Map<String, EntityType> runnableTypeMap = ImmutableMap.of(
      "mapreduce", EntityType.MAPREDUCE,
      "flows", EntityType.FLOW,
      "procedures", EntityType.PROCEDURE,
      "workflows", EntityType.WORKFLOW,
      "webapp", EntityType.WEBAPP
  );

  private enum AppFabricServiceStatus {

    OK(HttpResponseStatus.OK, ""),
    PROGRAM_ALREADY_RUNNING(HttpResponseStatus.CONFLICT, "Program is already running"),
    PROGRAM_ALREADY_STOPPED(HttpResponseStatus.CONFLICT, "Program already stopped"),
    PROGRAM_NOT_FOUND(HttpResponseStatus.NOT_FOUND, "Program not found"),
    INTERNAL_ERROR(HttpResponseStatus.INTERNAL_SERVER_ERROR, ""),
    RUNTIME_INFO_NOT_FOUND(HttpResponseStatus.INTERNAL_SERVER_ERROR,
        UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));
    private final HttpResponseStatus code;

    private final String message;

    /**
     * Describes the output status of app fabric operations.
     */
    AppFabricServiceStatus(HttpResponseStatus code, String message) {
      this.code = code;
      this.message = message;
    }

    public HttpResponseStatus getCode() {
      return code;
    }

    public String getMessage() {
      return message;
    }
  }

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
   * Returns status of a runnable specified by the type{flows,workflows,mapreduce,procedures}.
   */
  @GET
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/status")
  public void getStatus(final HttpRequest request, final HttpResponder responder,
                        @PathParam("app-id") final String appId,
                        @PathParam("runnable-type") final String runnableType,
                        @PathParam("runnable-id") final String runnableId){

    LOG.info("Status call from AppFabricHttpHandler for app {} : {} id {}", appId, runnableType, runnableId);
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(runnableId);
    id.setType(runnableTypeMap.get(runnableType));

    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);
    try {
      if (id.getType() == EntityType.MAPREDUCE) {
        String workflowName = getWorkflowName(id.getFlowId());
        if (workflowName != null) {
          //mapreduce is part of a workflow
          workflowClient.getWorkflowStatus(id.getAccountId(), id.getApplicationId(),
                                           workflowName, new WorkflowClient.Callback() {
                                             @Override
                                             public void handle(WorkflowClient.Status status) {
                                               JsonObject reply = new JsonObject();
                                               if (status.getCode().equals(WorkflowClient.Status.Code.OK)) {
                                                 reply.addProperty("status", "RUNNING");
                                               } else {
                                                 reply.addProperty("status", "STOPPED");
                                               }
                                               responder.sendJson(HttpResponseStatus.OK, reply);
                                             }
                                           }
          );
        } else {
          //mapreduce is not part of a workflow
          runnableStatus(request, responder, id);
        }
      } else if (id.getType() == null){
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        runnableStatus(request, responder, id);
      }
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
        JsonObject reply = new JsonObject();
        reply.addProperty("status", status.getStatus());
        responder.sendJson(HttpResponseStatus.OK, reply);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Starts / stops an operation.
   */
  @POST
  @Path("/apps/{app-id}/{runnable-type}/{flow-id}/{action}")
  public void startStopFlowType(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId,
                                @PathParam("runnable-type") final String runnableType,
                                @PathParam("runnable-id") final String runnableId,
                                @PathParam("action") final String action) {
    if (!"start".equals(action) && !"stop".equals(action)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(runnableId);
    id.setType(runnableTypeMap.get(runnableType));

    if (id.getType() == null || (id.getType() == EntityType.WORKFLOW && "start".equals(action))) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      LOG.info("{} call from AppFabricHttpHandler for app {}, flow type {} id {}",
          action, appId, runnableType, runnableId);
      runnableStartStop(request, responder, id, action);
    }
  }

  private void runnableStartStop(HttpRequest request, HttpResponder responder,
                                 ProgramId id, String action) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      id.setAccountId(accountId);
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
      AppFabricServiceStatus status = null;
      if ("start".equals(action)) {
        status = start(new ProgramDescriptor(id, decodeArguments(request)));
      } else if ("stop".equals(action)) {
        status = stop(token, id);
      }
      switch (status) {
        case OK:
          responder.sendStatus(HttpResponseStatus.OK);
          break;
        case PROGRAM_NOT_FOUND:
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          break;
        case PROGRAM_ALREADY_STOPPED:
        case PROGRAM_ALREADY_RUNNING:
          responder.sendString(HttpResponseStatus.CONFLICT, status.getMessage());
          break;
        case INTERNAL_ERROR:
        case RUNTIME_INFO_NOT_FOUND:
          LOG.error("Got error:", status.getMessage());
          responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
          break;
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private Map<String, String> decodeArguments(HttpRequest request) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return ImmutableMap.of();
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      Map<String, String> args = GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
      return args == null ? ImmutableMap.<String, String>of() : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    } finally {
      reader.close();
    }
  }

  private Type entityTypeToType(ProgramId identifier) {
    return Type.valueOf(identifier.getType().name());
  }

  /**
   * Starts a Program.
   */
  private AppFabricServiceStatus start(ProgramDescriptor descriptor) {

    try {
      ProgramId id = descriptor.getIdentifier();
      ProgramRuntimeService.RuntimeInfo existingRuntimeInfo = findRuntimeInfo(id);
      final Id.Program programId = Id.Program.from(id.getAccountId(), id.getApplicationId(), id.getFlowId());
      if (existingRuntimeInfo != null) {
        return AppFabricServiceStatus.PROGRAM_ALREADY_RUNNING;
      }

      Program program = store.loadProgram(programId, entityTypeToType(id));
      if (program == null) {
        return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
      }

      BasicArguments userArguments = new BasicArguments();
      if (descriptor.isSetArguments()) {
        userArguments = new BasicArguments(descriptor.getArguments());
      }

      ProgramRuntimeService.RuntimeInfo runtimeInfo =
          runtimeService.run(program, new SimpleProgramOptions(id.getFlowId(),
              new BasicArguments(),
              userArguments));
      ProgramController controller = runtimeInfo.getController();
      final String runId = controller.getRunId().getId();

      controller.addListener(new AbstractListener() {
        @Override
        public void stopped() {
          store.setStop(programId, runId,
              TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
              ProgramController.State.STOPPED.toString());
        }

        @Override
        public void error(Throwable cause) {
          LOG.info("Program stopped with error {}, {}", programId, runId, cause);
          store.setStop(programId, runId,
              TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
              ProgramController.State.ERROR.toString());
        }
      }, Threads.SAME_THREAD_EXECUTOR);


      store.setStart(programId, runId, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
      return AppFabricServiceStatus.OK;
    } catch (Throwable throwable) {
      LOG.error(throwable.getMessage(), throwable);
      return AppFabricServiceStatus.INTERNAL_ERROR;
    }
  }

  /**
   * Stops a Program.
   */
  private AppFabricServiceStatus stop(AuthToken token, ProgramId identifier) {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(identifier);
    if (runtimeInfo == null) {
      try {
        ProgramStatus status = getProgramStatus(token, identifier);
        if ("NOT_FOUND".equals(status.getStatus())) {
          return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
        } else if (ProgramController.State.STOPPED.toString().equals(status.getStatus())) {
          return AppFabricServiceStatus.PROGRAM_ALREADY_STOPPED;
        } else {
          return AppFabricServiceStatus.RUNTIME_INFO_NOT_FOUND;
        }
      } catch (AppFabricServiceException e) {
        return AppFabricServiceStatus.INTERNAL_ERROR;
      }
    }

    try {
      doStop(runtimeInfo);
      return AppFabricServiceStatus.OK;
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      return AppFabricServiceStatus.INTERNAL_ERROR;
    }
  }

  private RunIdentifier doStop(ProgramRuntimeService.RuntimeInfo runtimeInfo)
      throws ExecutionException, InterruptedException {
    Preconditions.checkNotNull(runtimeInfo, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));
    ProgramController controller = runtimeInfo.getController();
    RunId runId = controller.getRunId();
    controller.stop().get();
    return new RunIdentifier(runId.getId());
  }

  private synchronized ProgramStatus getProgramStatus(AuthToken token, ProgramId id)
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
      String status = controllerStateToString(runtimeInfo.getController().getState());
      return new ProgramStatus(programId.getApplicationId(), programId.getId(), runId, status);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /** NOTE: This was a temporary hack done to map the status to something that is
   * UI friendly. Internal states of program controller are reasonable and hence
   * no point in changing them.
   */
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
