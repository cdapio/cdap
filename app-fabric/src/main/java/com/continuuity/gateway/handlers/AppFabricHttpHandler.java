package com.continuuity.gateway.handlers;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.ProgramStatus;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.AppFabricServiceException;
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

  private static final Map<String, Type> runnableTypeMap = ImmutableMap.of(
      "mapreduce", Type.MAPREDUCE,
      "flows", Type.FLOW,
      "procedures", Type.PROCEDURE,
      "workflows", Type.WORKFLOW,
      "webapp", Type.WEBAPP
  );

  private enum AppFabricServiceStatus {

    OK(HttpResponseStatus.OK, ""),
    PROGRAM_ALREADY_RUNNING(HttpResponseStatus.CONFLICT, "Program is already running"),
    PROGRAM_ALREADY_STOPPED(HttpResponseStatus.CONFLICT, "Program already stopped"),
    RUNTIME_INFO_NOT_FOUND(HttpResponseStatus.CONFLICT,
        UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND)),
    PROGRAM_NOT_FOUND(HttpResponseStatus.NOT_FOUND, "Program not found"),
    INTERNAL_ERROR(HttpResponseStatus.INTERNAL_SERVER_ERROR, "");

    private final HttpResponseStatus code;
    private final String message;

    /**
     * Describes the output status of app fabric operations.
     */
    private AppFabricServiceStatus(HttpResponseStatus code, String message) {
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

    String accountId = getAuthenticatedAccountId(request);
    Id.Program id = Id.Program.from(accountId, appId, runnableId);
    Type type = runnableTypeMap.get(runnableType);

    try {
      if (type == Type.MAPREDUCE) {
        String workflowName = getWorkflowName(id.getId());
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
          runnableStatus(responder, id, type);
        }
      } else if (type == null){
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        runnableStatus(responder, id, type);
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

  private void runnableStatus(HttpResponder responder, Id.Program id, Type type) {
    try {
      ProgramStatus status = getProgramStatus(id, type);
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
   * Starts a program.
   */
  @POST
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/start")
  public void startProgram(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") final String appId,
                           @PathParam("runnable-type") final String runnableType,
                           @PathParam("runnable-id") final String runnableId) {
    startStopProgram(request, responder, appId, runnableType, runnableId, "start");
  }

  /**
   * Stops a program.
   */
  @POST
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/stop")
  public void stopProgram(HttpRequest request, HttpResponder responder,
                          @PathParam("app-id") final String appId,
                          @PathParam("runnable-type") final String runnableType,
                          @PathParam("runnable-id") final String runnableId){
    startStopProgram(request, responder, appId, runnableType, runnableId, "stop");
  }

  /**
   * Starts / stops a program.
   */
  private synchronized void startStopProgram(HttpRequest request, HttpResponder responder,
                                             final String appId, final String runnableType,
                                             final String runnableId, final String action) {
    Type type = runnableTypeMap.get(runnableType);

    if (type == null || (type == Type.WORKFLOW && "stop".equals(action))) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      LOG.info("{} call from AppFabricHttpHandler for app {}, flow type {} id {}",
          action, appId, runnableType, runnableId);
      runnableStartStop(request, responder, appId, runnableId, type, action);
    }
  }

  private void runnableStartStop(HttpRequest request, HttpResponder responder,
                                 String appId, String runnableId, Type type,
                                 String action) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program id = Id.Program.from(accountId, appId, runnableId);
      AppFabricServiceStatus status = null;
      if ("start".equals(action)) {
        status = start(id, type, decodeArguments(request));
      } else if ("stop".equals(action)) {
        status = stop(id, type);
      }
      if (status == AppFabricServiceStatus.INTERNAL_ERROR) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
      responder.sendString(status.getCode(), status.getMessage());
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


  /**
   * Starts a Program.
   */
  private AppFabricServiceStatus start(final Id.Program id, Type type, Map<String, String> arguments) {

    try {
      ProgramRuntimeService.RuntimeInfo existingRuntimeInfo = findRuntimeInfo(id, type);
      if (existingRuntimeInfo != null) {
        return AppFabricServiceStatus.PROGRAM_ALREADY_RUNNING;
      }

      Program program = store.loadProgram(id, type);
      if (program == null) {
        return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
      }

      BasicArguments userArguments = new BasicArguments();
      if (arguments != null) {
        userArguments = new BasicArguments(arguments);
      }

      ProgramRuntimeService.RuntimeInfo runtimeInfo =
          runtimeService.run(program, new SimpleProgramOptions(id.getId(),
              new BasicArguments(),
              userArguments));
      ProgramController controller = runtimeInfo.getController();
      final String runId = controller.getRunId().getId();

      controller.addListener(new AbstractListener() {
        @Override
        public void stopped() {
          store.setStop(id, runId,
              TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
              ProgramController.State.STOPPED.toString());
        }

        @Override
        public void error(Throwable cause) {
          LOG.info("Program stopped with error {}, {}", id, runId, cause);
          store.setStop(id, runId,
              TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
              ProgramController.State.ERROR.toString());
        }
      }, Threads.SAME_THREAD_EXECUTOR);


      store.setStart(id, runId, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
      return AppFabricServiceStatus.OK;
    } catch (Throwable throwable) {
      LOG.error(throwable.getMessage(), throwable);
      return AppFabricServiceStatus.INTERNAL_ERROR;
    }
  }

  /**
   * Stops a Program.
   */
  private AppFabricServiceStatus stop(Id.Program identifier, Type type) {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(identifier, type);
    if (runtimeInfo == null) {
      try {
        ProgramStatus status = getProgramStatus(identifier, type);
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
      Preconditions.checkNotNull(runtimeInfo, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));
      ProgramController controller = runtimeInfo.getController();
      controller.stop().get();
      return AppFabricServiceStatus.OK;
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      return AppFabricServiceStatus.INTERNAL_ERROR;
    }
  }

  private synchronized ProgramStatus getProgramStatus(Id.Program id, Type type)
    throws AppFabricServiceException {

    try {
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(id, type);

      if (runtimeInfo == null) {
        if (type != Type.WEBAPP) {
          //Runtime info not found. Check to see if the program exists.
          String spec = getSpecification(id, type);
          if (spec == null || spec.isEmpty()) {
            // program doesn't exist
            return new ProgramStatus(id.getApplicationId(), id.getId(), "NOT_FOUND");
          } else {
            // program exists and not running. so return stopped.
            return new ProgramStatus(id.getApplicationId(), id.getId(), ProgramController.State.STOPPED.toString());
          }
        } else {
          // TODO: Fetching webapp status is a hack. This will be fixed when webapp spec is added.
          Location webappLoc = null;
          try {
            Id.Program programId = Id.Program.from(id.getAccountId(), id.getApplicationId(), id.getId());
            webappLoc = Programs.programLocation(locationFactory, appFabricDir, programId, Type.WEBAPP);
          } catch (FileNotFoundException e) {
            // No location found for webapp, no need to log this exception
          }

          if (webappLoc != null && webappLoc.exists()) {
            // webapp exists and not running. so return stopped.
            return new ProgramStatus(id.getApplicationId(), id.getId(), ProgramController.State.STOPPED.toString());
          } else {
            // webapp doesn't exist
            return new ProgramStatus(id.getApplicationId(), id.getId(), "NOT_FOUND");
          }
        }
      }

      String status = controllerStateToString(runtimeInfo.getController().getState());
      return new ProgramStatus(id.getApplicationId(), id.getId(), status);
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

  private String getSpecification(Id.Program id, Type type)
    throws AppFabricServiceException {

    ApplicationSpecification appSpec;
    try {
      appSpec = store.getApplication(id.getApplication());
      if (appSpec == null) {
        return "";
      }

      String runnableId = id.getId();
      if (type == Type.FLOW && appSpec.getFlows().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getFlows().get(id.getId()));
      } else if (type == Type.PROCEDURE && appSpec.getProcedures().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getProcedures().get(id.getId()));
      } else if (type == Type.MAPREDUCE && appSpec.getMapReduce().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getMapReduce().get(id.getId()));
      } else if (type == Type.WORKFLOW && appSpec.getWorkflows().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getWorkflows().get(id.getId()));
      // TODO should figure out a way to integrate that when more endpoints go through this code
      //} else if (type == EntityType.APP) {
      //  return GSON.toJson(makeAppRecord(appSpec));
      }
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw new AppFabricServiceException("Could not retrieve application spec for " +
                                          id.toString() + ", reason: " + e.getMessage());
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
    return "";
  }

  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(Id.Program identifier, Type type) {
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (identifier.equals(info.getProgramId())) {
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
