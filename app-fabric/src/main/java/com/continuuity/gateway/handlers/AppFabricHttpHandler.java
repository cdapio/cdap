package com.continuuity.gateway.handlers;

import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.ProgramStatus;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.ArchiveId;
import com.continuuity.app.services.ArchiveInfo;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeployStatus;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.RunIdentifier;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.OperationException;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;
import com.continuuity.internal.app.deploy.ProgramTerminator;
import com.continuuity.internal.app.deploy.SessionInfo;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.internal.app.runtime.schedule.ScheduledRuntime;
import com.continuuity.internal.app.runtime.schedule.Scheduler;
import com.continuuity.internal.filesystem.LocationCodec;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
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
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 *  HttpHandler class for app-fabric requests.
 */
@Path(Constants.Gateway.GATEWAY_VERSION) //this will be removed/changed when gateway goes.
public class AppFabricHttpHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricHttpHandler.class);

  private static final java.lang.reflect.Type MAP_STRING_STRING_TYPE
    = new TypeToken<Map<String, String>>() { }.getType();

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
   * Maintains a mapping of transient session state. The state is stored in memory,
   * in case of failure, all the current running sessions will be terminated. As
   * per the current implementation only connection per account is allowed to upload.
   */
  private final Map<String, SessionInfo> sessions = Maps.newConcurrentMap();

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";

  private final WorkflowClient workflowClient;


  /**
   * The directory where the uploaded files would be placed.
   */
  private final String archiveDir;

  /**
   * DeploymentManager responsible for running pipeline.
   */
  private final ManagerFactory managerFactory;
  private final Scheduler scheduler;

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
                              LocationFactory locationFactory, ManagerFactory managerFactory,
                              StoreFactory storeFactory,
                              ProgramRuntimeService runtimeService,
                              WorkflowClient workflowClient, Scheduler service) {
    super(authenticator);
    this.locationFactory = locationFactory;
    this.managerFactory = managerFactory;
    this.configuration = configuration;
    this.runtimeService = runtimeService;
    this.appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR,
                                          System.getProperty("java.io.tmpdir"));
    this.archiveDir = this.appFabricDir + "/archive";
    this.store = storeFactory.create();
    this.workflowClient = workflowClient;
    this.scheduler = service;

  }

  /**
   * Ping: responds with an OK message.
   */
  @Path("/ping")
  @GET
  public void Get(HttpRequest request, HttpResponder response) {
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
                        @PathParam("runnable-id") final String runnableId) {

    LOG.trace("Status call from AppFabricHttpHandler for app {} : {} id {}", appId, runnableType, runnableId);

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
      } else if (type == null) {
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
      if (status.getStatus().equals("NOT_FOUND")) {
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
   * Starts a program with debugging enabled.
   */
  @POST
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/debug")
  public void debugProgram(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") final String appId,
                           @PathParam("runnable-type") final String runnableType,
                           @PathParam("runnable-id") final String runnableId) {
    if (!("flows".equals(runnableType) || "procedures".equals(runnableType))) {
      responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
      return;
    }
    startStopProgram(request, responder, appId, runnableType, runnableId, "debug");
  }

  /**
   * Stops a program.
   */
  @POST
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/stop")
  public void stopProgram(HttpRequest request, HttpResponder responder,
                          @PathParam("app-id") final String appId,
                          @PathParam("runnable-type") final String runnableType,
                          @PathParam("runnable-id") final String runnableId) {
    startStopProgram(request, responder, appId, runnableType, runnableId, "stop");
  }

  /**
   * Returns program run history.
   */
  @GET
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/history")
  public void runnableHistory(HttpRequest request, HttpResponder responder,
                          @PathParam("app-id") final String appId,
                          @PathParam("runnable-type") final String runnableType,
                          @PathParam("runnable-id") final String runnableId) {
    Type type = runnableTypeMap.get(runnableType);
    if (type == null || type == Type.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
    String startTs = getQueryParameter(decoder.getParameters(), Constants.AppFabric.QUERY_PARAM_START_TIME);
    String endTs = getQueryParameter(decoder.getParameters(), Constants.AppFabric.QUERY_PARAM_END_TIME);
    String resultLimit = getQueryParameter(decoder.getParameters(), Constants.AppFabric.QUERY_PARAM_LIMIT);

    long start = startTs == null ? Long.MIN_VALUE : Long.parseLong(startTs);
    long end = endTs == null ? Long.MAX_VALUE : Long.parseLong(endTs);
    int limit = resultLimit == null ? Constants.AppFabric.DEFAULT_HISTORY_RESULTS_LIMIT : Integer.parseInt(resultLimit);
    getHistory(request, responder, appId, runnableId, start, end, limit);
  }

  /**
   * Get runnable runtime args.
   */
  @GET
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/runtimeargs")
  public void getRunnableRuntimeArgs(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("runnable-type") final String runnableType,
                                     @PathParam("runnable-id") final String runnableId) {
    Type type = runnableTypeMap.get(runnableType);
    if (type == null || type == Type.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    String accountId = getAuthenticatedAccountId(request);
    Id.Program id = Id.Program.from(accountId, appId, runnableId);

    try {
      Map<String, String> runtimeArgs = store.getRunArguments(id);
      responder.sendJson(HttpResponseStatus.OK, runtimeArgs);
    } catch (Throwable e) {
      LOG.error("Error getting runtime args {}", e.getMessage(), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Save runnable runtime args.
   */
  @PUT
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/runtimeargs")
  public void saveRunnableRuntimeArgs(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") final String appId,
                                      @PathParam("runnable-type") final String runnableType,
                                      @PathParam("runnable-id") final String runnableId) {
    Type type = runnableTypeMap.get(runnableType);
    if (type == null || type == Type.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    String accountId = getAuthenticatedAccountId(request);
    Id.Program id = Id.Program.from(accountId, appId, runnableId);

    try {
      Map<String, String> args = decodeArguments(request);
      store.storeRunArguments(id, args);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable e) {
      LOG.error("Error getting runtime args {}", e.getMessage(), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private String getQueryParameter(Map<String, List<String>> parameters, String parameterName) {
    if (parameters == null || parameters.isEmpty()) {
      return null;
    } else {
      List<String> matchedParams = parameters.get(parameterName);
      return matchedParams == null || matchedParams.isEmpty() ? null : matchedParams.get(0);
    }
  }

  private void getHistory(HttpRequest request, HttpResponder responder, String appId,
                          String runnableId, long start, long end, int limit) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program programId = Id.Program.from(accountId, appId, runnableId);
      try {
        List<RunRecord> records = store.getRunHistory(programId, start, end, limit);
        JsonArray history = new JsonArray();

        for (RunRecord record : records) {
          JsonObject object = new JsonObject();
          object.addProperty("runid", record.getPid());
          object.addProperty("start", record.getStartTs());
          object.addProperty("end", record.getStopTs());
          object.addProperty("status", record.getEndStatus());
          history.add(object);
        }
        responder.sendJson(HttpResponseStatus.OK, history);
      } catch (OperationException e) {
        LOG.warn(String.format(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND),
            programId.toString(), e.getMessage()), e);
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private synchronized void startStopProgram(HttpRequest request, HttpResponder responder,
                                             final String appId, final String runnableType,
                                             final String runnableId, final String action) {
    Type type = runnableTypeMap.get(runnableType);

    if (type == null || (type == Type.WORKFLOW && "stop".equals(action))) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      LOG.trace("{} call from AppFabricHttpHandler for app {}, flow type {} id {}",
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
        status = start(id, type, decodeArguments(request), false);
      } else if ("debug".equals(action)) {
        status = start(id, type, decodeArguments(request), true);
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
  private AppFabricServiceStatus start(final Id.Program id, Type type, Map<String, String> arguments, boolean debug) {

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
        runtimeService.run(program, new SimpleProgramOptions(id.getId(), new BasicArguments(), userArguments, debug));

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
      } catch (Exception e) {
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


  /**
   * Returns number of instances for a flowlet within a flow.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances")
  public void getFlowletInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId,
                                  @PathParam("flowlet-id") final String flowletId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      int count = store.getFlowletInstances(Id.Program.from(accountId, appId, flowId), flowletId);
      JsonObject reply = new JsonObject();
      reply.addProperty("instances", count);
      responder.sendJson(HttpResponseStatus.OK, reply);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Increases number of instance for a flowlet within a flow.
   */
  @PUT
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances")
  public void setFlowletInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId,
                                  @PathParam("flowlet-id") final String flowletId) {
    Short instances = 0;
    try {
      instances = getInstances(request);
      if (instances < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }
    } catch (Throwable th) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid instance count.");
      return;
    }

    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program programID = Id.Program.from(accountId, appId, flowId);
      int oldInstances = store.getFlowletInstances(programID, flowletId);
      if (oldInstances != (int) instances) {
        store.setFlowletInstances(programID, flowletId, instances);
        ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(accountId, appId, flowId, Type.FLOW);
        if (runtimeInfo != null) {
          runtimeInfo.getController().command(ProgramOptionConstants.FLOWLET_INSTANCES,
                                              ImmutableMap.of("flowlet", flowletId,
                                                              "newInstances", String.valueOf((int) instances),
                                                              "oldInstances", String.valueOf(oldInstances))).get();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private short getInstances(HttpRequest request) throws IOException, NumberFormatException {
    String instanceCount = "";
    Map<String, String> arguments = decodeArguments(request);
    if (!arguments.isEmpty()) {
      instanceCount = arguments.get("instances");
    }
    return Short.parseShort(instanceCount);
  }

  private synchronized ProgramStatus getProgramStatus(Id.Program id, Type type)
    throws Exception {

    try {
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(id, type);

      if (runtimeInfo == null) {
        if (type != Type.WEBAPP) {
          //Runtime info not found. Check to see if the program exists.
          String spec = getProgramSpecification(id, type);
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
            webappLoc = Programs.programLocation(locationFactory, appFabricDir, id, Type.WEBAPP);
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
      throw new Exception(throwable.getMessage());
    }
  }

  /**
   * Deploys an application with the specified name.
   */
  @PUT
  @Path("/apps/{app-id}")
  public void deploy(HttpRequest request, HttpResponder responder, @PathParam("app-id") final String appId) {
    deployApp(request, responder, appId);
  }

  /**
   * Deploys an application.
   */
  @POST
  @Path("/apps")
  public void deploy(HttpRequest request, HttpResponder responder) {
    // null means use name provided by app spec
    deployApp(request, responder, null);
  }

  /**
   * Returns next scheduled runtime of a workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/nextruntime")
  public void getScheduledRunTime(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId,
                                  @PathParam("workflow-id") final String workflowId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program id = Id.Program.from(accountId, appId, workflowId);
      List<ScheduledRuntime> runtimes = scheduler.nextScheduledRuntime(id, Type.WORKFLOW);

      JsonArray array = new JsonArray();
      for (ScheduledRuntime runtime : runtimes) {
        JsonObject object = new JsonObject();
        object.addProperty("id", runtime.getScheduleId());
        object.addProperty("time", runtime.getTime());
        array.add(object);
      }
      responder.sendJson(HttpResponseStatus.OK, array);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns the schedule ids for a given workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules")
  public void workflowSchedules(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId,
                                @PathParam("workflow-id") final String workflowId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program id = Id.Program.from(accountId, appId, workflowId);
      responder.sendJson(HttpResponseStatus.OK, scheduler.getScheduleIds(id, Type.WORKFLOW));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get schedule state.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules/{schedule-id}/status")
  public void getScheuleState(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId,
                              @PathParam("workflow-id") final String workflowId,
                              @PathParam("schedule-id") final String scheduleId) {
    try {
      // get the accountId to catch if there is a security exception
      String accountId = getAuthenticatedAccountId(request);
      JsonObject json = new JsonObject();
      json.addProperty("status", scheduler.scheduleState(scheduleId).toString());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Suspend a workflow schedule.
   */
  @POST
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules/{schedule-id}/suspend")
  public void workflowScheduleSuspend(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") final String appId,
                                      @PathParam("workflow-id") final String workflowId,
                                      @PathParam("schedule-id") final String scheduleId) {
    try {
      // get the accountId to catch if there is a security exception
      String accountId = getAuthenticatedAccountId(request);
      Scheduler.ScheduleState state = scheduler.scheduleState(scheduleId);
      switch (state) {
        case NOT_FOUND:
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          break;
        case SCHEDULED:
          scheduler.suspendSchedule(scheduleId);
          responder.sendJson(HttpResponseStatus.OK, "OK");
          break;
        case SUSPENDED:
          responder.sendJson(HttpResponseStatus.OK, "Schedule already suspended");
          break;
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Resume a workflow schedule.
   */
  @POST
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules/{schedule-id}/resume")
  public void workflowScheduleResume(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("workflow-id") final String workflowId,
                                     @PathParam("schedule-id") final String scheduleId) {

    try {
      // get the accountId to catch if there is a security exception
      String accountId = getAuthenticatedAccountId(request);
      Scheduler.ScheduleState state = scheduler.scheduleState(scheduleId);
      switch (state) {
        case NOT_FOUND:
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          break;
        case SCHEDULED:
          responder.sendJson(HttpResponseStatus.OK, "Already resumed");
          break;
        case SUSPENDED:
          scheduler.resumeSchedule(scheduleId);
          responder.sendJson(HttpResponseStatus.OK, "OK");
          break;
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/live-info")
  @SuppressWarnings("unused")
  public void procedureLiveInfo(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId,
                                @PathParam("procedure-id") final String procedureId) {
    getLiveInfo(request, responder, appId, procedureId, Type.PROCEDURE);
  }

  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/live-info")
  @SuppressWarnings("unused")
  public void flowLiveInfo(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") final String appId,
                           @PathParam("flow-id") final String flowId) {
    getLiveInfo(request, responder, appId, flowId, Type.FLOW);
  }

  /**
   * Returns specification of a runnable - flow, procedure, mapreduce, workflow.
   */
  @GET
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}")
  public void runnableSpecification(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId,
                                @PathParam("runnable-type") final String runnableType,
                                @PathParam("runnable-id") final String runnableId) {

    Type type = runnableTypeMap.get(runnableType);
    if (type == null || type == Type.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program id = Id.Program.from(accountId, appId, runnableId);
      String specification = getProgramSpecification(id, type);
      if (specification == null || specification.isEmpty()) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendByteArray(HttpResponseStatus.OK, specification.getBytes(Charsets.UTF_8),
                                ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
     * Deploys an application.
     */
  private void deployApp(HttpRequest request, HttpResponder responder, final String appId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      String archiveName = request.getHeader(ARCHIVE_NAME_HEADER);
      if (archiveName == null || archiveName.isEmpty()) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, ARCHIVE_NAME_HEADER + " header not present");
        return;
      }
      ChannelBuffer content = request.getContent();
      if (content == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Archive is null");
        return;
      }

      try {
        ArchiveInfo rInfo = new ArchiveInfo(accountId, archiveName);
        rInfo.setApplicationId(appId);
        ArchiveId rIdentifier = init(rInfo);
        SessionInfo info = sessions.get(rIdentifier.getAccountId()).setStatus(DeployStatus.UPLOADING);
        OutputStream stream = info.getOutputStream();
        int length = content.readableBytes();
        byte[] archive = new byte[length];
        content.readSlice(length).toByteBuffer().get(archive);
        stream.write(archive);
        deploy(rIdentifier);
        responder.sendStatus(HttpResponseStatus.OK);

      } catch (Throwable throwable) {
        LOG.warn(throwable.getMessage(), throwable);
        throw new Exception("Failed to write channel buffer content.");
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }
  }

  private void setupSchedules(String accountId, ApplicationSpecification specification)  throws IOException {

    for (Map.Entry<String, WorkflowSpecification> entry : specification.getWorkflows().entrySet()) {
      Id.Program programId = Id.Program.from(accountId, specification.getName(), entry.getKey());
      List<String> existingSchedules = scheduler.getScheduleIds(programId, Type.WORKFLOW);
      //Delete the existing schedules and add new ones.
      if (!existingSchedules.isEmpty()) {
        scheduler.deleteSchedules(programId, Type.WORKFLOW, existingSchedules);
      }
      // Add new schedules.
      if (!entry.getValue().getSchedules().isEmpty()) {
        scheduler.schedule(programId, Type.WORKFLOW, entry.getValue().getSchedules());
      }
    }
  }

  // deploy helper
  private void deploy(final ArchiveId resource) throws Exception {
    LOG.debug("Finishing deploy of application " + resource.toString());
    if (!sessions.containsKey(resource.getAccountId())) {
      throw new Exception("No information about archive being uploaded is available.");
    }

    final SessionInfo sessionInfo = sessions.get(resource.getAccountId());
    DeployStatus status = sessionInfo.getStatus();
    try {
      Id.Account id = Id.Account.from(resource.getAccountId());
      Location archiveLocation = sessionInfo.getArchiveLocation();
      sessionInfo.getOutputStream().close();
      sessionInfo.setStatus(DeployStatus.VERIFYING);
      Manager<Location, ApplicationWithPrograms> manager = managerFactory.create(new ProgramTerminator() {
        @Override
        public void stop(Id.Account id, Id.Program programId, Type type) throws ExecutionException {
          deleteHandler(programId, type);
        }
      });

      ApplicationWithPrograms applicationWithPrograms =
        manager.deploy(id, sessionInfo.getApplicationId(), archiveLocation).get();
      ApplicationSpecification specification = applicationWithPrograms.getAppSpecLoc().getSpecification();

      setupSchedules(resource.getAccountId(), specification);
      status = DeployStatus.DEPLOYED;

    } catch (Throwable e) {
      LOG.warn(e.getMessage(), e);

      status = DeployStatus.FAILED;
      if (e instanceof ExecutionException) {
        Throwable cause = e.getCause();

        if (cause instanceof ClassNotFoundException) {
          status.setMessage(String.format(UserMessages.getMessage(UserErrors.CLASS_NOT_FOUND), cause.getMessage()));
        } else if (cause instanceof IllegalArgumentException) {
          status.setMessage(String.format(UserMessages.getMessage(UserErrors.SPECIFICATION_ERROR), cause.getMessage()));
        } else {
          status.setMessage(cause.getMessage());
        }
      }

      status.setMessage(e.getMessage());
      throw new Exception(e.getMessage());
    } finally {
      save(sessionInfo.setStatus(status));
      sessions.remove(resource.getAccountId());
    }
  }

  /**
   * Defines the class for sending deploy status to client.
   */
  private static class Status {
    private final int code;
    private final String status;
    private final String message;

    public Status(int code, String message) {
      this.code = code;
      this.status = DeployStatus.getMessage(code);
      this.message = message;
    }
  }
  /**
   * Gets application deployment status.
   */
  @GET
  @Path("/deploy/status")
  public void getDeployStatus(HttpRequest request, HttpResponder responder) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
      DeploymentStatus status  = dstatus(new ArchiveId(accountId, "", ""));
      LOG.trace("Deployment status call at AppFabricHttpHandler , Status: {}", status);
      responder.sendJson(HttpResponseStatus.OK, new Status(status.getOverall(), status.getMessage()));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /*
   * Retrieves a {@link SessionInfo} from the file system.
   */
  @Nullable
  private SessionInfo retrieve(String accountId) {
    try {
      final Location outputDir = locationFactory.create(archiveDir + "/" + accountId);
      if (!outputDir.exists()) {
        return null;
      }
      final Location sessionInfoFile = outputDir.append("session.json");
      InputSupplier<Reader> reader = new InputSupplier<Reader>() {
        @Override
        public Reader getInput() throws IOException {
          return new InputStreamReader(sessionInfoFile.getInputStream(), "UTF-8");
        }
      };

      Gson gson = new GsonBuilder().registerTypeAdapter(Location.class, new LocationCodec(locationFactory)).create();
      Reader r = reader.getInput();
      try {
        return gson.fromJson(r, SessionInfo.class);
      } finally {
        Closeables.closeQuietly(r);
      }
    } catch (IOException e) {
      LOG.warn("Failed to retrieve session info for account.");
    }
    return null;
  }

  /*
   * Returns DeploymentStatus
   */
  private DeploymentStatus dstatus(ArchiveId resource) {
    if (!sessions.containsKey(resource.getAccountId())) {
      SessionInfo info = retrieve(resource.getAccountId());
      return new DeploymentStatus(info.getStatus().getCode(), info.getStatus().getMessage());
    } else {
      SessionInfo info = sessions.get(resource.getAccountId());
      return new DeploymentStatus(info.getStatus().getCode(), info.getStatus().getMessage());
    }
  }

  /*
   * Initializes deployment of resources from the client.
   * <p>
   *   Upon receiving a request to initialize an upload with auth-token and resource information,
   *   we create a unique identifier for the upload and also create directories needed for storing
   *   the uploading archive. At this point the upload has not yet begun. The bytes of the archive
   *   are still on the client machine. An session id is returned back to client - which will use
   *   the session id provided to upload the chunks.
   * </p>
   * <p>
   *   <i>Note:</i> As the state of upload are transient they are not being persisted on the server.
   * </p>
   *
   * @param info ArchiveInfo
   * @return ArchiveId instance containing the resource id and
   * resource version.
   */
  private ArchiveId init(ArchiveInfo info) throws Exception {
    LOG.debug("Init deploying application " + info.toString());
    ArchiveId identifier = new ArchiveId(info.getAccountId(), "appId", "resourceId");

    try {
      if (sessions.containsKey(info.getAccountId())) {
        throw new Exception("An upload is already in progress for this account.");
      }
      Location uploadDir = locationFactory.create(archiveDir + "/" + info.getAccountId());
      if (!uploadDir.exists() && !uploadDir.mkdirs()) {
        LOG.warn("Unable to create directory '{}'", uploadDir.getName());
      }
      Location archive = uploadDir.append(info.getFilename());
      SessionInfo sessionInfo = new SessionInfo(identifier, info, archive, DeployStatus.REGISTERED);
      sessions.put(info.getAccountId(), sessionInfo);
      return identifier;
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new Exception(throwable.getMessage());
    }
  }


  private void deleteHandler(Id.Program programId, Type type)
    throws ExecutionException {
    try {
      switch (type) {
        case FLOW:
          //Stop the flow if it not running
          ProgramRuntimeService.RuntimeInfo flowRunInfo = findRuntimeInfo(new ProgramId(programId.getAccountId(),
                                                                                        programId.getApplicationId(),
                                                                                        programId.getId()));
          if (flowRunInfo != null) {
            doStop(flowRunInfo);
          }
          break;
        case PROCEDURE:
          //Stop the procedure if it not running
          ProgramRuntimeService.RuntimeInfo procedureRunInfo = findRuntimeInfo(new ProgramId(
            programId.getAccountId(), programId.getApplicationId(),
            programId.getId()));
          if (procedureRunInfo != null) {
            doStop(procedureRunInfo);
          }
          break;
        case WORKFLOW:
          List<String> scheduleIds = scheduler.getScheduleIds(programId, type);
          scheduler.deleteSchedules(programId, Type.WORKFLOW, scheduleIds);
          break;
        case MAPREDUCE:
          //no-op
          break;
      };
    } catch (InterruptedException e) {
      throw new ExecutionException(e);
    }
  }

  /**
   * Saves the {@link SessionInfo} to the filesystem.
   *
   * @param info to be saved.
   * @return true if and only if successful; false otherwise.
   */
  private boolean save(SessionInfo info) {
    try {
      Gson gson = new GsonBuilder().registerTypeAdapter(Location.class, new LocationCodec(locationFactory)).create();
      String accountId = info.getArchiveId().getAccountId();
      Location outputDir = locationFactory.create(archiveDir + "/" + accountId);
      if (!outputDir.exists()) {
        return false;
      }
      final Location sessionInfoFile = outputDir.append("session.json");
      OutputSupplier<Writer> writer = new OutputSupplier<Writer>() {
        @Override
        public Writer getOutput() throws IOException {
          return new OutputStreamWriter(sessionInfoFile.getOutputStream(), "UTF-8");
        }
      };

      Writer w = writer.getOutput();
      try {
        gson.toJson(info, w);
      } finally {
        Closeables.closeQuietly(w);
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      return false;
    }
    return true;
  }

  private RunIdentifier doStop(ProgramRuntimeService.RuntimeInfo runtimeInfo)
    throws ExecutionException, InterruptedException {
    Preconditions.checkNotNull(runtimeInfo, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));
    ProgramController controller = runtimeInfo.getController();
    RunId runId = controller.getRunId();
    controller.stop().get();
    return new RunIdentifier(runId.getId());
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

  private String getProgramSpecification(Id.Program id, Type type)
    throws Exception {

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
      }
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new Exception(throwable.getMessage());
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

  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(Id.Program identifier, Type type) {
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (identifier.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(String accountId, String appId,
                                                            String flowId, Type typeId) {
    Type type = Type.valueOf(typeId.name());
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               accountId, flowId);

    Id.Program programId = Id.Program.from(accountId, appId, flowId);

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  private void getLiveInfo(HttpRequest request, HttpResponder responder,
                           final String appId, final String programId, Type type) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      responder.sendJson(HttpResponseStatus.OK,
                         runtimeService.getLiveInfo(Id.Program.from(accountId,
                                                                    appId,
                                                                    programId),
                                                    type));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
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
