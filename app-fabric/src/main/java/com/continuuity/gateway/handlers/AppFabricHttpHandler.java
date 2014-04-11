package com.continuuity.gateway.handlers;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.ArchiveId;
import com.continuuity.app.services.ArchiveInfo;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeployStatus;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramStatus;
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
import com.continuuity.internal.app.runtime.schedule.Scheduler;
import com.continuuity.internal.filesystem.LocationCodec;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 *  HttpHandler class for app-fabric requests.
 */
@Path(Constants.Gateway.GATEWAY_VERSION) //this will be removed/changed when gateway goes.
public class AppFabricHttpHandler extends AuthenticatedHttpHandler  {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricHttpHandler.class);
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

  private static final Map<String, EntityType> runnableTypeMap = ImmutableMap.of(
    "mapreduce", EntityType.MAPREDUCE,
    "flows", EntityType.FLOW,
    "procedures", EntityType.PROCEDURE,
    "workflows", EntityType.WORKFLOW,
    "webapp", EntityType.WEBAPP
  );

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

  /*
   * checks the status of the program
   */
  private ProgramStatus getProgramStatus(AuthToken token, ProgramId id)
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

  /**
   * Deploys an application with speicifed name.
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
        throw new AppFabricServiceException("Failed to write channel buffer content.");
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }
  }

  private void setupSchedules(String accountId, ApplicationSpecification specification)  throws IOException {

    for (Map.Entry<String, WorkflowSpecification> entry : specification.getWorkflows().entrySet()){
      Id.Program programId = Id.Program.from(accountId, specification.getName(), entry.getKey());
      List<String> existingSchedules = scheduler.getScheduleIds(programId, Type.WORKFLOW);
      //Delete the existing schedules and add new ones.
      if (!existingSchedules.isEmpty()){
        scheduler.deleteSchedules(programId, Type.WORKFLOW, existingSchedules);
      }
      // Add new schedules.
      if (!entry.getValue().getSchedules().isEmpty()) {
        scheduler.schedule(programId, Type.WORKFLOW, entry.getValue().getSchedules());
      }
    }
  }

  // deploy helper
  private void deploy(final ArchiveId resource) throws AppFabricServiceException {
    LOG.debug("Finishing deploy of application " + resource.toString());
    if (!sessions.containsKey(resource.getAccountId())) {
      throw new AppFabricServiceException("No information about archive being uploaded is available.");
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
          deleteHandler(id, programId, type);
        }
      });

      ApplicationWithPrograms applicationWithPrograms =
        manager.deploy(id, sessionInfo.getApplicationId(), archiveLocation).get();
      ApplicationSpecification specification = applicationWithPrograms.getAppSpecLoc().getSpecification();

      setupSchedules(resource.getAccountId(), specification);
      status = DeployStatus.DEPLOYED;
      LOG.info("Deployed the file: {}", sessionInfo.getFilename());

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
      throw new AppFabricServiceException(e.getMessage());
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
      DeploymentStatus status  = dstatus(token, new ArchiveId(accountId, "", ""));
      LOG.info("Deployment status call at AppFabricHttpHandler , Status: {}", status);
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
  private DeploymentStatus dstatus(AuthToken token, ArchiveId resource) throws AppFabricServiceException {
    try {
      if (!sessions.containsKey(resource.getAccountId())) {
        SessionInfo info = retrieve(resource.getAccountId());
        return new DeploymentStatus(info.getStatus().getCode(), info.getStatus().getMessage());
      } else {
        SessionInfo info = sessions.get(resource.getAccountId());
        return new DeploymentStatus(info.getStatus().getCode(), info.getStatus().getMessage());
      }
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
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
  private ArchiveId init(ArchiveInfo info) throws AppFabricServiceException {
    LOG.debug("Init deploying application " + info.toString());
    ArchiveId identifier = new ArchiveId(info.getAccountId(), "appId", "resourceId");

    try {
      if (sessions.containsKey(info.getAccountId())) {
        throw new AppFabricServiceException("An upload is already in progress for this account.");
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
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }


  private void deleteHandler(Id.Account id, Id.Program programId, Type type)
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
