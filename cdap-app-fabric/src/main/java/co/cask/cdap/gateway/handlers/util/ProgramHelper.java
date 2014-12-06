/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.handlers.util;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.gateway.handlers.WorkflowClient;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.schedule.ScheduledRuntime;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Helper class for performing actions on programs in {@link co.cask.http.HttpHandler} classes
 *
 * TODO: Eventually, none of the methods in this class should need the {@link HttpRequest} parameter.
 * TODO: Also eventually, the accountId parameter in these methods should be replaced with namespaceId
 */
public class ProgramHelper {

  private static final Logger LOG = LoggerFactory.getLogger(AppHelper.class);

  private static final Gson GSON = new Gson();

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  /**
   * Factory for handling the location - can do both in either Distributed or Local mode.
   */
  private final LocationFactory locationFactory;

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  private final Scheduler scheduler;

  private final WorkflowClient workflowClient;

  /**
   * App fabric output directory.
   */
  private final String appFabricDir;

  /**
   * Convenience class for representing the necessary components for retrieving status
   */
  public static class StatusMap {
    private String status = null;
    private String error = null;
    private Integer statusCode = null;

    private StatusMap(String status, String error, int statusCode) {
      this.status = status;
      this.error = error;
      this.statusCode = statusCode;
    }

    public StatusMap() { }

    public int getStatusCode() {
      return statusCode;
    }

    public String getError() {
      return error;
    }

    public String getStatus() {
      return status;
    }

    public void setStatusCode(int statusCode) {
      this.statusCode = statusCode;
    }

    public void setError(String error) {
      this.error = error;
    }

    public void setStatus(String status) {
      this.status = status;
    }
  }

  @Inject
  public ProgramHelper(CConfiguration configuration, ProgramRuntimeService runtimeService, Scheduler scheduler,
                       LocationFactory locationFactory, StoreFactory storeFactory,
                       WorkflowClient workflowClient) {
    this.runtimeService = runtimeService;
    this.scheduler = scheduler;
    this.locationFactory = locationFactory;
    this.store = storeFactory.create();
    this.appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
    this.workflowClient = workflowClient;
  }

  public final void programList(HttpRequest request, HttpResponder responder, String accountId, ProgramType type,
                                   @Nullable String applicationId, Store store) {
    if (applicationId != null && applicationId.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Application id is empty");
      return;
    }

    try {
      List<ProgramRecord> programRecords;
      if (applicationId == null) {
        Id.Account accId = Id.Account.from(accountId);
        programRecords = listPrograms(accId, type, store);
      } else {
        Id.Application appId = Id.Application.from(accountId, applicationId);
        programRecords = listProgramsByApp(appId, type, store);
      }

      if (programRecords == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        String result = GSON.toJson(programRecords);
        responder.sendByteArray(HttpResponseStatus.OK, result.getBytes(Charsets.UTF_8),
                                ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private final List<ProgramRecord> listPrograms(Id.Account accId, ProgramType type, Store store) throws Exception {
    try {
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(accId);
      return listPrograms(appSpecs, type);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      String errorMessage = String.format("Could not retrieve application spec for %s, reason: %s",
                                          accId.toString(), throwable.getMessage());
      throw new Exception(errorMessage, throwable);
    }
  }

  /**
   * Return a list of {@link ProgramRecord} for a {@link ProgramType} in an Application. The return value may be
   * null if the applicationId does not exist.
   */
  private List<ProgramRecord> listProgramsByApp(Id.Application appId, ProgramType type, Store store) throws Exception {
    ApplicationSpecification appSpec;
    try {
      appSpec = store.getApplication(appId);
      return appSpec == null ? null : listPrograms(Collections.singletonList(appSpec), type);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      String errorMessage = String.format("Could not retrieve application spec for %s, reason: %s",
                                          appId.toString(), throwable.getMessage());
      throw new Exception(errorMessage, throwable);
    }
  }

  private final List<ProgramRecord> listPrograms(Collection<ApplicationSpecification> appSpecs, ProgramType type)
    throws Exception {
    List<ProgramRecord> programRecords = Lists.newArrayList();
    for (ApplicationSpecification appSpec : appSpecs) {
      switch (type) {
        case FLOW:
          createProgramRecords(appSpec.getName(), type, appSpec.getFlows().values(), programRecords);
          break;
        case PROCEDURE:
          createProgramRecords(appSpec.getName(), type, appSpec.getProcedures().values(), programRecords);
          break;
        case MAPREDUCE:
          createProgramRecords(appSpec.getName(), type, appSpec.getMapReduce().values(), programRecords);
          break;
        case SPARK:
          createProgramRecords(appSpec.getName(), type, appSpec.getSpark().values(), programRecords);
          break;
        case SERVICE:
          createProgramRecords(appSpec.getName(), type, appSpec.getServices().values(), programRecords);
          break;
        case WORKFLOW:
          createProgramRecords(appSpec.getName(), type, appSpec.getWorkflows().values(), programRecords);
          break;
        default:
          throw new Exception("Unknown program type: " + type.name());
      }
    }
    return programRecords;
  }

  private void createProgramRecords(String appId, ProgramType type,
                                    Iterable<? extends ProgramSpecification> programSpecs,
                                    List<ProgramRecord> programRecords) {
    for (ProgramSpecification programSpec : programSpecs) {
      programRecords.add(makeProgramRecord(appId, programSpec, type));
    }
  }

  public static ProgramRecord makeProgramRecord(String appId, ProgramSpecification spec, ProgramType type) {
    return new ProgramRecord(type, appId, spec.getName(), spec.getName(), spec.getDescription());
  }

  public ProgramRuntimeService.RuntimeInfo findRuntimeInfo(String accountId, String appId,
                                                              String flowId, ProgramType typeId,
                                                              ProgramRuntimeService runtimeService) {
    ProgramType type = ProgramType.valueOf(typeId.name());
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

  public void getLiveInfo(HttpRequest request, HttpResponder responder, final String accountId, final String appId,
                             final String programId, ProgramType type, ProgramRuntimeService runtimeService) {
    try {
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

  public void runnableStatus(HttpResponder responder, Id.Program id, ProgramType type) {
    try {
      ProgramStatus status = getProgramStatus(id, type);
      if (status.getStatus().equals(HttpResponseStatus.NOT_FOUND.toString())) {
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

  public void getStatus(HttpRequest request, HttpResponder responder, String accountId, String appId,
                        String runnableType, String runnableId) {
    try {
      final Id.Program id = Id.Program.from(accountId, appId, runnableId);
      final ProgramType type = ProgramType.valueOfCategoryName(runnableType);
      StatusMap statusMap = getStatus(id, type);
      // If status is null, then there was an error
      if (statusMap.getStatus() == null) {
        responder.sendString(HttpResponseStatus.valueOf(statusMap.getStatusCode()), statusMap.getError());
        return;
      }
      Map<String, String> status = ImmutableMap.of("status", statusMap.getStatus());
      responder.sendJson(HttpResponseStatus.OK, status);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns a map where the pairs map from status to program status (e.g. {"status" : "RUNNING"}) or
   * in case of an error in the input (e.g. invalid id, program not found), a map from statusCode to integer and
   * error to error message (e.g. {"statusCode": 404, "error": "Program not found"})
   * TODO: This is only used outside of this class in the /status endpoint handler method. If that method can move to
   * this class, both this method and the StatusMap object can become private to this class.
   *
   * @param id The Program Id to get the status of
   * @param type The Type of the Program to get the status of
   * @throws RuntimeException if failed to determine the program status
   */
  public StatusMap getStatus(final Id.Program id, final ProgramType type) {
    // invalid type does not exist
    if (type == null) {
      return new StatusMap(null, "Invalid program type provided", HttpResponseStatus.BAD_REQUEST.getCode());
    }

    try {
      // check that app exists
      ApplicationSpecification appSpec = store.getApplication(id.getApplication());
      if (appSpec == null) {
        return new StatusMap(null, "App: " + id.getApplicationId() + " not found",
                             HttpResponseStatus.NOT_FOUND.getCode());
      }

      // For program type other than MapReduce
      if (type != ProgramType.MAPREDUCE) {
        return getProgramStatus(id, type, new StatusMap());
      }

      // must do it this way to allow anon function in workflow to modify status
      // check that mapreduce exists
      if (!appSpec.getMapReduce().containsKey(id.getId())) {
        return new StatusMap(null, "Program: " + id.getId() + " not found", HttpResponseStatus.NOT_FOUND.getCode());
      }

      // See if the MapReduce is part of a workflow
      String workflowName = getWorkflowName(id.getId());
      if (workflowName == null) {
        // Not from workflow, treat it as simple program status
        return getProgramStatus(id, type, new StatusMap());
      }

      // MapReduce is part of a workflow. Query the status of the workflow instead
      final SettableFuture<StatusMap> statusFuture = SettableFuture.create();
      workflowClient.getWorkflowStatus(id.getAccountId(), id.getApplicationId(),
                                       workflowName, new WorkflowClient.Callback() {
          @Override
          public void handle(WorkflowClient.Status status) {
            StatusMap result = new StatusMap();

            if (status.getCode().equals(WorkflowClient.Status.Code.OK)) {
              result.setStatus("RUNNING");
              result.setStatusCode(HttpResponseStatus.OK.getCode());
            } else {
              //mapreduce name might follow the same format even when its not part of the workflow.
              try {
                // getProgramStatus returns program status or http response status NOT_FOUND
                getProgramStatus(id, type, result);
              } catch (Exception e) {
                LOG.error("Exception raised when getting program status for {} {}", id, type, e);
                // error occurred so say internal server error
                result.setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode());
                result.setError(e.getMessage());
              }
            }

            // This would make all changes in the result statusMap available to the other thread that doing
            // the take() call.
            statusFuture.set(result);
          }
        }
      );
      // wait for status to come back in case we are polling mapreduce status in workflow
      // status map contains either a status or an error
      return Futures.getUnchecked(statusFuture);
    } catch (Exception e) {
      LOG.error("Exception raised when getting program status for {} {}", id, type, e);
      return new StatusMap(null, "Failed to get program status", HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode());
    }
  }

  /**
   * Get workflow name from mapreduceId.
   * Format of mapreduceId: WorkflowName_mapreduceName, if the mapreduce is a part of workflow.
   *
   * @param mapreduceId id of the mapreduce job in CDAP
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

  public ProgramStatus getProgramStatus(Id.Program id, ProgramType type) {
    try {
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(id, type);

      if (runtimeInfo == null) {
        if (type != ProgramType.WEBAPP) {
          //Runtime info not found. Check to see if the program exists.
          String spec = getProgramSpecification(id, type);
          if (spec == null || spec.isEmpty()) {
            // program doesn't exist
            return new ProgramStatus(id.getApplicationId(), id.getId(), HttpResponseStatus.NOT_FOUND.toString());
          } else {
            // program exists and not running. so return stopped.
            return new ProgramStatus(id.getApplicationId(), id.getId(), ProgramController.State.STOPPED.toString());
          }
        } else {
          // TODO: Fetching webapp status is a hack. This will be fixed when webapp spec is added.
          Location webappLoc = null;
          try {
            webappLoc = Programs.programLocation(locationFactory, appFabricDir, id, ProgramType.WEBAPP);
          } catch (FileNotFoundException e) {
            // No location found for webapp, no need to log this exception
          }

          if (webappLoc != null && webappLoc.exists()) {
            // webapp exists and not running. so return stopped.
            return new ProgramStatus(id.getApplicationId(), id.getId(), ProgramController.State.STOPPED.toString());
          } else {
            // webapp doesn't exist
            return new ProgramStatus(id.getApplicationId(), id.getId(), HttpResponseStatus.NOT_FOUND.toString());
          }
        }
      }

      String status = controllerStateToString(runtimeInfo.getController().getState());
      return new ProgramStatus(id.getApplicationId(), id.getId(), status);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw Throwables.propagate(throwable);
    }
  }

  private StatusMap getProgramStatus(Id.Program id, ProgramType type, StatusMap statusMap) {
    // getProgramStatus returns program status or http response status NOT_FOUND
    String progStatus = getProgramStatus(id, type).getStatus();
    if (progStatus.equals(HttpResponseStatus.NOT_FOUND.toString())) {
      statusMap.setStatusCode(HttpResponseStatus.NOT_FOUND.getCode());
      statusMap.setError("Program not found");
    } else {
      statusMap.setStatus(progStatus);
      statusMap.setStatusCode(HttpResponseStatus.OK.getCode());
    }
    return statusMap;
  }

  public void runnableSpecification(HttpRequest request, HttpResponder responder, final String accountId,
                                     final String appId, ProgramType runnableType, final String runnableId) {
    try {
      Id.Program id = Id.Program.from(accountId, appId, runnableId);
      String specification = getProgramSpecification(id, runnableType);
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

  private String getProgramSpecification(Id.Program id, ProgramType type)
    throws Exception {

    ApplicationSpecification appSpec;
    try {
      appSpec = store.getApplication(id.getApplication());
      if (appSpec == null) {
        return "";
      }
      String runnableId = id.getId();
      if (type == ProgramType.FLOW && appSpec.getFlows().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getFlows().get(id.getId()));
      } else if (type == ProgramType.PROCEDURE && appSpec.getProcedures().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getProcedures().get(id.getId()));
      } else if (type == ProgramType.MAPREDUCE && appSpec.getMapReduce().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getMapReduce().get(id.getId()));
      } else if (type == ProgramType.SPARK && appSpec.getSpark().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getSpark().get(id.getId()));
      } else if (type == ProgramType.WORKFLOW && appSpec.getWorkflows().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getWorkflows().get(id.getId()));
      } else if (type == ProgramType.SERVICE && appSpec.getServices().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getServices().get(id.getId()));
      }
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new Exception(throwable.getMessage());
    }
    return "";
  }

  protected void deleteHandler(Id.Program programId, ProgramType type)
    throws ExecutionException {
    try {
      switch (type) {
        case FLOW:
          stopProgramIfRunning(programId, type, runtimeService);
          break;
        case PROCEDURE:
          stopProgramIfRunning(programId, type, runtimeService);
          break;
        case WORKFLOW:
          List<String> scheduleIds = scheduler.getScheduleIds(programId, type);
          scheduler.deleteSchedules(programId, ProgramType.WORKFLOW, scheduleIds);
          break;
        case MAPREDUCE:
          //no-op
          break;
        case SERVICE:
          stopProgramIfRunning(programId, type, runtimeService);
          break;
      }
    } catch (InterruptedException e) {
      throw new ExecutionException(e);
    }
  }

  private void stopProgramIfRunning(Id.Program programId, ProgramType type, ProgramRuntimeService runtimeService)
    throws InterruptedException, ExecutionException {
    ProgramRuntimeService.RuntimeInfo programRunInfo = findRuntimeInfo(programId.getAccountId(),
                                                                       programId.getApplicationId(),
                                                                       programId.getId(),
                                                                       type, runtimeService);
    if (programRunInfo != null) {
      doStop(programRunInfo);
    }
  }

  private void doStop(ProgramRuntimeService.RuntimeInfo runtimeInfo)
    throws ExecutionException, InterruptedException {
    Preconditions.checkNotNull(runtimeInfo, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));
    ProgramController controller = runtimeInfo.getController();
    controller.stop().get();
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

  public ProgramRuntimeService.RuntimeInfo findRuntimeInfo(Id.Program identifier, ProgramType type) {
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               identifier.getAccountId(), identifier.getApplicationId());
    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (identifier.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  public synchronized void startStopProgram(HttpRequest request, HttpResponder responder, final String accountId,
                                             final String appId, final String runnableType, final String runnableId,
                                             final String action, final Map<String, String> args) {
    ProgramType type = ProgramType.valueOfCategoryName(runnableType);

    if (type == null || (type == ProgramType.WORKFLOW && "stop".equals(action))) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      LOG.trace("{} call from AppFabricHttpHandler for app {}, flow type {} id {}",
                action, appId, runnableType, runnableId);
      runnableStartStop(request, responder, accountId, appId, runnableId, type, action, args);
    }
  }

  public void runnableStartStop(HttpRequest request, HttpResponder responder, String accountId, String appId,
                                 String runnableId, ProgramType type, String action, Map<String, String> args) {
    try {
      Id.Program id = Id.Program.from(accountId, appId, runnableId);
      AppFabricServiceStatus status = null;
      if ("start".equals(action)) {
        status = start(id, type, args, false);
      } else if ("debug".equals(action)) {
        status = start(id, type, args, true);
      } else if ("stop".equals(action)) {
        status = stop(id, type);
      }
      if (status == AppFabricServiceStatus.INTERNAL_ERROR) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        return;
      }

      responder.sendString(status.getCode(), status.getMessage());
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Starts a Program.
   */
  private AppFabricServiceStatus start(final Id.Program id, ProgramType type,
                                       Map<String, String> overrides, boolean debug) {

    try {
      Program program = store.loadProgram(id, type);
      if (program == null) {
        return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
      }

      if (isRunning(id, type)) {
        return AppFabricServiceStatus.PROGRAM_ALREADY_RUNNING;
      }

      Map<String, String> userArgs = store.getRunArguments(id);
      if (overrides != null) {
        for (Map.Entry<String, String> entry : overrides.entrySet()) {
          userArgs.put(entry.getKey(), entry.getValue());
        }
      }

      BasicArguments userArguments = new BasicArguments(userArgs);
      ProgramRuntimeService.RuntimeInfo runtimeInfo =
        runtimeService.run(program, new SimpleProgramOptions(id.getId(), new BasicArguments(), userArguments, debug));

      final ProgramController controller = runtimeInfo.getController();
      final String runId = controller.getRunId().getId();

      controller.addListener(new AbstractListener() {

        @Override
        public void init(ProgramController.State state) {
          store.setStart(id, runId, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
          if (state == ProgramController.State.STOPPED) {
            stopped();
          }
          if (state == ProgramController.State.ERROR) {
            error(controller.getFailureCause());
          }
        }
        @Override
        public void stopped() {
          store.setStop(id, runId,
                        TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                        ProgramController.State.STOPPED);
        }

        @Override
        public void error(Throwable cause) {
          LOG.info("Program stopped with error {}, {}", id, runId, cause);
          store.setStop(id, runId,
                        TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                        ProgramController.State.ERROR);
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      return AppFabricServiceStatus.OK;
    } catch (DatasetInstantiationException e) {
      return new AppFabricServiceStatus(HttpResponseStatus.UNPROCESSABLE_ENTITY, e.getMessage());
    } catch (Throwable throwable) {
      LOG.error(throwable.getMessage(), throwable);
      if (throwable instanceof FileNotFoundException) {
        return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
      }
      return AppFabricServiceStatus.INTERNAL_ERROR;
    }
  }

  /**
   * Stops a Program.
   */
  private AppFabricServiceStatus stop(Id.Program identifier, ProgramType type) {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(identifier, type);
    if (runtimeInfo == null) {
      try {
        ProgramStatus status = getProgramStatus(identifier, type);
        if (status.getStatus().equals(HttpResponseStatus.NOT_FOUND.toString())) {
          return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
        } else if (ProgramController.State.STOPPED.toString().equals(status.getStatus())) {
          return AppFabricServiceStatus.PROGRAM_ALREADY_STOPPED;
        } else {
          return AppFabricServiceStatus.RUNTIME_INFO_NOT_FOUND;
        }
      } catch (Exception e) {
        if (e instanceof FileNotFoundException) {
          return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
        }
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

  public void getRuns(HttpRequest request, HttpResponder responder, String accountId, String appId, String runnableId,
                       String status, long start, long end, int limit) {
    try {
      Id.Program programId = Id.Program.from(accountId, appId, runnableId);
      try {
        ProgramRunStatus runStatus = (status == null) ? ProgramRunStatus.ALL :
          ProgramRunStatus.valueOf(status.toUpperCase());
        responder.sendJson(HttpResponseStatus.OK, store.getRuns(programId, runStatus, start, end, limit));
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                             "Supported options for status of runs are running/completed/failed");
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

  public void getRunnableRuntimeArgs(HttpRequest request, HttpResponder responder, String accountId, String appId,
                             String runnableType, String runnableId) {
    ProgramType type = ProgramType.valueOfCategoryName(runnableType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    Id.Program id = Id.Program.from(accountId, appId, runnableId);


    try {
      if (!store.programExists(id, type)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }
      Map<String, String> runtimeArgs = store.getRunArguments(id);
      responder.sendJson(HttpResponseStatus.OK, runtimeArgs);
    } catch (Throwable e) {
      LOG.error("Error getting runtime args {}", e.getMessage(), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public void saveRunnableRuntimeArgs(HttpRequest request, HttpResponder responder, String accountId, String appId,
                                      String runnableType, String runnableId, Map<String, String> args) {
    ProgramType type = ProgramType.valueOfCategoryName(runnableType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    Id.Program id = Id.Program.from(accountId, appId, runnableId);

    try {
      if (!store.programExists(id, type)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }
      store.storeRunArguments(id, args);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable e) {
      LOG.error("Error getting runtime args {}", e.getMessage(), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public void getProcedureInstances(HttpRequest request, HttpResponder responder, String accountId, String appId,
                                    String procedureId) {
    try {
      Id.Program programId = Id.Program.from(accountId, appId, procedureId);

      if (!store.programExists(programId, ProgramType.PROCEDURE)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }

      int count = getProgramInstances(programId);
      responder.sendJson(HttpResponseStatus.OK, new Instances(count));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable throwable) {
      LOG.error("Got exception : ", throwable);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public void setProcedureInstances(HttpRequest request, HttpResponder responder, String accountId, String appId,
                                    String procedureId, int instances) {
    try {
      Id.Program programId = Id.Program.from(accountId, appId, procedureId);

      if (!store.programExists(programId, ProgramType.PROCEDURE)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }

      if (instances < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }

      setProgramInstances(programId, instances);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable throwable) {
      LOG.error("Got exception : ", throwable);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void setProgramInstances(Id.Program programId, int instances) throws Exception {
    try {
      store.setProcedureInstances(programId, instances);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, ProgramType.PROCEDURE);
      if (runtimeInfo != null) {
        runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                            ImmutableMap.of(programId.getId(), instances)).get();
      }
    } catch (Throwable throwable) {
      LOG.warn("Exception when getting instances for {}.{} to {}. {}",
               programId.getId(), ProgramType.PROCEDURE.getPrettyName(), throwable.getMessage(), throwable);
      throw new Exception(throwable.getMessage());
    }
  }

  private int getProgramInstances(Id.Program programId) throws Exception {
    try {
      return store.getProcedureInstances(programId);
    } catch (Throwable throwable) {
      LOG.warn("Exception when getting instances for {}.{} to {}.{}",
               programId.getId(), ProgramType.PROCEDURE.getPrettyName(), throwable.getMessage(), throwable);
      throw new Exception(throwable.getMessage());
    }
  }

  private boolean isRunning(Id.Program id, ProgramType type) {
    String programStatus = getStatus(id, type).getStatus();
    return programStatus != null && !"STOPPED".equals(programStatus);
  }

  /**
   * Check if any program that satisfy the given {@link com.google.common.base.Predicate} is running.
   *
   * @param predicate Get call on each running {@link Id.Program}.
   * @param types Types of program to check
   * returns True if a program is running as defined by the predicate.
   */
  public boolean checkAnyRunning(Predicate<Id.Program> predicate, ProgramType... types) {
    for (ProgramType type : types) {
      for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry :  runtimeService.list(type).entrySet()) {
        ProgramController.State programState = entry.getValue().getController().getState();
        if (programState == ProgramController.State.STOPPED || programState == ProgramController.State.ERROR) {
          continue;
        }
        Id.Program programId = entry.getValue().getProgramId();
        if (predicate.apply(programId)) {
          LOG.trace("Program still running in checkAnyRunning: {} {} {} {}",
                    programId.getApplicationId(), type, programId.getId(), entry.getValue().getController().getRunId());
          return true;
        }
      }
    }
    return false;
  }

  public void getScheduledRunTime(HttpRequest request, HttpResponder responder, String accountId, String appId,
                                  String workflowId) {
    try {
      Id.Program id = Id.Program.from(accountId, appId, workflowId);
      List<ScheduledRuntime> runtimes = scheduler.nextScheduledRuntime(id, ProgramType.WORKFLOW);

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

  public void getWorkflowSchedules(HttpRequest request, HttpResponder responder, String accountId, String appId,
                                   String workflowId) {
    try {
      Id.Program id = Id.Program.from(accountId, appId, workflowId);
      responder.sendJson(HttpResponseStatus.OK, scheduler.getScheduleIds(id, ProgramType.WORKFLOW));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public void getScheduleState(HttpRequest request, HttpResponder responder, String accountId, String workflowId,
                               String scheduleId) {
    try {
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

  public void suspendWorkflowSchedule(HttpRequest request, HttpResponder responder, String accountId, String appId,
                                      String workflowId, String scheduleId) {
    try {
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
          responder.sendJson(HttpResponseStatus.CONFLICT, "Schedule already suspended");
          break;
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public void resumeWorkflowSchedule(HttpRequest request, HttpResponder responder, String accountId, String appId,
                                     String workflowId, String scheduleId) {
    try {
      Scheduler.ScheduleState state = scheduler.scheduleState(scheduleId);
      switch (state) {
        case NOT_FOUND:
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          break;
        case SCHEDULED:
          responder.sendJson(HttpResponseStatus.CONFLICT, "Already resumed");
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

  public void getWorkflowStatus(HttpRequest request, final HttpResponder responder, String accountId, String appId,
                                String workflowName) {
    try {
      workflowClient.getWorkflowStatus(accountId, appId, workflowName,
                                       new WorkflowClient.Callback() {
                                         @Override
                                         public void handle(WorkflowClient.Status status) {
                                           if (status.getCode() == WorkflowClient.Status.Code.NOT_FOUND) {
                                             responder.sendStatus(HttpResponseStatus.NOT_FOUND);
                                           } else if (status.getCode() == WorkflowClient.Status.Code.OK) {
                                             responder.sendByteArray(HttpResponseStatus.OK,
                                                                     status.getResult().getBytes(),
                                                                     ImmutableMultimap.of(
                                                                       HttpHeaders.Names.CONTENT_TYPE,
                                                                       "application/json; charset=utf-8"));

                                           } else {
                                             responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                                 status.getResult());
                                           }
                                         }
                                       });
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
