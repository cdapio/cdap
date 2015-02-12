/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.services.Data;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.RESTMigrationUtils;
import co.cask.cdap.config.ConsoleSettingsStore;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.BodyConsumer;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Closeables;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 *  HttpHandler class for app-fabric requests.
 */
@Path(Constants.Gateway.API_VERSION_2) //this will be removed/changed when gateway goes.
public class AppFabricHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricHttpHandler.class);

  /**
   * Configuration object passed from higher up.
   */
  private final CConfiguration configuration;

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  /**
   * Client talking to transaction system.
   */
  private TransactionSystemClient txClient;

  /**
   * Access Dataset Service
   */
  private final DatasetFramework dsFramework;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final QueueAdmin queueAdmin;

  private final StreamAdmin streamAdmin;

  /**
   * V3 API Handlers
   */
  private final AppLifecycleHttpHandler appLifecycleHttpHandler;

  private final ProgramLifecycleHttpHandler programLifecycleHttpHandler;

  private final AppFabricStreamHttpHandler appFabricStreamHttpHandler;

  private final PreferencesStore preferencesStore;

  private final ConsoleSettingsStore consoleSettingsStore;

  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  @Inject
  public AppFabricHttpHandler(Authenticator authenticator, CConfiguration configuration,
                              StoreFactory storeFactory,
                              ProgramRuntimeService runtimeService, StreamAdmin streamAdmin,
                              QueueAdmin queueAdmin, TransactionSystemClient txClient, DatasetFramework dsFramework,
                              AppLifecycleHttpHandler appLifecycleHttpHandler,
                              ProgramLifecycleHttpHandler programLifecycleHttpHandler,
                              AppFabricStreamHttpHandler appFabricStreamHttpHandler,
                              PreferencesStore preferencesStore, ConsoleSettingsStore consoleSettingsStore) {

    super(authenticator);
    this.streamAdmin = streamAdmin;
    this.configuration = configuration;
    this.runtimeService = runtimeService;
    this.store = storeFactory.create();
    this.queueAdmin = queueAdmin;
    this.txClient = txClient;
    this.dsFramework =
      new NamespacedDatasetFramework(dsFramework, new DefaultDatasetNamespace(configuration, Namespace.USER));
    this.appLifecycleHttpHandler = appLifecycleHttpHandler;
    this.programLifecycleHttpHandler = programLifecycleHttpHandler;
    this.appFabricStreamHttpHandler = appFabricStreamHttpHandler;
    this.preferencesStore = preferencesStore;
    this.consoleSettingsStore = consoleSettingsStore;
  }

  /**
   * Ping to check handler status.
   */
  @Path("/ping")
  @GET
  public void ping(HttpRequest request, HttpResponder responder) {
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Retrieve the state of the transaction manager.
   */
  @Path("/transactions/state")
  @GET
  public void getTxManagerSnapshot(HttpRequest request, HttpResponder responder) {
    try {
      LOG.trace("Taking transaction manager snapshot at time {}", System.currentTimeMillis());
      InputStream in = txClient.getSnapshotInputStream();
      LOG.trace("Took and retrieved transaction manager snapshot successfully.");
      try {
        ChunkResponder chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK,
                                                                 ImmutableMultimap.<String, String>of());
        while (true) {
          // netty doesn't copy the readBytes buffer, so we have to reallocate a new buffer
          byte[] readBytes = new byte[4096];
          int res = in.read(readBytes, 0, 4096);
          if (res == -1) {
            break;
          }
          // If failed to send chunk, IOException will be raised.
          // It'll just propagated to the netty-http library to handle it
          chunkResponder.sendChunk(ChannelBuffers.wrappedBuffer(readBytes, 0, res));
        }
        Closeables.closeQuietly(chunkResponder);
      } finally {
        in.close();
      }
    } catch (Exception e) {
      LOG.error("Could not take transaction manager snapshot", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Invalidate a transaction.
   * @param txId transaction ID.
   */
  @Path("/transactions/{tx-id}/invalidate")
  @POST
  public void invalidateTx(HttpRequest request, HttpResponder responder,
                           @PathParam("tx-id") final String txId) {
    try {
      long txIdLong = Long.parseLong(txId);
      boolean success = txClient.invalidate(txIdLong);
      if (success) {
        LOG.info("Transaction {} successfully invalidated", txId);
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        LOG.info("Transaction {} could not be invalidated: not in progress.", txId);
        responder.sendStatus(HttpResponseStatus.CONFLICT);
      }
    } catch (NumberFormatException e) {
      LOG.info("Could not invalidate transaction: {} is not a valid tx id", txId);
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    }
  }

  /**
   * Reset the state of the transaction manager.
   */
  @Path("/transactions/state")
  @POST
  public void resetTxManagerState(HttpRequest request, HttpResponder responder) {
    txClient.resetState();
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Returns status of a type specified by {flows,workflows,mapreduce,spark,procedures,services,schedules}.
   */
  @GET
  @Path("/apps/{app-id}/{type}/{id}/status")
  public void getStatus(final HttpRequest request, final HttpResponder responder,
                        @PathParam("app-id") final String appId,
                        @PathParam("type") final String type,
                        @PathParam("id") final String id) {
    programLifecycleHttpHandler.getStatus(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                          type, id);
  }

  /**
   * starts a webapp.
   */
  @POST
  @Path("/apps/{app-id}/webapp/start")
  public void webappStart(final HttpRequest request, final HttpResponder responder,
                          @PathParam("app-id") final String appId) {
    programLifecycleHttpHandler.runnableStartStop(request, responder, Constants.DEFAULT_NAMESPACE, appId,
                                                  ProgramType.WEBAPP.getPrettyName().toLowerCase(), ProgramType.WEBAPP,
                                                  "start");
  }


  /**
   * stops a webapp.
   */
  @POST
  @Path("/apps/{app-id}/webapp/stop")
  public void webappStop(final HttpRequest request, final HttpResponder responder,
                         @PathParam("app-id") final String appId) {
    programLifecycleHttpHandler.runnableStartStop(request, responder, Constants.DEFAULT_NAMESPACE, appId,
                                                  ProgramType.WEBAPP.getPrettyName().toLowerCase(), ProgramType.WEBAPP,
                                                  "stop");
  }

  /**
   * Returns status of a webapp.
   */
  @GET
  @Path("/apps/{app-id}/webapp/status")
  public void webappStatus(final HttpRequest request, final HttpResponder responder,
                           @PathParam("app-id") final String appId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program id = Id.Program.from(accountId, appId, ProgramType.WEBAPP.getPrettyName().toLowerCase());
      runnableStatus(responder, id, ProgramType.WEBAPP);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable t) {
      LOG.error("Got exception:", t);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void runnableStatus(HttpResponder responder, Id.Program id, ProgramType type) {
    try {
      ProgramStatus status = programLifecycleHttpHandler.getProgramStatus(id, type);
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


  /**
   * Starts a program.
   */
  @POST
  @Path("/apps/{app-id}/{type}/{id}/start")
  public void startProgram(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") final String appId,
                           @PathParam("type") final String type,
                           @PathParam("id") final String id) {
    programLifecycleHttpHandler.performAction(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                              appId, type, id, "start");
  }

  /**
   * Starts a program with debugging enabled.
   */
  @POST
  @Path("/apps/{app-id}/{type}/{id}/debug")
  public void debugProgram(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") final String appId,
                           @PathParam("type") final String type,
                           @PathParam("id") final String id) {
    programLifecycleHttpHandler.performAction(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                              appId, type, id, "debug");
  }

  /**
   * Stops a program.
   */
  @POST
  @Path("/apps/{app-id}/{type}/{id}/stop")
  public void stopProgram(HttpRequest request, HttpResponder responder,
                          @PathParam("app-id") final String appId,
                          @PathParam("type") final String type,
                          @PathParam("id") final String id) {
    programLifecycleHttpHandler.performAction(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                              appId, type, id, "stop");
  }

  /**
   * Returns program runs based on options it returns either currently running or completed or failed.
   * Default it returns all.
   */
  @GET
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/runs")
  public void runnableHistory(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") String appId,
                              @PathParam("runnable-type") String runnableType,
                              @PathParam("runnable-id") String runnableId,
                              @QueryParam("status") String status,
                              @QueryParam("start") String startTs,
                              @QueryParam("end") String endTs,
                              @QueryParam("limit") @DefaultValue("100") final int resultLimit) {
    programLifecycleHttpHandler.runnableHistory(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                                runnableType, runnableId, status, startTs, endTs, resultLimit);
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
    programLifecycleHttpHandler.getRunnableRuntimeArgs(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                       appId, runnableType, runnableId);
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
    programLifecycleHttpHandler.saveRunnableRuntimeArgs(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                        appId, runnableType, runnableId);
  }

  /**
   * Returns number of instances for a procedure.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/instances")
  public void getProcedureInstances(HttpRequest request, HttpResponder responder,
                                    @PathParam("app-id") final String appId,
                                    @PathParam("procedure-id") final String procedureId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
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


  /**
   * Sets number of instances for a procedure.
   */
  @PUT
  @Path("/apps/{app-id}/procedures/{procedure-id}/instances")
  public void setProcedureInstances(HttpRequest request, HttpResponder responder,
                                    @PathParam("app-id") final String appId,
                                    @PathParam("procedure-id") final String procedureId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program programId = Id.Program.from(accountId, appId, procedureId);

      if (!store.programExists(programId, ProgramType.PROCEDURE)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }

      int instances = getInstances(request);
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

  /**
   * TODO: This method should move to {@link ProgramLifecycleHttpHandler} when set and get instances v3 APIs are
   * implemented.
   */
  private void setProgramInstances(Id.Program programId, int instances) throws Exception {
    try {
      store.setProcedureInstances(programId, instances);
      ProgramRuntimeService.RuntimeInfo runtimeInfo =
        programLifecycleHttpHandler.findRuntimeInfo(programId, ProgramType.PROCEDURE);
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

  /**
   * Returns number of instances for a flowlet within a flow.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances")
  public void getFlowletInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") String appId, @PathParam("flow-id") String flowId,
                                  @PathParam("flowlet-id") String flowletId) {
    programLifecycleHttpHandler.getFlowletInstances(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                    appId, flowId, flowletId);
  }

  /**
   * Returns the number of instances for all program runnables that are passed into the data. The data is an array of
   * Json objects where each object must contain the following three elements: appId, programType, and programId
   * (flow name, service name, or procedure name). Retrieving instances only applies to flows, procedures, and user
   * services. For flows and procedures, another parameter, "runnableId", must be provided. This corresponds to the
   * flowlet/runnable for which to retrieve the instances. This does not apply to procedures.
   *
   * Example input:
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "runnableId": "Runnable1"},
   *  {"appId": "App1", "programType": "Procedure", "programId": "Proc2"},
   *  {"appId": "App2", "programType": "Flow", "programId": "Flow1", "runnableId": "Flowlet1"}]
   *
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as 3 fields:
   * "provisioned" which maps to the number of instances actually provided for the input runnable,
   * "requested" which maps to the number of instances the user has requested for the input runnable,
   * "statusCode" which maps to the http status code for the data in that JsonObjects. (200, 400, 404)
   * If an error occurs in the input (i.e. in the example above, Flowlet1 does not exist), then all JsonObjects for
   * which the parameters have a valid instances will have the provisioned and requested fields status code fields
   * but all JsonObjects for which the parameters are not valid will have an error message and statusCode.
   *
   * E.g. given the above data, if there is no Flowlet1, then the response would be 200 OK with following possible data:
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "runnableId": "Runnable1",
   *   "statusCode": 200, "provisioned": 2, "requested": 2},
   *  {"appId": "App1", "programType": "Procedure", "programId": "Proc2", "statusCode": 200, "provisioned": 1,
   *   "requested": 3},
   *  {"appId": "App2", "programType": "Flow", "programId": "Flow1", "runnableId": "Flowlet1", "statusCode": 404,
   *   "error": "Runnable": Flowlet1 not found"}]
   *
   * @param request
   * @param responder
   */
  @POST
  @Path("/instances")
  public void getInstances(HttpRequest request, HttpResponder responder) {
    programLifecycleHttpHandler.getInstances(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Returns the status for all programs that are passed into the data. The data is an array of Json objects
   * where each object must contain the following three elements: appId, programType, and programId
   * (flow name, service name, etc.).
   * <p/>
   * Example input:
   * [{"appId": "App1", "programType": "Service", "programId": "Service1"},
   * {"appId": "App1", "programType": "Procedure", "programId": "Proc2"},
   * {"appId": "App2", "programType": "Flow", "programId": "Flow1"}]
   * <p/>
   * The response will be an array of JsonObjects each of which will contain the three input parameters
   * as well as 2 fields, "status" which maps to the status of the program and "statusCode" which maps to the
   * status code for the data in that JsonObjects. If an error occurs in the
   * input (i.e. in the example above, App2 does not exist), then all JsonObjects for which the parameters
   * have a valid status will have the status field but all JsonObjects for which the parameters do not have a valid
   * status will have an error message and statusCode.
   * <p/>
   * For example, if there is no App2 in the data above, then the response would be 200 OK with following possible data:
   * [{"appId": "App1", "programType": "Service", "programId": "Service1", "statusCode": 200, "status": "RUNNING"},
   * {"appId": "App1", "programType": "Procedure", "programId": "Proc2"}, "statusCode": 200, "status": "STOPPED"},
   * {"appId":"App2", "programType":"Flow", "programId":"Flow1", "statusCode":404, "error": "App: App2 not found"}]
   *
   * @param request
   * @param responder
   */
  @POST
  @Path("/status")
  public void getStatuses(HttpRequest request, HttpResponder responder) {
    programLifecycleHttpHandler.getStatuses(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Increases number of instance for a flowlet within a flow.
   */
  @PUT
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances")
  public void setFlowletInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") String appId, @PathParam("flow-id") String flowId,
                                  @PathParam("flowlet-id") String flowletId) {
    programLifecycleHttpHandler.setFlowletInstances(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                    appId, flowId, flowletId);
  }

  /**
   * Changes input stream for a flowlet connection.
   */
  @PUT
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/connections/{stream-id}")
  public void changeFlowletStreamConnection(HttpRequest request, HttpResponder responder,
                                            @PathParam("app-id") String appId,
                                            @PathParam("flow-id") String flowId,
                                            @PathParam("flowlet-id") String flowletId,
                                            @PathParam("stream-id") String streamId) throws IOException {
    programLifecycleHttpHandler.changeFlowletStreamConnection(rewriteRequest(request), responder,
                                                              Constants.DEFAULT_NAMESPACE, appId, flowId, flowletId,
                                                              streamId);
  }

  /**
   * Deploys an application with the specified name.
   */
  @PUT
  @Path("/apps/{app-id}")
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder, @PathParam("app-id") final String appId,
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName) {
    try {
      return appLifecycleHttpHandler.deploy(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                            archiveName);
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Deploy failed: {}" + ex.getMessage());
      return null;
    }

  }

  /**
   * Deploys an application.
   */
  @POST
  @Path("/apps")
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder,
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName) {
    // null means use name provided by app spec
    try {
      return appLifecycleHttpHandler.deploy(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, null,
                                            archiveName);
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Deploy failed: " + ex.getMessage());
      return null;
    }
  }

  /**
   * Returns next scheduled runtime of a workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/nextruntime")
  public void getScheduledRunTime(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") String appId,
                                  @PathParam("workflow-id") String workflowId) {
    programLifecycleHttpHandler.getScheduledRunTime(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                    appId, workflowId);
  }

  /**
   * Returns the schedule ids for a given workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules")
  public void getWorkflowSchedules(HttpRequest request, HttpResponder responder,
                                   @PathParam("app-id") String appId,
                                   @PathParam("workflow-id") String workflowId) {
    programLifecycleHttpHandler.getWorkflowSchedules(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                     appId, workflowId);
  }

  /**
   * Suspend a schedule.
   */
  @POST
  @Path("/apps/{app-id}/schedules/{schedule-name}/suspend")
  public void suspendSchedule(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId,
                                      @PathParam("schedule-name") String scheduleName) {
    programLifecycleHttpHandler.performAction(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                              appId, "schedules", scheduleName, "suspend");
  }

  /**
   * Resume a schedule.
   */
  @POST
  @Path("/apps/{app-id}/schedules/{schedule-name}/resume")
  public void resumeSchedule(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") String appId,
                                     @PathParam("schedule-name") String scheduleName) {
    programLifecycleHttpHandler.performAction(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                              appId, "schedules", scheduleName, "resume");
  }

  /**
   * Procedures directly execute v2 calls, they do not delegate to v3 handlers since they are deprecated
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/live-info")
  @SuppressWarnings("unused")
  public void procedureLiveInfo(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") String appId,
                                @PathParam("procedure-id") String procedureId) {
    getLiveInfo(request, responder, Constants.DEFAULT_NAMESPACE, appId, procedureId, ProgramType.PROCEDURE,
                runtimeService);
  }

  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/live-info")
  @SuppressWarnings("unused")
  public void flowLiveInfo(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("flow-id") String flowId) {
    programLifecycleHttpHandler.liveInfo(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                         ProgramType.FLOW.getCategoryName(), flowId);
  }

  /**
   * Returns specification of a runnable - flow.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}")
  public void flowSpecification(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") String appId,
                                @PathParam("flow-id")String flowId) {
    programLifecycleHttpHandler.runnableSpecification(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                      appId, ProgramType.FLOW.getCategoryName(), flowId);
  }

  /**
   * Returns specification of procedure.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}")
  public void procedureSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") String appId,
                                     @PathParam("procedure-id") String procId) {
    programLifecycleHttpHandler.runnableSpecification(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                      appId, ProgramType.PROCEDURE.getCategoryName(), procId);
  }

  /**
   * Returns specification of mapreduce.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}")
  public void mapreduceSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("mapreduce-id")final String mapreduceId) {
    programLifecycleHttpHandler.runnableSpecification(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                      appId, ProgramType.MAPREDUCE.getCategoryName(), mapreduceId);
  }

  /**
   * Returns specification of spark program.
   */
  @GET
  @Path("/apps/{app-id}/spark/{spark-id}")
  public void sparkSpecification(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") final String appId,
                                 @PathParam("spark-id")final String sparkId) {
    programLifecycleHttpHandler.runnableSpecification(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                      appId, ProgramType.SPARK.getCategoryName(), sparkId);
  }

  /**
   * Returns specification of workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}")
  public void workflowSpecification(HttpRequest request, HttpResponder responder,
                                    @PathParam("app-id") final String appId,
                                    @PathParam("workflow-id")final String workflowId) {
    programLifecycleHttpHandler.runnableSpecification(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                      appId, ProgramType.WORKFLOW.getCategoryName(), workflowId);
  }

  @GET
  @Path("/apps/{app-id}/services/{service-id}")
  public void serviceSpecification(HttpRequest request, HttpResponder responder,
                                   @PathParam("app-id") String appId,
                                   @PathParam("service-id") String serviceId) {
    programLifecycleHttpHandler.runnableSpecification(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                      appId, ProgramType.SERVICE.getCategoryName(), serviceId);
  }

  /**
   * Delete an application specified by appId.
   */
  @DELETE
  @Path("/apps/{app-id}")
  public void deleteApp(HttpRequest request, HttpResponder responder,
                        @PathParam("app-id") final String appId) {
    appLifecycleHttpHandler.deleteApp(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId);
  }

  /**
   * Deletes all applications in CDAP.
   */
  @DELETE
  @Path("/apps")
  public void deleteAllApps(HttpRequest request, HttpResponder responder) {
    appLifecycleHttpHandler.deleteAllApps(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Deletes queues.
   */
  @DELETE
  @Path("/apps/{app-id}/flows/{flow-id}/queues")
  public void deleteFlowQueues(HttpRequest request, HttpResponder responder,
                               @PathParam("app-id") String appId,
                               @PathParam("flow-id") String flowId) {
    programLifecycleHttpHandler.deleteFlowQueues(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                 appId, flowId);
  }

  @DELETE
  @Path("/queues")
  public void deleteQueues(HttpRequest request, HttpResponder responder) {
    programLifecycleHttpHandler.deleteQueues(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  @DELETE
  @Path("/streams")
  public void deleteStreams(HttpRequest request, HttpResponder responder) {
    try {
      streamAdmin.dropAll();
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      LOG.error("Error while deleting streams", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @GET
  @Path("/apps/{app-id}/workflows/{workflow-name}/current")
  public void workflowStatus(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") String appId, @PathParam("workflow-name") String workflowName) {
    programLifecycleHttpHandler.workflowStatus(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                               workflowName);
  }

  /**
   * Returns a list of flows associated with an account.
   */
  @GET
  @Path("/flows")
  public void getAllFlows(HttpRequest request, HttpResponder responder) {
    programLifecycleHttpHandler.getAllFlows(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Returns a list of procedures associated with an account.
   */
  @GET
  @Path("/procedures")
  public void getAllProcedures(HttpRequest request, HttpResponder responder) {
    programLifecycleHttpHandler.getAllProcedures(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Returns a list of map/reduces associated with an account.
   */
  @GET
  @Path("/mapreduce")
  public void getAllMapReduce(HttpRequest request, HttpResponder responder) {
    programLifecycleHttpHandler.getAllMapReduce(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Returns a list of spark jobs associated with an account.
   */
  @GET
  @Path("/spark")
  public void getAllSpark(HttpRequest request, HttpResponder responder) {
    programLifecycleHttpHandler.getAllSpark(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Returns a list of workflows associated with an account.
   */
  @GET
  @Path("/workflows")
  public void getAllWorkflows(HttpRequest request, HttpResponder responder) {
    programLifecycleHttpHandler.getAllWorkflows(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Returns a list of applications associated with an account.
   */
  @GET
  @Path("/apps")
  public void getAllApps(HttpRequest request, HttpResponder responder) {
    appLifecycleHttpHandler.getAllApps(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Returns the info associated with the application.
   */
  @GET
  @Path("/apps/{app-id}")
  public void getAppInfo(HttpRequest request, HttpResponder responder,
                         @PathParam("app-id") String appId) {
    appLifecycleHttpHandler.getAppInfo(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/flows")
  public void getFlowsByApp(HttpRequest request, HttpResponder responder,
                            @PathParam("app-id") String appId) {
    programLifecycleHttpHandler.getProgramsByApp(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                                 ProgramType.FLOW.getCategoryName());
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/procedures")
  public void getProceduresByApp(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") String appId) {
    programLifecycleHttpHandler.getProgramsByApp(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                                 ProgramType.PROCEDURE.getCategoryName());
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce")
  public void getMapreduceByApp(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") String appId) {
    programLifecycleHttpHandler.getProgramsByApp(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                                 ProgramType.MAPREDUCE.getCategoryName());
  }

  /**
   * Returns a list of spark jobs associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/spark")
  public void getSparkByApp(HttpRequest request, HttpResponder responder,
                            @PathParam("app-id") String appId) {
    programLifecycleHttpHandler.getProgramsByApp(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                                 ProgramType.SPARK.getCategoryName());
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/workflows")
  public void getWorkflowssByApp(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") String appId) {
    programLifecycleHttpHandler.getProgramsByApp(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                                 ProgramType.WORKFLOW.getCategoryName());
  }

  /**
   * Returns a list of streams associated with account.
   */
  @GET
  @Path("/streams")
  public void getStreams(HttpRequest request, HttpResponder responder) {
    appFabricStreamHttpHandler.getStreams(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                          Constants.DEFAULT_NAMESPACE);
  }

  /**
   * Returns a stream associated with account.
   */
  @GET
  @Path("/streams/{stream-id}")
  public void getStreamSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("stream-id") final String streamId) {
    dataList(request, responder, store, dsFramework, Data.STREAM, Constants.DEFAULT_NAMESPACE, streamId, null);
  }

  /**
   * Returns a list of streams associated with application.
   */
  @GET
  @Path("/apps/{app-id}/streams")
  public void getStreamsByApp(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId) {
    appFabricStreamHttpHandler.getStreamsByApp(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                               Constants.DEFAULT_NAMESPACE, appId);
  }

  /**
   * Returns a list of dataset associated with account.
   */
  @GET
  @Path("/datasets")
  public void getDatasets(HttpRequest request, HttpResponder responder) {
    dataList(request, responder, store, dsFramework, Data.DATASET, Constants.DEFAULT_NAMESPACE, null, null);
  }

  /**
   * Returns a dataset associated with account.
   */
  @GET
  @Path("/datasets/{dataset-id}")
  public void getDatasetSpecification(HttpRequest request, HttpResponder responder,
                                      @PathParam("dataset-id") final String datasetId) {
    dataList(request, responder, store, dsFramework, Data.DATASET, Constants.DEFAULT_NAMESPACE, datasetId, null);
  }

  /**
   * Returns a list of dataset associated with application.
   */
  @GET
  @Path("/apps/{app-id}/datasets")
  public void getDatasetsByApp(HttpRequest request, HttpResponder responder,
                               @PathParam("app-id") final String appId) {
    dataList(request, responder, store, dsFramework, Data.DATASET, Constants.DEFAULT_NAMESPACE, null, appId);
  }

  /**
   * Returns all flows associated with a stream.
   */
  @GET
  @Path("/streams/{stream-id}/flows")
  public void getFlowsByStream(HttpRequest request, HttpResponder responder,
                               @PathParam("stream-id") final String streamId) {
    appFabricStreamHttpHandler.getFlowsByStream(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                                Constants.DEFAULT_NAMESPACE, streamId);
  }

  /**
   * Returns all flows associated with a dataset.
   */
  @GET
  @Path("/datasets/{dataset-id}/flows")
  public void getFlowsByDataset(HttpRequest request, HttpResponder responder,
                                @PathParam("dataset-id") final String datasetId) {
    programListByDataAccess(request, responder, store, dsFramework, ProgramType.FLOW, Data.DATASET,
                            Constants.DEFAULT_NAMESPACE, datasetId);
  }

  /**
   * DO NOT DOCUMENT THIS API.
   */
  @POST
  @Path("/unrecoverable/reset")
  public void resetCDAP(HttpRequest request, HttpResponder responder) {

    try {
      if (!configuration.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET,
                                    Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET)) {
        responder.sendStatus(HttpResponseStatus.FORBIDDEN);
        return;
      }
      String account = getAuthenticatedAccountId(request);
      final Id.Namespace namespaceId = Id.Namespace.from(account);

      // Check if any program is still running
      boolean appRunning = appLifecycleHttpHandler.checkAnyRunning(new Predicate<Id.Program>() {
        @Override
        public boolean apply(Id.Program programId) {
          return programId.getNamespaceId().equals(namespaceId.getId());
        }
      }, ProgramType.values());

      if (appRunning) {
        throw new Exception("Cannot reset while programs are running");
      }

      LOG.info("Deleting all data for account '" + account + "'.");

      // remove preferences stored at instance level
      preferencesStore.deleteProperties();

      // remove all data in consolesettings
      consoleSettingsStore.delete();

      dsFramework.deleteAllInstances();
      dsFramework.deleteAllModules();

      // todo: do efficiently and also remove timeseries metrics as well: CDAP-1125
      appLifecycleHttpHandler.deleteMetrics(account, null);
      // delete all meta data
      store.removeAll(namespaceId);
      // todo: delete only for specified account
      // delete queues and streams data
      queueAdmin.dropAll();
      streamAdmin.dropAll();

      LOG.info("All data for account '" + account + "' deleted.");
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.warn(e.getMessage(), e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format(UserMessages.getMessage(UserErrors.RESET_FAIL), e.getMessage()));
    }
  }

  /**
   * DO NOT DOCUMENT THIS API.
   */
  @DELETE
  @Path("/unrecoverable/data/datasets")
  public void deleteDatasets(HttpRequest request, HttpResponder responder) {

    try {
      if (!configuration.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET,
                                    Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET)) {
        responder.sendStatus(HttpResponseStatus.FORBIDDEN);
        return;
      }
      String account = getAuthenticatedAccountId(request);
      final Id.Namespace namespaceId = Id.Namespace.from(account);

      // Check if any program is still running
      boolean appRunning = appLifecycleHttpHandler.checkAnyRunning(new Predicate<Id.Program>() {
        @Override
        public boolean apply(Id.Program programId) {
          return programId.getNamespaceId().equals(namespaceId.getId());
        }
      }, ProgramType.values());

      if (appRunning) {
        throw new Exception("Cannot delete all datasets while programs are running");
      }

      LOG.info("Deleting all datasets for account '" + account + "'.");

      dsFramework.deleteAllInstances();

      LOG.info("All datasets for account '" + account + "' deleted.");
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.warn(e.getMessage(), e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format(UserMessages.getMessage(UserErrors.DATASETS_DELETE_FAIL), e.getMessage()));
    }
  }
}
