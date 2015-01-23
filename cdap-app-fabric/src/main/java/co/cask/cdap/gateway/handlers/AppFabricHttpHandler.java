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

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.services.Data;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
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
import co.cask.cdap.proto.DatasetRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.StreamRecord;
import co.cask.http.BodyConsumer;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
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
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

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
   * Returns status of a runnable specified by the type{flows,workflows,mapreduce,spark,procedures,services}.
   */
  @GET
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/status")
  public void getStatus(final HttpRequest request, final HttpResponder responder,
                        @PathParam("app-id") final String appId,
                        @PathParam("runnable-type") final String runnableType,
                        @PathParam("runnable-id") final String runnableId) {
    programLifecycleHttpHandler.getStatus(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId,
                                          runnableType, runnableId);
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
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/start")
  public void startProgram(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") final String appId,
                           @PathParam("runnable-type") final String runnableType,
                           @PathParam("runnable-id") final String runnableId) {
    programLifecycleHttpHandler.startStopDebugProgram(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                      appId, runnableType, runnableId, "start");
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
    programLifecycleHttpHandler.startStopDebugProgram(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                      appId, runnableType, runnableId, "debug");
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
    programLifecycleHttpHandler.startStopDebugProgram(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                      appId, runnableType, runnableId, "stop");
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

  //TODO [SAGAR]: CDAP-1155: Implement API to get ScheduleSpecification given schedule name
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
   * Get schedule state.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules/{schedule-id}/status")
  public void getScheduleState(HttpRequest request, HttpResponder responder,
                               @PathParam("app-id") String appId,
                               @PathParam("workflow-id") String workflowId,
                               @PathParam("schedule-id") String scheduleId) {
    programLifecycleHttpHandler.getScheduleState(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                 appId, workflowId, scheduleId);
  }

  /**
   * Suspend a workflow schedule.
   */
  @POST
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules/{schedule-id}/suspend")
  public void workflowScheduleSuspend(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId,
                                      @PathParam("workflow-id") String workflowId,
                                      @PathParam("schedule-id") String scheduleId) {
    programLifecycleHttpHandler.workflowScheduleSuspend(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                        appId, workflowId, scheduleId);
  }

  /**
   * Resume a workflow schedule.
   */
  @POST
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules/{schedule-id}/resume")
  public void workflowScheduleResume(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") String appId,
                                     @PathParam("workflow-id") String workflowId,
                                     @PathParam("schedule-id") String scheduleId) {
    programLifecycleHttpHandler.workflowScheduleResume(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE,
                                                       appId, workflowId, scheduleId);
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
  public void clearQueues(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.QUEUES);
  }

  @DELETE
  @Path("/streams")
  public void clearStreams(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.STREAMS);
  }

  private static enum ToClear {
    QUEUES, STREAMS
  }

  private void clear(HttpRequest request, final HttpResponder responder, ToClear toClear) {
    try {
      getAuthenticatedAccountId(request);
      try {
        if (toClear == ToClear.QUEUES) {
          queueAdmin.dropAll();
        } else if (toClear == ToClear.STREAMS) {
          streamAdmin.dropAll();
        }
        responder.sendStatus(HttpResponseStatus.OK);
      } catch (Exception e) {
        LOG.error("Exception clearing data fabric: ", e);
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
    dataList(request, responder, Data.STREAM, null, null);
  }

  /**
   * Returns a stream associated with account.
   */
  @GET
  @Path("/streams/{stream-id}")
  public void getStreamSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("stream-id") final String streamId) {
    dataList(request, responder, Data.STREAM, streamId, null);
  }

  /**
   * Returns a list of streams associated with application.
   */
  @GET
  @Path("/apps/{app-id}/streams")
  public void getStreamsByApp(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId) {
    dataList(request, responder, Data.STREAM, null, appId);
  }

  /**
   * Returns a list of dataset associated with account.
   */
  @GET
  @Path("/datasets")
  public void getDatasets(HttpRequest request, HttpResponder responder) {
    dataList(request, responder, Data.DATASET, null, null);
  }

  /**
   * Returns a dataset associated with account.
   */
  @GET
  @Path("/datasets/{dataset-id}")
  public void getDatasetSpecification(HttpRequest request, HttpResponder responder,
                                      @PathParam("dataset-id") final String datasetId) {
    dataList(request, responder, Data.DATASET, datasetId, null);
  }

  /**
   * Returns a list of dataset associated with application.
   */
  @GET
  @Path("/apps/{app-id}/datasets")
  public void getDatasetsByApp(HttpRequest request, HttpResponder responder,
                               @PathParam("app-id") final String appId) {
    dataList(request, responder, Data.DATASET, null, appId);
  }

  private void dataList(HttpRequest request, HttpResponder responder, Data type, String name, String appId) {
    try {
      if ((name != null && name.isEmpty()) || (appId != null && appId.isEmpty())) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Empty name provided");
        return;
      }

      String accountId = getAuthenticatedAccountId(request);
      Id.Program program = Id.Program.from(accountId, appId == null ? "" : appId, "");
      String json = name != null ? getDataEntity(program, type, name) :
        appId != null ? listDataEntitiesByApp(program, type) : listDataEntities(program, type);
      if (json.isEmpty()) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                                ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception : ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private String getDataEntity(Id.Program programId, Data type, String name) {
    Id.Namespace namespace = new Id.Namespace(programId.getNamespaceId());
    if (type == Data.DATASET) {
      DatasetSpecification dsSpec = getDatasetSpec(name);
      String typeName = null;
      if (dsSpec != null) {
        typeName = dsSpec.getType();
      }
      return GSON.toJson(makeDataSetRecord(name, typeName));
    } else if (type == Data.STREAM) {
      StreamSpecification spec = store.getStream(namespace, name);
      return spec == null ? "" : GSON.toJson(makeStreamRecord(spec.getName(), spec));
    }
    return "";
  }

  private String listDataEntities(Id.Program programId, Data type) throws Exception {
    if (type == Data.DATASET) {
      Collection<DatasetSpecification> instances = dsFramework.getInstances();
      List<DatasetRecord> result = Lists.newArrayListWithExpectedSize(instances.size());
      for (DatasetSpecification instance : instances) {
        result.add(makeDataSetRecord(instance.getName(), instance.getType()));
      }
      return GSON.toJson(result);
    } else if (type == Data.STREAM) {
      Collection<StreamSpecification> specs = store.getAllStreams(new Id.Namespace(programId.getNamespaceId()));
      List<StreamRecord> result = Lists.newArrayListWithExpectedSize(specs.size());
      for (StreamSpecification spec : specs) {
        result.add(makeStreamRecord(spec.getName(), null));
      }
      return GSON.toJson(result);
    }
    return "";

  }

  private String listDataEntitiesByApp(Id.Program programId, Data type) throws Exception {
    Id.Namespace namespace = new Id.Namespace(programId.getNamespaceId());
    ApplicationSpecification appSpec = store.getApplication(new Id.Application(
      namespace, programId.getApplicationId()));
    if (type == Data.DATASET) {
      Set<String> dataSetsUsed = dataSetsUsedBy(appSpec);
      List<DatasetRecord> result = Lists.newArrayListWithExpectedSize(dataSetsUsed.size());
      for (String dsName : dataSetsUsed) {
        String typeName = null;
        DatasetSpecification dsSpec = getDatasetSpec(dsName);
        if (dsSpec != null) {
          typeName = dsSpec.getType();
        }
        result.add(makeDataSetRecord(dsName, typeName));
      }
      return GSON.toJson(result);
    }
    if (type == Data.STREAM) {
      Set<String> streamsUsed = streamsUsedBy(appSpec);
      List<StreamRecord> result = Lists.newArrayListWithExpectedSize(streamsUsed.size());
      for (String streamName : streamsUsed) {
        result.add(makeStreamRecord(streamName, null));
      }
      return GSON.toJson(result);
    }
    return "";
  }

  @Nullable
  private DatasetSpecification getDatasetSpec(String dsName) {
    try {
      return dsFramework.getDatasetSpec(dsName);
    } catch (Exception e) {
      LOG.warn("Couldn't get spec for dataset: " + dsName);
      return null;
    }
  }

  private Set<String> dataSetsUsedBy(FlowSpecification flowSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowletDefinition flowlet : flowSpec.getFlowlets().values()) {
      result.addAll(flowlet.getDatasets());
    }
    return result;
  }

  private Set<String> dataSetsUsedBy(ApplicationSpecification appSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
      result.addAll(dataSetsUsedBy(flowSpec));
    }
    for (ProcedureSpecification procSpec : appSpec.getProcedures().values()) {
      result.addAll(procSpec.getDataSets());
    }
    for (MapReduceSpecification mrSpec : appSpec.getMapReduce().values()) {
      result.addAll(mrSpec.getDataSets());
    }
    return result;
  }

  private Set<String> streamsUsedBy(FlowSpecification flowSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowletConnection con : flowSpec.getConnections()) {
      if (FlowletConnection.Type.STREAM == con.getSourceType()) {
        result.add(con.getSourceName());
      }
    }
    return result;
  }

  private Set<String> streamsUsedBy(ApplicationSpecification appSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
      result.addAll(streamsUsedBy(flowSpec));
    }
    result.addAll(appSpec.getStreams().keySet());
    return result;
  }

  /**
   * Returns all flows associated with a stream.
   */
  @GET
  @Path("/streams/{stream-id}/flows")
  public void getFlowsByStream(HttpRequest request, HttpResponder responder,
                               @PathParam("stream-id") final String streamId) {
    programListByDataAccess(request, responder, ProgramType.FLOW, Data.STREAM, streamId);
  }

  /**
   * Returns all flows associated with a dataset.
   */
  @GET
  @Path("/datasets/{dataset-id}/flows")
  public void getFlowsByDataset(HttpRequest request, HttpResponder responder,
                                @PathParam("dataset-id") final String datasetId) {
    programListByDataAccess(request, responder, ProgramType.FLOW, Data.DATASET, datasetId);
  }

  private void programListByDataAccess(HttpRequest request, HttpResponder responder,
                                       ProgramType type, Data data, String name) {
    try {
      if (name.isEmpty()) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, data.prettyName().toLowerCase() + " name is empty");
        return;
      }
      String accountId = getAuthenticatedAccountId(request);
      Id.Program programId = Id.Program.from(accountId, "", "");
      List<ProgramRecord> programRecords = listProgramsByDataAccess(programId, type, data, name);
      if (programRecords == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendJson(HttpResponseStatus.OK, programRecords);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * @return list of program records, an empty list if no programs were found, or null if the stream or
   * dataset does not exist
   */
  private List<ProgramRecord> listProgramsByDataAccess(Id.Program programId, ProgramType type,
                                                       Data data, String name) throws Exception {
    // search all apps for programs that use this
    List<ProgramRecord> result = Lists.newArrayList();
    Collection<ApplicationSpecification> appSpecs = store.getAllApplications(
      new Id.Namespace(programId.getNamespaceId()));
    if (appSpecs != null) {
      for (ApplicationSpecification appSpec : appSpecs) {
        if (type == ProgramType.FLOW) {
          for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
            if ((data == Data.DATASET && usesDataSet(flowSpec, name))
              || (data == Data.STREAM && usesStream(flowSpec, name))) {
              result.add(makeProgramRecord(appSpec.getName(), flowSpec, ProgramType.FLOW));
            }
          }
        } else if (type == ProgramType.PROCEDURE) {
          for (ProcedureSpecification procedureSpec : appSpec.getProcedures().values()) {
            if (data == Data.DATASET && procedureSpec.getDataSets().contains(name)) {
              result.add(makeProgramRecord(appSpec.getName(), procedureSpec, ProgramType.PROCEDURE));
            }
          }
        } else if (type == ProgramType.MAPREDUCE) {
          for (MapReduceSpecification mrSpec : appSpec.getMapReduce().values()) {
            if (data == Data.DATASET && mrSpec.getDataSets().contains(name)) {
              result.add(makeProgramRecord(appSpec.getName(), mrSpec, ProgramType.MAPREDUCE));
            }
          }
        }
      }
    }
    if (!result.isEmpty()) {
      return result;
    }
    // if no programs were found, check whether the data exists, return [] if yes, null if not
    boolean exists = false;
    if (data == Data.DATASET) {
      exists = dsFramework.hasInstance(name);
    } else if (data == Data.STREAM) {
      exists = store.getStream(new Id.Namespace(Constants.DEFAULT_NAMESPACE), name) != null;
    }
    return exists ? result : null;
  }

  private static boolean usesDataSet(FlowSpecification flowSpec, String dataset) {
    for (FlowletDefinition flowlet : flowSpec.getFlowlets().values()) {
      if (flowlet.getDatasets().contains(dataset)) {
        return true;
      }
    }
    return false;
  }

  private static boolean usesStream(FlowSpecification flowSpec, String stream) {
    for (FlowletConnection con : flowSpec.getConnections()) {
      if (FlowletConnection.Type.STREAM == con.getSourceType() && stream.equals(con.getSourceName())) {
        return true;
      }
    }
    return false;
  }

   /* -----------------  helpers to return Json consistently -------------- */

  private static DatasetRecord makeDataSetRecord(String name, String classname) {
    return new DatasetRecord("Dataset", name, name, classname);
  }

  private static StreamRecord makeStreamRecord(String name, StreamSpecification specification) {
    return new StreamRecord("Stream", name, name, GSON.toJson(specification));
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
