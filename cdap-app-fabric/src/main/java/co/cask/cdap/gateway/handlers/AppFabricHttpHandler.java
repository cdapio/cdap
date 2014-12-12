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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.services.Data;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.discovery.TimeLimitEndpointStrategy;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.schedule.ScheduledRuntime;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.DatasetRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.NotRunningProgramLiveInfo;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.StreamRecord;
import co.cask.http.BodyConsumer;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.ning.http.client.Body;
import com.ning.http.client.BodyGenerator;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.commons.io.IOUtils;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
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
   * Timeout to upload to remote app fabric.
   */
  private static final long UPLOAD_TIMEOUT = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

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
   * Client talking to transaction system.
   */
  private TransactionSystemClient txClient;

  /**
   * Access Dataset Service
   */
  private final DatasetFramework dsFramework;

  /**
   * App fabric output directory.
   */
  private final String appFabricDir;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final WorkflowClient workflowClient;

  private final DiscoveryServiceClient discoveryServiceClient;

  private final QueueAdmin queueAdmin;

  private final StreamAdmin streamAdmin;

  /**
   * V3 API Handler
   */
  private final AppLifecycleHttpHandler appLifecycleHttpHandler;

  private final Scheduler scheduler;

  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  @Inject
  public AppFabricHttpHandler(Authenticator authenticator, CConfiguration configuration,
                              LocationFactory locationFactory,
                              StoreFactory storeFactory,
                              ProgramRuntimeService runtimeService, StreamAdmin streamAdmin,
                              WorkflowClient workflowClient, Scheduler service, QueueAdmin queueAdmin,
                              DiscoveryServiceClient discoveryServiceClient, TransactionSystemClient txClient,
                              DatasetFramework dsFramework, AppLifecycleHttpHandler appLifecycleHttpHandler) {

    super(authenticator);
    this.locationFactory = locationFactory;
    this.streamAdmin = streamAdmin;
    this.configuration = configuration;
    this.runtimeService = runtimeService;
    this.appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR,
                                          System.getProperty("java.io.tmpdir"));
    this.store = storeFactory.create();
    this.workflowClient = workflowClient;
    this.scheduler = service;
    this.discoveryServiceClient = discoveryServiceClient;
    this.queueAdmin = queueAdmin;
    this.txClient = txClient;
    this.dsFramework =
      new NamespacedDatasetFramework(dsFramework,
                                     new DefaultDatasetNamespace(configuration, Namespace.USER));
    this.appLifecycleHttpHandler = appLifecycleHttpHandler;
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

    try {
      String accountId = getAuthenticatedAccountId(request);
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
   *
   * @param id The Program Id to get the status of
   * @param type The Type of the Program to get the status of
   * @throws RuntimeException if failed to determine the program status
   */
  private StatusMap getStatus(final Id.Program id, final ProgramType type) {
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

  /**
   * starts a webapp.
   */
  @POST
  @Path("/apps/{app-id}/webapp/start")
  public void webappStart(final HttpRequest request, final HttpResponder responder,
                          @PathParam("app-id") final String appId) {
    runnableStartStop(request, responder, appId, ProgramType.WEBAPP.getPrettyName().toLowerCase(),
                      ProgramType.WEBAPP, "start");
  }


  /**
   * stops a webapp.
   */
  @POST
  @Path("/apps/{app-id}/webapp/stop")
  public void webappStop(final HttpRequest request, final HttpResponder responder,
                         @PathParam("app-id") final String appId) {
    runnableStartStop(request, responder, appId, ProgramType.WEBAPP.getPrettyName().toLowerCase(),
                      ProgramType.WEBAPP, "stop");
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

  private void runnableStatus(HttpResponder responder, Id.Program id, ProgramType type) {
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
    if (!("flows".equals(runnableType) || "procedures".equals(runnableType) || "services".equals(runnableType))) {
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
    ProgramType type = ProgramType.valueOfCategoryName(runnableType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }
    long start = (startTs == null || startTs.isEmpty()) ? Long.MIN_VALUE : Long.parseLong(startTs);
    long end = (endTs == null || endTs.isEmpty()) ? Long.MAX_VALUE : Long.parseLong(endTs);
    getRuns(request, responder, appId, runnableId, status, start, end, resultLimit);
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
    ProgramType type = ProgramType.valueOfCategoryName(runnableType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    String accountId = getAuthenticatedAccountId(request);
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

  /**
   * Save runnable runtime args.
   */
  @PUT
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/runtimeargs")
  public void saveRunnableRuntimeArgs(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") final String appId,
                                      @PathParam("runnable-type") final String runnableType,
                                      @PathParam("runnable-id") final String runnableId) {
    ProgramType type = ProgramType.valueOfCategoryName(runnableType);
    if (type == null || type == ProgramType.WEBAPP) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    String accountId = getAuthenticatedAccountId(request);
    Id.Program id = Id.Program.from(accountId, appId, runnableId);


    try {
      if (!store.programExists(id, type)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Runnable not found");
        return;
      }
      Map<String, String> args = decodeArguments(request);
      store.storeRunArguments(id, args);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable e) {
      LOG.error("Error getting runtime args {}", e.getMessage(), e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void getRuns(HttpRequest request, HttpResponder responder, String appId,
                       String runnableId, String status, long start, long end, int limit) {
    try {
      String accountId = getAuthenticatedAccountId(request);
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

  private synchronized void startStopProgram(HttpRequest request, HttpResponder responder,
                                             final String appId, final String runnableType,
                                             final String runnableId, final String action) {
    ProgramType type = ProgramType.valueOfCategoryName(runnableType);

    if (type == null || (type == ProgramType.WORKFLOW && "stop".equals(action))) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      LOG.trace("{} call from AppFabricHttpHandler for app {}, flow type {} id {}",
                action, appId, runnableType, runnableId);
      runnableStartStop(request, responder, appId, runnableId, type, action);
    }
  }

  private void runnableStartStop(HttpRequest request, HttpResponder responder,
                                 String appId, String runnableId, ProgramType type,
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


  private boolean isRunning(Id.Program id, ProgramType type) {
    String programStatus = getStatus(id, type).getStatus();
    return programStatus != null && !"STOPPED".equals(programStatus);
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
      responder.sendJson(HttpResponseStatus.OK, new Instances(count));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      if (respondIfElementNotFound(e, responder)) {
        return;
      }
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
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
    try {
      String accountId = getAuthenticatedAccountId(request);
      List<BatchEndpointInstances> args = instancesFromBatchArgs(decodeArrayArguments(request, responder));
      // if args is null then the response has already been sent
      if (args == null) {
        return;
      }
      for (BatchEndpointInstances requestedObj : args) {
        String appId = requestedObj.getAppId();
        String programTypeStr = requestedObj.getProgramType();
        String programId = requestedObj.getProgramId();
        // these values will be overwritten later
        int requested, provisioned;
        ApplicationSpecification spec = store.getApplication(Id.Application.from(accountId, appId));
        if (spec == null) {
          addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(), "App: " + appId + " not found");
          continue;
        }

        ProgramType programType = ProgramType.valueOfPrettyName(programTypeStr);

        // cant get instances for things that are not flows, services, or procedures
        if (!EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.PROCEDURE).contains(programType)) {
          addCodeError(requestedObj, HttpResponseStatus.BAD_REQUEST.getCode(),
                       "Program type: " + programType + " is not a valid program type to get instances");
          continue;
        }

        String runnableId;
        if (programType == ProgramType.PROCEDURE) {
          // the "runnable" for procedures has the same id as the procedure name
          runnableId = programId;
          if (!spec.getProcedures().containsKey(programId)) {
            addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(),
                         "Procedure: " + programId + " not found");
            continue;
          }
          requested = store.getProcedureInstances(Id.Program.from(accountId, appId, programId));

        } else {
          // services and flows must have runnable id
          if (requestedObj.getRunnableId() == null) {
            addCodeError(requestedObj, HttpResponseStatus.BAD_REQUEST.getCode(),
                         "Must provide a string runnableId for flows/services");
            continue;
          }

          runnableId = requestedObj.getRunnableId();
          if (programType == ProgramType.FLOW) {
            FlowSpecification flowSpec = spec.getFlows().get(programId);
            if (flowSpec == null) {
              addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(), "Flow: " + programId + " not found");
              continue;
            }

            FlowletDefinition flowletDefinition = flowSpec.getFlowlets().get(runnableId);
            if (flowletDefinition == null) {
              addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(),
                           "Flowlet: " + runnableId + " not found");
              continue;
            }
            requested = flowletDefinition.getInstances();

         } else {
            // Services
            ServiceSpecification serviceSpec = spec.getServices().get(programId);
            if (serviceSpec == null) {
              addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(),
                           "Service: " + programId + " not found");
              continue;
            }

            if (serviceSpec.getName().equals(runnableId)) {
              // If runnable name is the same as the service name, returns the service http server instances
              requested = serviceSpec.getInstances();
            } else {
              // Otherwise, get it from the worker
              ServiceWorkerSpecification workerSpec = serviceSpec.getWorkers().get(runnableId);
              if (workerSpec == null) {
                addCodeError(requestedObj, HttpResponseStatus.NOT_FOUND.getCode(),
                             "Runnable: " + runnableId + " not found");
                continue;
              }
              requested = workerSpec.getInstances();
            }
          }
        }
        // use the pretty name of program types to be consistent
        requestedObj.setProgramType(programType.getPrettyName());
        provisioned = getRunnableCount(accountId, appId, programType, programId, runnableId);
        requestedObj.setStatusCode(HttpResponseStatus.OK.getCode());
        requestedObj.setRequested(requested);
        requestedObj.setProvisioned(provisioned);
      }
      responder.sendJson(HttpResponseStatus.OK, args);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (JsonSyntaxException e) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
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
    try {
      String accountId = getAuthenticatedAccountId(request);
      List<BatchEndpointStatus> args = statusFromBatchArgs(decodeArrayArguments(request, responder));
      // if args is null, then there was an error in decoding args and response was already sent
      if (args == null) {
        return;
      }
      for (int i = 0; i < args.size(); ++i) {
        BatchEndpointStatus requestedObj = args.get(i);
        Id.Program progId = Id.Program.from(accountId, requestedObj.getAppId(), requestedObj.getProgramId());
        ProgramType programType = ProgramType.valueOfPrettyName(requestedObj.getProgramType());
        // get th statuses
        StatusMap statusMap = getStatus(progId, programType);
        if (statusMap.getStatus() != null) {
          requestedObj.setStatusCode(HttpResponseStatus.OK.getCode());
          requestedObj.setStatus(statusMap.getStatus());
        } else {
          requestedObj.setStatusCode(statusMap.getStatusCode());
          requestedObj.setError(statusMap.getError());
        }
        // set the program type to the pretty name in case the request originally didn't have pretty name
        requestedObj.setProgramType(programType.getPrettyName());
      }
      responder.sendJson(HttpResponseStatus.OK, args);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Adds the status code and error to the JsonObject. The JsonObject will have 2 new properties:
   * 'statusCode': code, 'error': error
   *
   * @param object The JsonObject to add the code and error to
   * @param code The status code to add
   * @param error The error message to add
   */
  private void addCodeError(BatchEndpointArgs object, int code, String error) {
    object.setStatusCode(code);
    object.setError(error);
  }

  /**
   * Returns the number of instances currently running for different runnables for different programs
   *
   * @param accountId
   * @param appId
   * @param programType
   * @param programId
   * @param runnableId
   * @return
   */
  private int getRunnableCount(String accountId, String appId, ProgramType programType,
                               String programId, String runnableId) {
    ProgramLiveInfo info = runtimeService.getLiveInfo(Id.Program.from(accountId, appId, programId), programType);
    int count = 0;
    if (info instanceof NotRunningProgramLiveInfo) {
      return count;
    } else if (info instanceof Containers) {
      Containers containers = (Containers) info;
      for (Containers.ContainerInfo container : containers.getContainers()) {
        if (container.getName().equals(runnableId)) {
          count++;
        }
      }
      return count;
    } else {
      // Not running on YARN default 1
      return 1;
    }
  }

  /**
   * Deserializes and parses the HttpRequest data into a list of JsonObjects. Checks the HttpRequest data to see that
   * the input has valid fields corresponding to the /instances and /status endpoints. If the input data is empty or
   * the data is not of the form of an array of json objects, it sends an appropriate response through the responder
   * and returns null.
   *
   * @param request The HttpRequest to parse
   * @param responder The HttpResponder used to send responses in case of errors
   * @return List of JsonObjects from the request data
   * @throws IOException Thrown in case of Exceptions when reading the http request data
   */
  @Nullable
  private List<BatchEndpointArgs> decodeArrayArguments(HttpRequest request, HttpResponder responder)
    throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Cannot read request");
      return null;
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      List<BatchEndpointArgs> input = GSON.fromJson(reader, new TypeToken<List<BatchEndpointArgs>>() { }.getType());
      for (int i = 0; i < input.size(); ++i) {
        BatchEndpointArgs requestedObj;
        try {
          requestedObj = input.get(i);
        } catch (ClassCastException e) {
          responder.sendString(HttpResponseStatus.BAD_REQUEST, "All elements in array must be valid JSON Objects");
          return null;
        }
        // make sure the following args exist
        if (requestedObj.getAppId() == null || requestedObj.getProgramId() == null ||
          requestedObj.getProgramType() == null) {
          responder.sendJson(HttpResponseStatus.BAD_REQUEST,
                             "Must provide appId, programType, and programId as strings for each object");
          return null;
        }
        // invalid type
        try {
          if (ProgramType.valueOfPrettyName(requestedObj.getProgramType()) == null) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST,
                               "Invalid program type provided: " + requestedObj.getProgramType());
            return null;
          }
        } catch (IllegalArgumentException e) {
          responder.sendJson(HttpResponseStatus.BAD_REQUEST,
                             "Invalid program type provided: " + requestedObj.getProgramType());
          return null;
        }

      }
      return input;
    } catch (JsonSyntaxException e) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Invalid Json object provided");
      return null;
    } finally {
      reader.close();
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
    int instances = 0;
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
      if (oldInstances != instances) {
        store.setFlowletInstances(programID, flowletId, instances);
        ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(accountId, appId, flowId, ProgramType.FLOW,
                                                                        runtimeService);
        if (runtimeInfo != null) {
          runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                              ImmutableMap.of("flowlet", flowletId,
                                                              "newInstances", String.valueOf(instances),
                                                              "oldInstances", String.valueOf(oldInstances))).get();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      if (respondIfElementNotFound(e, responder)) {
        return;
      }
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Changes input stream for a flowlet connection.
   */
  @PUT
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/connections/{stream-id}")
  public void changeFlowletStreamConnection(HttpRequest request, HttpResponder responder,
                                            @PathParam("app-id") final String appId,
                                            @PathParam("flow-id") final String flowId,
                                            @PathParam("flowlet-id") final String flowletId,
                                            @PathParam("stream-id") final String streamId) throws IOException {

    try {
      Map<String, String> arguments = decodeArguments(request);
      String oldStreamId = arguments.get("oldStreamId");
      if (oldStreamId == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "oldStreamId param is required");
        return;
      }

      String accountId = getAuthenticatedAccountId(request);
      StreamSpecification stream = store.getStream(Id.Account.from(accountId), streamId);
      if (stream == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Stream specified with streamId param does not exist");
        return;
      }

      Id.Program programID = Id.Program.from(accountId, appId, flowId);
      store.changeFlowletSteamConnection(programID, flowletId, oldStreamId, streamId);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      if (respondIfElementNotFound(e, responder)) {
        return;
      }
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private ProgramStatus getProgramStatus(Id.Program id, ProgramType type) {
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

  /**
   * Deploys an application with the specified name.
   */
  @PUT
  @Path("/apps/{app-id}")
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder, @PathParam("app-id") final String appId) {
    try {
      return appLifecycleHttpHandler.deployAppStream(request, responder, DEFAULT_NAMESPACE, appId);
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
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder) {
    // null means use name provided by app spec
    try {
      return appLifecycleHttpHandler.deployAppStream(request, responder, DEFAULT_NAMESPACE, null);
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
                                  @PathParam("app-id") final String appId,
                                  @PathParam("workflow-id") final String workflowId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
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
      responder.sendJson(HttpResponseStatus.OK, scheduler.getScheduleIds(id, ProgramType.WORKFLOW));
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

  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/live-info")
  @SuppressWarnings("unused")
  public void procedureLiveInfo(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId,
                                @PathParam("procedure-id") final String procedureId) {
    getLiveInfo(request, responder, appId, procedureId, ProgramType.PROCEDURE, runtimeService);
  }

  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/live-info")
  @SuppressWarnings("unused")
  public void flowLiveInfo(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") final String appId,
                           @PathParam("flow-id") final String flowId) {
    getLiveInfo(request, responder, appId, flowId, ProgramType.FLOW, runtimeService);
  }

  /**
   * Returns specification of a runnable - flow.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}")
  public void flowSpecification(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId,
                                @PathParam("flow-id")final String flowId) {
    runnableSpecification(request, responder, appId, ProgramType.FLOW, flowId);
  }

  /**
   * Returns specification of procedure.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}")
  public void procedureSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("procedure-id")final String procId) {
    runnableSpecification(request, responder, appId, ProgramType.PROCEDURE, procId);
  }

  /**
   * Returns specification of mapreduce.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}")
  public void mapreduceSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("mapreduce-id")final String mapreduceId) {
    runnableSpecification(request, responder, appId, ProgramType.MAPREDUCE, mapreduceId);
  }

  /**
   * Returns specification of spark program.
   */
  @GET
  @Path("/apps/{app-id}/spark/{spark-id}")
  public void sparkSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("spark-id")final String sparkId) {
    runnableSpecification(request, responder, appId, ProgramType.SPARK, sparkId);
  }

  /**
   * Returns specification of workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}")
  public void workflowSpecification(HttpRequest request, HttpResponder responder,
                                    @PathParam("app-id") final String appId,
                                    @PathParam("workflow-id")final String workflowId) {
    runnableSpecification(request, responder, appId, ProgramType.WORKFLOW, workflowId);
  }

  @GET
  @Path("/apps/{app-id}/services/{service-id}")
  public void serviceSpecification(HttpRequest request, HttpResponder responder,
                                   @PathParam("app-id") String appId,
                                   @PathParam("service-id") String serviceId) {
    runnableSpecification(request, responder, appId, ProgramType.SERVICE, serviceId);
  }



  private void runnableSpecification(HttpRequest request, HttpResponder responder,
                                     final String appId, ProgramType runnableType,
                                     final String runnableId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
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

  /**
   * Gets application deployment status.
   */
  @GET
  @Path("/deploy/status")
  public void getDeployStatus(HttpRequest request, HttpResponder responder) {
    appLifecycleHttpHandler.getDeployStatus(responder, DEFAULT_NAMESPACE);
  }


  /**
   * Promote an application to another CDAP instance.
   */
  @POST
  @Path("/apps/{app-id}/promote")
  public void promoteApp(HttpRequest request, HttpResponder responder, @PathParam("app-id") final String appId) {
    try {
      String postBody = null;

      try {
        postBody = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));
      } catch (IOException e) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }

      Map<String, String> content = null;
      try {
        content = GSON.fromJson(postBody, STRING_MAP_TYPE);
      } catch (JsonSyntaxException e) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST, "Not a valid body specified.");
        return;
      }

      if (!content.containsKey("hostname")) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST, "Hostname not specified.");
        return;
      }

      // Checks DNS, Ipv4, Ipv6 address in one go.
      String hostname = content.get("hostname");
      Preconditions.checkArgument(!hostname.isEmpty(), "Empty hostname passed.");

      String accountId = getAuthenticatedAccountId(request);
      String token = request.getHeader(Constants.Gateway.API_KEY);

      final Location appArchive = store.getApplicationArchiveLocation(Id.Application.from(accountId, appId));
      if (appArchive == null || !appArchive.exists()) {
        throw new IOException("Unable to locate the application.");
      }

      if (!promote(token, accountId, appId, hostname)) {
        responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to promote application " + appId);
      } else {
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public boolean promote(String authToken, String accountId, String appId, String hostname) throws Exception {

    try {
      final Location appArchive = store.getApplicationArchiveLocation(Id.Application.from(accountId,
                                                                                          appId));
      if (appArchive == null || !appArchive.exists()) {
        throw new Exception("Unable to locate the application.");
      }

      String schema = "https";
      if ("localhost".equals(hostname)) {
        schema = "http";
      }

      // Construct URL for promotion of application to remote cluster
      int gatewayPort;
      if (configuration.getBoolean(Constants.Security.SSL_ENABLED)) {
        gatewayPort = Integer.parseInt(configuration.get(Constants.Router.ROUTER_SSL_PORT,
                                                         Constants.Router.DEFAULT_ROUTER_SSL_PORT));
      } else {
        gatewayPort = Integer.parseInt(configuration.get(Constants.Router.ROUTER_PORT,
                                                         Constants.Router.DEFAULT_ROUTER_PORT));
      }

      String url = String.format("%s://%s:%s/v2/apps/%s",
                                 schema, hostname, gatewayPort, appId);

      SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
        .setUrl(url)
        .setRequestTimeoutInMs((int) UPLOAD_TIMEOUT)
        .setHeader("X-Archive-Name", appArchive.getName())
        .setHeader(Constants.Gateway.API_KEY, authToken)
        .build();

      try {
        Future<Response> future = client.put(new LocationBodyGenerator(appArchive));
        Response response = future.get(UPLOAD_TIMEOUT, TimeUnit.MILLISECONDS);
        if (response.getStatusCode() != 200) {
          throw new RuntimeException(response.getResponseBody());
        }
        return true;
      } finally {
        client.close();
      }
    } catch (Exception ex) {
      LOG.warn(ex.getMessage(), ex);
      throw ex;
    }
  }

  private static final class LocationBodyGenerator implements BodyGenerator {

    private final Location location;

    private LocationBodyGenerator(Location location) {
      this.location = location;
    }

    @Override
    public Body createBody() throws IOException {
      final InputStream input = location.getInputStream();

      return new Body() {
        @Override
        public long getContentLength() {
          try {
            return location.length();
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public long read(ByteBuffer buffer) throws IOException {
          // Fast path
          if (buffer.hasArray()) {
            int len = input.read(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            if (len > 0) {
              buffer.position(buffer.position() + len);
            }
            return len;
          }

          byte[] bytes = new byte[buffer.remaining()];
          int len = input.read(bytes);
          if (len < 0) {
            return len;
          }
          buffer.put(bytes, 0, len);
          return len;
        }

        @Override
        public void close() throws IOException {
          input.close();
        }
      };
    }
  }

  /**
   * Delete an application specified by appId.
   */
  @DELETE
  @Path("/apps/{app-id}")
  public void deleteApp(HttpRequest request, HttpResponder responder,
                        @PathParam("app-id") final String appId) {
    try {
      Id.Program id = Id.Program.from(DEFAULT_NAMESPACE, appId, "");
      AppFabricServiceStatus appStatus = appLifecycleHttpHandler.removeApplication(id);
      LOG.trace("Delete call for Application {} at AppFabricHttpHandler", appId);
      responder.sendString(appStatus.getCode(), appStatus.getMessage());
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Deletes all applications in CDAP.
   */
  @DELETE
  @Path("/apps")
  public void deleteAllApps(HttpRequest request, HttpResponder responder) {
    try {
      Id.Account id = Id.Account.from(DEFAULT_NAMESPACE);
      AppFabricServiceStatus status = appLifecycleHttpHandler.removeAll(id);
      LOG.trace("Delete All call at AppFabricHttpHandler");
      responder.sendString(status.getCode(), status.getMessage());
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Deletes queues.
   */
  @DELETE
  @Path("/apps/{app-id}/flows/{flow-id}/queues")
  public void deleteFlowQueues(HttpRequest request, HttpResponder responder,
                               @PathParam("app-id") final String appId,
                               @PathParam("flow-id") final String flowId) {
    String accountId = getAuthenticatedAccountId(request);
    Id.Program programId = Id.Program.from(accountId, appId, flowId);
    try {
      ProgramStatus status = getProgramStatus(programId, ProgramType.FLOW);
      if (status.getStatus().equals(HttpResponseStatus.NOT_FOUND.toString())) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else if (status.getStatus().equals("RUNNING")) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "Flow is running, please stop it first.");
      } else {
        queueAdmin.dropAllForFlow(appId, flowId);
        // delete process metrics that are used to calculate the queue size (process.events.pending metric name)
        deleteProcessMetricsForFlow(appId, flowId);
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
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

  // deletes the process metrics for a flow
  private void deleteProcessMetricsForFlow(String application, String flow) throws IOException {
    Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(Constants.Service.METRICS);
    Discoverable discoverable = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoverables),
                                                              3L, TimeUnit.SECONDS).pick();

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      throw new IOException("Can't find Metrics endpoint");
    }

    LOG.debug("Deleting metrics for flow {}.{}", application, flow);
    String url = String.format("http://%s:%d%s/metrics/system/apps/%s/flows/%s?prefixEntity=process",
                               discoverable.getSocketAddress().getHostName(),
                               discoverable.getSocketAddress().getPort(),
                               Constants.Gateway.API_VERSION_2,
                               application, flow);

    long timeout = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) timeout)
      .build();

    try {
      client.delete().get(timeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error("exception making metrics delete call", e);
      Throwables.propagate(e);
    } finally {
      client.close();
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


  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(Id.Program identifier, ProgramType type) {
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

  @GET
  @Path("/apps/{app-id}/workflows/{workflow-name}/current")
  public void workflowStatus(HttpRequest request, final HttpResponder responder,
                             @PathParam("app-id") String appId, @PathParam("workflow-name") String workflowName) {

    try {
      String accountId = getAuthenticatedAccountId(request);
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

  /**
   * Returns a list of flows associated with an account.
   */
  @GET
  @Path("/flows")
  public void getAllFlows(HttpRequest request, HttpResponder responder) {
    programList(request, responder, ProgramType.FLOW, null, store);
  }

  /**
   * Returns a list of procedures associated with an account.
   */
  @GET
  @Path("/procedures")
  public void getAllProcedures(HttpRequest request, HttpResponder responder) {
    programList(request, responder, ProgramType.PROCEDURE, null, store);
  }

  /**
   * Returns a list of map/reduces associated with an account.
   */
  @GET
  @Path("/mapreduce")
  public void getAllMapReduce(HttpRequest request, HttpResponder responder) {
    programList(request, responder, ProgramType.MAPREDUCE, null, store);
  }

  /**
   * Returns a list of spark jobs associated with an account.
   */
  @GET
  @Path("/spark")
  public void getAllSpark(HttpRequest request, HttpResponder responder) {
    programList(request, responder, ProgramType.SPARK, null, store);
  }

  /**
   * Returns a list of workflows associated with an account.
   */
  @GET
  @Path("/workflows")
  public void getAllWorkflows(HttpRequest request, HttpResponder responder) {
    programList(request, responder, ProgramType.WORKFLOW, null, store);
  }

  /**
   * Returns a list of applications associated with an account.
   */
  @GET
  @Path("/apps")
  public void getAllApps(HttpRequest request, HttpResponder responder) {
    appLifecycleHttpHandler.getAppDetails(responder, DEFAULT_NAMESPACE, null);
  }

  /**
   * Returns the info associated with the application.
   */
  @GET
  @Path("/apps/{app-id}")
  public void getAppInfo(HttpRequest request, HttpResponder responder,
                         @PathParam("app-id") final String appId) {
    appLifecycleHttpHandler.getAppDetails(responder, DEFAULT_NAMESPACE, appId);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/flows")
  public void getFlowsByApp(HttpRequest request, HttpResponder responder,
                            @PathParam("app-id") final String appId) {
    programList(request, responder, ProgramType.FLOW, appId, store);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/procedures")
  public void getProceduresByApp(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") final String appId) {
    programList(request, responder, ProgramType.PROCEDURE, appId, store);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce")
  public void getMapreduceByApp(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId) {
    programList(request, responder, ProgramType.MAPREDUCE, appId, store);
  }

  /**
   * Returns a list of spark jobs associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/spark")
  public void getSparkByApp(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId) {
    programList(request, responder, ProgramType.SPARK, appId, store);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/workflows")
  public void getWorkflowssByApp(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") final String appId) {
    programList(request, responder, ProgramType.WORKFLOW, appId, store);
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

  private String getDataEntity(Id.Program programId, Data type, String name) throws Exception {
    try {
      Id.Account account = new Id.Account(programId.getAccountId());
      if (type == Data.DATASET) {
        DatasetSpecification dsSpec = getDatasetSpec(name);
        String typeName = null;
        if (dsSpec != null) {
          typeName = dsSpec.getType();
        }
        return GSON.toJson(makeDataSetRecord(name, typeName));
      } else if (type == Data.STREAM) {
        StreamSpecification spec = store.getStream(account, name);
        return spec == null ? "" : GSON.toJson(makeStreamRecord(spec.getName(), spec));
      }
      return "";
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw new Exception("Could not retrieve data specs for " + programId.toString() + ", reason: " + e.getMessage());
    }
  }

  private String listDataEntities(Id.Program programId, Data type) throws Exception {
    try {
      if (type == Data.DATASET) {
        Collection<DatasetSpecification> instances = dsFramework.getInstances();
        List<DatasetRecord> result = Lists.newArrayListWithExpectedSize(instances.size());
        for (DatasetSpecification instance : instances) {
          result.add(makeDataSetRecord(instance.getName(), instance.getType()));
        }
        return GSON.toJson(result);
      } else if (type == Data.STREAM) {
        Collection<StreamSpecification> specs = store.getAllStreams(new Id.Account(programId.getAccountId()));
        List<StreamRecord> result = Lists.newArrayListWithExpectedSize(specs.size());
        for (StreamSpecification spec : specs) {
          result.add(makeStreamRecord(spec.getName(), null));
        }
        return GSON.toJson(result);
      }
      return "";
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw new Exception("Could not retrieve data specs for " + programId.toString() + ", reason: " + e.getMessage());
    }
  }

  private String listDataEntitiesByApp(Id.Program programId, Data type) throws Exception {
    try {
      Id.Account account = new Id.Account(programId.getAccountId());
      ApplicationSpecification appSpec = store.getApplication(new Id.Application(
        account, programId.getApplicationId()));
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
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw new Exception("Could not retrieve data specs for " + programId.toString() + ", reason: " + e.getMessage());
    }
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
      String list = listProgramsByDataAccess(programId, type, data, name);
      if (list.isEmpty()) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendByteArray(HttpResponseStatus.OK, list.getBytes(Charsets.UTF_8),
                                ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private String listProgramsByDataAccess(Id.Program programId, ProgramType type, Data data,
                                          String name) throws Exception {
    try {
      List<ProgramRecord> result = Lists.newArrayList();
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(
        new Id.Account(programId.getAccountId()));
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
      return GSON.toJson(result);
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw new Exception("Could not retrieve application specs for " +
                                             programId.toString() + ", reason: " + e.getMessage());
    }
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
      final Id.Account accountId = Id.Account.from(account);

      // Check if any program is still running
      boolean appRunning = appLifecycleHttpHandler.checkAnyRunning(new Predicate<Id.Program>() {
        @Override
        public boolean apply(Id.Program programId) {
          return programId.getAccountId().equals(accountId.getId());
        }
      }, ProgramType.values());

      if (appRunning) {
        throw new Exception("Cannot reset while programs are running");
      }

      LOG.info("Deleting all data for account '" + account + "'.");

      dsFramework.deleteAllInstances();
      dsFramework.deleteAllModules();

      appLifecycleHttpHandler.deleteMetrics(account, null);
      // delete all meta data
      store.removeAll(accountId);
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
      final Id.Account accountId = Id.Account.from(account);

      // Check if any program is still running
      boolean appRunning = appLifecycleHttpHandler.checkAnyRunning(new Predicate<Id.Program>() {
        @Override
        public boolean apply(Id.Program programId) {
          return programId.getAccountId().equals(accountId.getId());
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

  /**
   * Convenience class for representing the necessary components in the batch endpoint.
   */
  private class BatchEndpointArgs {
    private String appId = null;
    private String programType = null;
    private String programId = null;
    private String runnableId = null;
    private String error = null;
    private Integer statusCode = null;

    private BatchEndpointArgs(String appId, String programType, String programId, String runnableId, String error,
                              Integer statusCode) {
      this.appId = appId;
      this.programType = programType;
      this.programId = programId;
      this.runnableId = runnableId;
      this.error = error;
      this.statusCode = statusCode;
    }

    public BatchEndpointArgs(BatchEndpointArgs arg) {
      this(arg.appId, arg.programType, arg.programId, arg.runnableId, arg.error, arg.statusCode);
    }

    public String getRunnableId() {
      return runnableId;
    }

    public void setRunnableId(String runnableId) {
      this.runnableId = runnableId;
    }

    public void setError(String error) {
      this.error = error;
    }

    public void setStatusCode(Integer statusCode) {
      this.statusCode = statusCode;
    }

    public int getStatusCode() {
      return statusCode;
    }

    public String getError() {
      return error;
    }

    public String getProgramId() {
      return programId;
    }

    public String getProgramType() {
      return programType;
    }

    public String getAppId() {
      return appId;
    }

    public void setProgramType(String programType) {
      this.programType = programType;
    }
  }

  private class BatchEndpointInstances extends BatchEndpointArgs {
    private Integer requested = null;
    private Integer provisioned = null;

    public BatchEndpointInstances(BatchEndpointArgs arg) {
      super(arg);
    }

    public Integer getProvisioned() {
      return provisioned;
    }

    public void setProvisioned(Integer provisioned) {
      this.provisioned = provisioned;
    }

    public Integer getRequested() {
      return requested;
    }

    public void setRequested(Integer requested) {
      this.requested = requested;
    }
  }

  private class BatchEndpointStatus extends BatchEndpointArgs {
    private String status = null;

    public BatchEndpointStatus(BatchEndpointArgs arg) {
      super(arg);
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }
  }

  private List<BatchEndpointInstances> instancesFromBatchArgs(List<BatchEndpointArgs> args) {
    if (args == null) {
      return null;
    }
    List<BatchEndpointInstances> retVal = new ArrayList<BatchEndpointInstances>(args.size());
    for (BatchEndpointArgs arg: args) {
      retVal.add(new BatchEndpointInstances(arg));
    }
    return retVal;
  }

  private List<BatchEndpointStatus> statusFromBatchArgs(List<BatchEndpointArgs> args) {
    if (args == null) {
      return null;
    }
    List<BatchEndpointStatus> retVal = new ArrayList<BatchEndpointStatus>(args.size());
    for (BatchEndpointArgs arg: args) {
      retVal.add(new BatchEndpointStatus(arg));
    }
    return retVal;
  }

  /**
   * Convenience class for representing the necessary components for retrieving status
   */
  private class StatusMap {
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
}
