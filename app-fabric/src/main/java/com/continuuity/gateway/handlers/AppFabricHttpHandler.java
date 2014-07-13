package com.continuuity.gateway.handlers;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
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
import com.continuuity.app.services.Data;
import com.continuuity.app.services.DeployStatus;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.datafabric.dataset.DatasetMetaTableUtil;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.NamespacedDatasetFramework;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.dataset.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.handlers.util.AbstractAppFabricHttpHandler;
import com.continuuity.gateway.util.Util;
import com.continuuity.http.BodyConsumer;
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
import com.continuuity.internal.app.runtime.flow.FlowUtils;
import com.continuuity.internal.app.runtime.schedule.ScheduledRuntime;
import com.continuuity.internal.app.runtime.schedule.Scheduler;
import com.continuuity.internal.filesystem.LocationCodec;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.metrics.MetricsConstants;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
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
import com.ning.http.client.Body;
import com.ning.http.client.BodyGenerator;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.commons.io.IOUtils;
import org.apache.twill.api.RunId;
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
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 *  HttpHandler class for app-fabric requests.
 */
@Path(Constants.Gateway.GATEWAY_VERSION) //this will be removed/changed when gateway goes.
public class AppFabricHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricHttpHandler.class);

  private static final java.lang.reflect.Type MAP_STRING_STRING_TYPE
    = new TypeToken<Map<String, String>>() { }.getType();

  /**
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

  /**
   * Timeout to get response from metrics system.
   */
  private static final long METRICS_SERVER_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  private static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";

  /**
   * Timeout to upload to remote app fabric.
   */
  private static final long UPLOAD_TIMEOUT = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

  private static final Map<String, Type> RUNNABLE_TYPE_MAP = new ImmutableMap.Builder<String, Type>()
    .put("mapreduce", Type.MAPREDUCE)
    .put("flows", Type.FLOW)
    .put("procedures", Type.PROCEDURE)
    .put("workflows", Type.WORKFLOW)
    .put("webapp", Type.WEBAPP)
    .put("services", Type.SERVICE)
    .build();

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
   * Maintains a mapping of transient session state. The state is stored in memory,
   * in case of failure, all the current running sessions will be terminated. As
   * per the current implementation only connection per account is allowed to upload.
   */
  private final Map<String, SessionInfo> sessions = Maps.newConcurrentMap();

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final WorkflowClient workflowClient;

  private final DiscoveryServiceClient discoveryServiceClient;

  private final QueueAdmin queueAdmin;

  private final DataSetInstantiatorFromMetaData datasetInstantiator;

  private final DataSetAccessor dataSetAccessor;

  private final StreamAdmin streamAdmin;

  private final StreamConsumerFactory streamConsumerFactory;

  /**
   * Number of seconds for timing out a service endpoint discovery.
   */
  private static final long DISCOVERY_TIMEOUT_SECONDS = 3;

  /**
   * The directory where the uploaded files would be placed.
   */
  private final String archiveDir;

  private final ManagerFactory<Location, ApplicationWithPrograms> managerFactory;
  private final Scheduler scheduler;

  private static final java.lang.reflect.Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private static final java.lang.reflect.Type LONG_MAP_TYPE = new TypeToken<Map<String, Long>>() { }.getType();

  private static final class AppFabricServiceStatus {

    private static final AppFabricServiceStatus OK = new AppFabricServiceStatus(HttpResponseStatus.OK, "");

    private static final AppFabricServiceStatus PROGRAM_STILL_RUNNING =
      new AppFabricServiceStatus(HttpResponseStatus.FORBIDDEN, "Program is still running");

    private static final AppFabricServiceStatus PROGRAM_ALREADY_RUNNING =
      new AppFabricServiceStatus(HttpResponseStatus.CONFLICT, "Program is already running");

    private static final AppFabricServiceStatus PROGRAM_ALREADY_STOPPED =
      new AppFabricServiceStatus(HttpResponseStatus.CONFLICT, "Program already stopped");

    private static final AppFabricServiceStatus RUNTIME_INFO_NOT_FOUND =
      new AppFabricServiceStatus(HttpResponseStatus.CONFLICT,
                                 UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));

    private static final AppFabricServiceStatus PROGRAM_NOT_FOUND =
      new AppFabricServiceStatus(HttpResponseStatus.NOT_FOUND, "Program not found");

    private static final AppFabricServiceStatus INTERNAL_ERROR =
      new AppFabricServiceStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal server error");

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
                              DataSetAccessor dataSetAccessor, LocationFactory locationFactory,
                              ManagerFactory<Location, ApplicationWithPrograms> managerFactory,
                              StoreFactory storeFactory,
                              ProgramRuntimeService runtimeService, StreamAdmin streamAdmin,
                              StreamConsumerFactory streamConsumerFactory,
                              WorkflowClient workflowClient, Scheduler service, QueueAdmin queueAdmin,
                              DiscoveryServiceClient discoveryServiceClient, TransactionSystemClient txClient,
                              DataSetInstantiatorFromMetaData datasetInstantiator,
                              DatasetFramework dsFramework) {

    super(authenticator);
    this.locationFactory = locationFactory;
    this.managerFactory = managerFactory;
    this.streamAdmin = streamAdmin;
    this.streamConsumerFactory = streamConsumerFactory;
    this.configuration = configuration;
    this.runtimeService = runtimeService;
    this.appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR,
                                          System.getProperty("java.io.tmpdir"));
    this.archiveDir = this.appFabricDir + "/archive";
    this.store = storeFactory.create();
    this.workflowClient = workflowClient;
    this.scheduler = service;
    this.discoveryServiceClient = discoveryServiceClient;
    this.queueAdmin = queueAdmin;
    this.txClient = txClient;
    this.dsFramework =
      new NamespacedDatasetFramework(dsFramework,
                                     new ReactorDatasetNamespace(configuration, DataSetAccessor.Namespace.USER));
    this.datasetInstantiator = datasetInstantiator;
    this.dataSetAccessor = dataSetAccessor;
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
        responder.sendChunkStart(HttpResponseStatus.OK, ImmutableMultimap.<String, String>of());
        while (true) {
          // netty doesn't copy the readBytes buffer, so we have to reallocate a new buffer
          byte[] readBytes = new byte[4096];
          int res = in.read(readBytes, 0, 4096);
          if (res == -1) {
            break;
          }
          responder.sendChunk(ChannelBuffers.wrappedBuffer(readBytes, 0, res));
        }
        responder.sendChunkEnd();
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
   * Returns status of a runnable specified by the type{flows,workflows,mapreduce,procedures}.
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
      final Type type = RUNNABLE_TYPE_MAP.get(runnableType);

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
                  responder.sendJson(HttpResponseStatus.OK, reply);
                } else {
                  //mapreduce name might follow the same format even when its not part of the workflow.
                  runnableStatus(responder, id, type);
                }
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
   * starts a webapp.
   */
  @POST
  @Path("/apps/{app-id}/webapp/start")
  public void webappStart(final HttpRequest request, final HttpResponder responder,
                          @PathParam("app-id") final String appId) {
    runnableStartStop(request, responder, appId, Type.WEBAPP.prettyName().toLowerCase(), Type.WEBAPP, "start");
  }


  /**
   * stops a webapp.
   */
  @POST
  @Path("/apps/{app-id}/webapp/stop")
  public void webappStop(final HttpRequest request, final HttpResponder responder,
                         @PathParam("app-id") final String appId) {
    runnableStartStop(request, responder, appId, Type.WEBAPP.prettyName().toLowerCase(), Type.WEBAPP, "stop");
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
      Id.Program id = Id.Program.from(accountId, appId, Type.WEBAPP.prettyName().toLowerCase());
      runnableStatus(responder, id, Type.WEBAPP);
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
   * Returns program run history.
   */
  @GET
  @Path("/apps/{app-id}/{runnable-type}/{runnable-id}/history")
  public void runnableHistory(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId,
                              @PathParam("runnable-type") final String runnableType,
                              @PathParam("runnable-id") final String runnableId) {
    Type type = RUNNABLE_TYPE_MAP.get(runnableType);
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
    Type type = RUNNABLE_TYPE_MAP.get(runnableType);
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
    Type type = RUNNABLE_TYPE_MAP.get(runnableType);
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
    Type type = RUNNABLE_TYPE_MAP.get(runnableType);

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
  private AppFabricServiceStatus start(final Id.Program id, Type type, Map<String, String> overrides, boolean debug) {

    try {
      ProgramRuntimeService.RuntimeInfo existingRuntimeInfo = findRuntimeInfo(id, type);
      if (existingRuntimeInfo != null) {
        return AppFabricServiceStatus.PROGRAM_ALREADY_RUNNING;
      }

      Program program = store.loadProgram(id, type);
      if (program == null) {
        return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
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
    } catch (DataSetInstantiationException e) {
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
      int count = getProgramInstances(Id.Program.from(accountId, appId, procedureId));
      JsonObject json = new JsonObject();
      json.addProperty("instances", count);

      responder.sendJson(HttpResponseStatus.OK, json);
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
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, Type.PROCEDURE);
      if (runtimeInfo != null) {
        runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                            ImmutableMap.of(programId.getId(), instances)).get();
      }
    } catch (Throwable throwable) {
      LOG.warn("Exception when getting instances for {}.{} to {}. {}",
               programId.getId(), Type.PROCEDURE.prettyName(), throwable.getMessage(), throwable);
      throw new Exception(throwable.getMessage());
    }
  }

  private int getProgramInstances(Id.Program programId) throws Exception {
    try {
      return store.getProcedureInstances(programId);
    } catch (Throwable throwable) {
      LOG.warn("Exception when getting instances for {}.{} to {}.{}",
               programId.getId(), Type.PROCEDURE.prettyName(), throwable.getMessage(), throwable);
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
        ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(accountId, appId, flowId, Type.FLOW);
        if (runtimeInfo != null) {
          runtimeInfo.getController().command(ProgramOptionConstants.FLOWLET_INSTANCES,
                                              ImmutableMap.of("flowlet", flowletId,
                                                              "newInstances", String.valueOf(instances),
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
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private ProgramStatus getProgramStatus(Id.Program id, Type type)
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
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder, @PathParam("app-id") final String appId) {
    try {
      return deployAppStream(request, responder, appId);
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
      return deployAppStream(request, responder, null);
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Deploy failed: {}" + ex.getMessage());
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
   * Returns specification of a runnable - flow.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}")
  public void flowSpecification(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId,
                                @PathParam("flow-id")final String flowId) {
    runnableSpecification(request, responder, appId, Type.FLOW, flowId);
  }

  /**
   * Returns specification of procedure.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}")
  public void procedureSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("procedure-id")final String procId) {
    runnableSpecification(request, responder, appId, Type.PROCEDURE, procId);
  }

  /**
   * Returns specification of mapreduce.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}")
  public void mapreduceSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("mapreduce-id")final String mapreduceId) {
    runnableSpecification(request, responder, appId, Type.MAPREDUCE, mapreduceId);
  }

  /**
   * Returns specification of workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}")
  public void workflowSpecification(HttpRequest request, HttpResponder responder,
                                    @PathParam("app-id") final String appId,
                                    @PathParam("workflow-id")final String workflowId) {
    runnableSpecification(request, responder, appId, Type.WORKFLOW, workflowId);
  }



  private void runnableSpecification(HttpRequest request, HttpResponder responder,
                                     final String appId, Type runnableType,
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

  private BodyConsumer deployAppStream (final HttpRequest request,
                                        final HttpResponder responder, final String appId) throws IOException {
    final String archiveName = request.getHeader(ARCHIVE_NAME_HEADER);
    final String accountId = getAuthenticatedAccountId(request);
    final Location uploadDir = locationFactory.create(archiveDir + "/" + accountId);
    final Location archive = uploadDir.append(archiveName);
    final OutputStream os = archive.getOutputStream();

    if (archiveName == null || archiveName.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, ARCHIVE_NAME_HEADER + " header not present");
    }

    final SessionInfo sessionInfo = new SessionInfo(accountId, appId, archiveName, archive, DeployStatus.UPLOADING);
    sessions.put(accountId, sessionInfo);

    return new BodyConsumer() {
      @Override
      public void chunk(ChannelBuffer request, HttpResponder responder) {
        try {
          request.readBytes(os, request.readableBytes());
        } catch (IOException e) {
          sessionInfo.setStatus(DeployStatus.FAILED);
          LOG.error("Failed to write deploy jar", e);
          responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
      }
      @Override
      public void finished(HttpResponder responder) {
        try {
          os.close();
          sessionInfo.setStatus(DeployStatus.VERIFYING);
          deploy(accountId, appId, archive);
          sessionInfo.setStatus(DeployStatus.DEPLOYED);
          responder.sendString(HttpResponseStatus.OK, "Deploy Complete");
        } catch (Exception e) {
          sessionInfo.setStatus(DeployStatus.FAILED);
          LOG.error("Deploy failure", e);
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        } finally {
          save(sessionInfo.setStatus(sessionInfo.getStatus()), accountId);
          sessions.remove(accountId);
        }
      }
      @Override
      public void handleError(Throwable t) {
        try {
          os.close();
          sessionInfo.setStatus(DeployStatus.FAILED);
          responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getCause().getMessage());
        } catch (IOException e) {
          LOG.error("Error while saving deploy jar.", e);
        } finally {
          save(sessionInfo.setStatus(sessionInfo.getStatus()), accountId);
          sessions.remove(accountId);
        }
      }
    };

  }

  // deploy helper
  private void deploy(final String accountId, final String appId , Location archive) throws Exception {

    try {
      Id.Account id = Id.Account.from(accountId);
      Location archiveLocation = archive;
      Manager<Location, ApplicationWithPrograms> manager = managerFactory.create(new ProgramTerminator() {
        @Override
        public void stop(Id.Account id, Id.Program programId, Type type) throws ExecutionException {
          deleteHandler(programId, type);
        }
      });

      ApplicationWithPrograms applicationWithPrograms =
        manager.deploy(id, appId, archiveLocation).get();
      ApplicationSpecification specification = applicationWithPrograms.getAppSpecLoc().getSpecification();
      setupSchedules(accountId, specification);
    } catch (Throwable e) {
      LOG.warn(e.getMessage(), e);
      throw new Exception(e.getMessage());
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
      DeployStatus status  = dstatus(accountId);
      LOG.trace("Deployment status call at AppFabricHttpHandler , Status: {}", status);
      responder.sendJson(HttpResponseStatus.OK, new Status(status.getCode(), status.getMessage()));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }


  /**
   * Promote an application another reactor.
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
        content = GSON.fromJson(postBody, MAP_STRING_STRING_TYPE);
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
      String token = request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY);

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

      Map<String, String> split = Splitter.on(',').withKeyValueSeparator(":").split(
        configuration.get(Constants.Router.FORWARD, Constants.Router.DEFAULT_FORWARD));

      BiMap<String, String> portForwards = HashBiMap.create(split);

      String url = String.format("%s://%s:%s/v2/apps/%s",
                                 schema, hostname, portForwards.inverse().get(Constants.Service.GATEWAY), appId);

      SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
        .setUrl(url)
        .setRequestTimeoutInMs((int) UPLOAD_TIMEOUT)
        .setHeader("X-Archive-Name", appArchive.getName())
        .setHeader("X-Continuuity-ApiKey", authToken)
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
      String accountId = getAuthenticatedAccountId(request);
      Id.Program id = Id.Program.from(accountId, appId, "");
      AppFabricServiceStatus appStatus = removeApplication(id);
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
   * Deletes all applications in the reactor.
   */
  @DELETE
  @Path("/apps")
  public void deleteAllApps(HttpRequest request, HttpResponder responder) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Account id = Id.Account.from(accountId);
      AppFabricServiceStatus status = removeAll(id);
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
      ProgramStatus status = getProgramStatus(programId, Type.FLOW);
      if (status.getStatus().equals("NOT_FOUND")) {
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

  private AppFabricServiceStatus removeAll(Id.Account identifier) throws Exception {
    List<ApplicationSpecification> allSpecs = new ArrayList<ApplicationSpecification>(
      store.getAllApplications(identifier));

    //Check if any App associated with this account is running
    final Id.Account accId = Id.Account.from(identifier.getId());
    boolean appRunning = checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.getApplication().getAccount().equals(accId);
      }
    }, Type.values());

    if (appRunning) {
      return AppFabricServiceStatus.PROGRAM_STILL_RUNNING;
    }

    //All Apps are STOPPED, delete them
    for (ApplicationSpecification appSpec : allSpecs) {
      Id.Program id = Id.Program.from(identifier.getId(), appSpec.getName() , "");
      removeApplication(id);
    }
    return AppFabricServiceStatus.OK;
  }

  private AppFabricServiceStatus removeApplication(Id.Program identifier) throws Exception {
    Id.Account accountId = Id.Account.from(identifier.getAccountId());
    final Id.Application appId = Id.Application.from(accountId, identifier.getApplicationId());

    //Check if all are stopped.
    boolean appRunning = checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.getApplication().equals(appId);
      }
    }, Type.values());

    if (appRunning) {
      return AppFabricServiceStatus.PROGRAM_STILL_RUNNING;
    }

    ApplicationSpecification spec = store.getApplication(appId);
    if (spec == null) {
      return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
    }

    //Delete the schedules
    for (WorkflowSpecification workflowSpec : spec.getWorkflows().values()) {
      Id.Program workflowProgramId = Id.Program.from(appId, workflowSpec.getName());
      List<String> schedules = scheduler.getScheduleIds(workflowProgramId, Type.WORKFLOW);
      if (!schedules.isEmpty()) {
        scheduler.deleteSchedules(workflowProgramId, Type.WORKFLOW, schedules);
      }
    }

    deleteMetrics(identifier.getAccountId(), identifier.getApplicationId());

    // Delete all streams and queues state of each flow
    // TODO: This should be unified with the DeletedProgramHandlerStage
    for (FlowSpecification flowSpecification : spec.getFlows().values()) {
      Id.Program flowProgramId = Id.Program.from(appId, flowSpecification.getName());

      // Collects stream name to all group ids consuming that stream
      Multimap<String, Long> streamGroups = HashMultimap.create();
      for (FlowletConnection connection : flowSpecification.getConnections()) {
        if (connection.getSourceType() == FlowletConnection.Type.STREAM) {
          long groupId = FlowUtils.generateConsumerGroupId(flowProgramId, connection.getTargetName());
          streamGroups.put(connection.getSourceName(), groupId);
        }
      }
      // Remove all process states and group states for each stream
      String namespace = String.format("%s.%s", flowProgramId.getApplicationId(), flowProgramId.getId());
      for (Map.Entry<String, Collection<Long>> entry : streamGroups.asMap().entrySet()) {
        streamConsumerFactory.dropAll(QueueName.fromStream(entry.getKey()), namespace, entry.getValue());
      }

      queueAdmin.dropAllForFlow(identifier.getApplicationId(), flowSpecification.getName());
    }
    deleteProgramLocations(appId);

    Location appArchive = store.getApplicationArchiveLocation(appId);
    Preconditions.checkNotNull(appArchive, "Could not find the location of application", appId.getId());
    appArchive.delete();
    store.removeApplication(appId);
    return AppFabricServiceStatus.OK;
  }

  private void deleteMetrics(String accountId, String applicationId) throws IOException, OperationException {
    Collection<ApplicationSpecification> applications = Lists.newArrayList();
    if (applicationId == null) {
      applications = this.store.getAllApplications(new Id.Account(accountId));
    } else {
      ApplicationSpecification spec = this.store.getApplication
        (new Id.Application(new Id.Account(accountId), applicationId));
      applications.add(spec);
    }
    Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(Constants.Service.METRICS);
    Discoverable discoverable = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoverables),
                                                              DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS).pick();

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      throw new IOException("Can't find Metrics endpoint");
    }

    for (MetricsScope scope : MetricsScope.values()) {
      for (ApplicationSpecification application : applications) {
        String url = String.format("http://%s:%d%s/metrics/%s/apps/%s",
                                   discoverable.getSocketAddress().getHostName(),
                                   discoverable.getSocketAddress().getPort(),
                                   Constants.Gateway.GATEWAY_VERSION,
                                   scope.name().toLowerCase(),
                                   application.getName());
        sendMetricsDelete(url);
      }
    }

    if (applicationId == null) {
      String url = String.format("http://%s:%d%s/metrics", discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort(), Constants.Gateway.GATEWAY_VERSION);
      sendMetricsDelete(url);
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
    String url = String.format("http://%s:%d%s/metrics/reactor/apps/%s/flows/%s?prefixEntity=process",
                               discoverable.getSocketAddress().getHostName(),
                               discoverable.getSocketAddress().getPort(),
                               Constants.Gateway.GATEWAY_VERSION,
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


  private void sendMetricsDelete(String url) {
    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) METRICS_SERVER_RESPONSE_TIMEOUT)
      .build();

    try {
      client.delete().get(METRICS_SERVER_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error("exception making metrics delete call", e);
      Throwables.propagate(e);
    } finally {
      client.close();
    }
  }

  /**
   * Check if any program that satisfy the given {@link Predicate} is running.
   *
   * @param predicate Get call on each running {@link Id.Program}.
   * @param types Types of program to check
   * returns True if a program is running as defined by the predicate.
   */
  private boolean checkAnyRunning(Predicate<Id.Program> predicate, Type... types) {
    for (Type type : types) {
      for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry :  runtimeService.list(type).entrySet()) {
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

  /**
   * Delete the jar location of the program.
   *
   * @param appId        applicationId.
   * @throws IOException if there are errors with location IO
   */
  private void deleteProgramLocations(Id.Application appId) throws IOException, OperationException {
    ApplicationSpecification specification = store.getApplication(appId);

    Iterable<ProgramSpecification> programSpecs = Iterables.concat(specification.getFlows().values(),
                                                                   specification.getMapReduce().values(),
                                                                   specification.getProcedures().values(),
                                                                   specification.getWorkflows().values());

    for (ProgramSpecification spec : programSpecs) {
      Type type = Type.typeOfSpecification(spec);
      Id.Program programId = Id.Program.from(appId, spec.getName());
      Location location = Programs.programLocation(locationFactory, appFabricDir, programId, type);
      location.delete();
    }

    // Delete webapp
    // TODO: this will go away once webapp gets a spec
    try {
      Id.Program programId = Id.Program.from(appId.getAccountId(), appId.getId(), Type.WEBAPP.name().toLowerCase());
      Location location = Programs.programLocation(locationFactory, appFabricDir, programId, Type.WEBAPP);
      location.delete();
    } catch (FileNotFoundException e) {
      // expected exception when webapp is not present.
    }
  }

  /*
   * Returns DeploymentStatus
   */
  private DeployStatus dstatus(String accountId) {
    if (!sessions.containsKey(accountId)) {
      SessionInfo info = retrieve(accountId);
      if (info == null) {
        return DeployStatus.NOT_FOUND;
      }
      return info.getStatus();
    } else {
      SessionInfo info = sessions.get(accountId);
      return info.getStatus();
    }
  }

  private void deleteHandler(Id.Program programId, Type type)
    throws ExecutionException {
    try {
      switch (type) {
        case FLOW:
          //Stop the flow if it not running
          ProgramRuntimeService.RuntimeInfo flowRunInfo = findRuntimeInfo(programId.getAccountId(),
                                                                          programId.getApplicationId(),
                                                                          programId.getId(),
                                                                          type);
          if (flowRunInfo != null) {
            doStop(flowRunInfo);
          }
          break;
        case PROCEDURE:
          //Stop the procedure if it not running
          ProgramRuntimeService.RuntimeInfo procedureRunInfo = findRuntimeInfo(programId.getAccountId(),
                                                                               programId.getApplicationId(),
                                                                               programId.getId(),
                                                                               type);
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
  private boolean save(SessionInfo info, String accountId) {
    try {
      Gson gson = new GsonBuilder().registerTypeAdapter(Location.class, new LocationCodec(locationFactory)).create();
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
      } else if (type == Type.SERVICE && appSpec.getServices().containsKey(runnableId)) {
        return GSON.toJson(appSpec.getServices().get(id.getId()));
      }
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new Exception(throwable.getMessage());
    }
    return "";
  }


  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(Id.Program identifier, Type type) {
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

  @POST
  @Path("/datasets/{dataset-id}/truncate")
  public void truncate(HttpRequest request, final HttpResponder responder,
                       @PathParam("dataset-id") String tableName) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      try {
        truncateTable(tableName, new OperationContext(accountId));
        responder.sendStatus(HttpResponseStatus.OK);
      } catch (OperationException e) {
        LOG.error("could not truncate dataset {}", tableName, e);
        responder.sendStatus(HttpResponseStatus.CONFLICT);
      }
    } catch (DataSetInstantiationException e) {
      LOG.error("Cannot instantiate table {}", tableName, e);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void truncateTable(String tableName, OperationContext opContext) throws OperationException {
    // NOTE: for now we just try to do the best we can: find all used DataSets of type Table and truncate them. This
    //       should be done better, when we refactor DataSet API (towards separating user API and management parts)
    DataSetSpecification config = datasetInstantiator.getDataSetSpecification(tableName, opContext);
    List<DataSetSpecification> allDataSets = getAllUsedDataSets(config);
    for (DataSetSpecification spec : allDataSets) {
      DataSet ds = datasetInstantiator.getDataSet(spec.getName(), opContext);
      if (ds instanceof Table) {
        try {
          DataSetManager dataSetManager =
            dataSetAccessor.getDataSetManager(OrderedColumnarTable.class, DataSetAccessor.Namespace.USER);
          dataSetManager.truncate(ds.getName());
        } catch (Exception e) {
          throw new OperationException(StatusCode.INTERNAL_ERROR, "failed to truncate table: " + ds.getName(), e);
        }
      }
    }
  }

  private List<DataSetSpecification> getAllUsedDataSets(DataSetSpecification config) {
    List<DataSetSpecification> all = new ArrayList<DataSetSpecification>();
    LinkedList<DataSetSpecification> stack = Lists.newLinkedList();
    stack.add(config);
    while (stack.size() > 0) {
      DataSetSpecification current = stack.removeLast();
      all.add(current);
      Iterable<DataSetSpecification> children = current.getSpecifications();
      if (children != null) {
        for (DataSetSpecification child : children) {
          stack.addLast(child);
        }
      }
    }
    return all;
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
   * Returns a list of flows associated with account.
   */
  @GET
  @Path("/flows")
  public void getAllFlows(HttpRequest request, HttpResponder responder) {
    programList(request, responder, Type.FLOW, null);
  }

  /**
   * Returns a list of procedures associated with account.
   */
  @GET
  @Path("/procedures")
  public void getAllProcedures(HttpRequest request, HttpResponder responder) {
    programList(request, responder, Type.PROCEDURE, null);
  }

  /**
   * Returns a list of map/reduces associated with account.
   */
  @GET
  @Path("/mapreduce")
  public void getAllMapReduce(HttpRequest request, HttpResponder responder) {
    programList(request, responder, Type.MAPREDUCE, null);
  }

  /**
   * Returns a list of workflows associated with account.
   */
  @GET
  @Path("/workflows")
  public void getAllWorkflows(HttpRequest request, HttpResponder responder) {
    programList(request, responder, Type.WORKFLOW, null);
  }

  /**
   * Returns a list of applications associated with account.
   */
  @GET
  @Path("/apps")
  public void getAllApps(HttpRequest request, HttpResponder responder) {
    getAppDetails(request, responder, null);
  }

  /**
   * Returns the info associated with the application.
   */
  @GET
  @Path("/apps/{app-id}")
  public void getAppInfo(HttpRequest request, HttpResponder responder,
                         @PathParam("app-id") final String appId) {
    getAppDetails(request, responder, appId);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/flows")
  public void getFlowsByApp(HttpRequest request, HttpResponder responder,
                            @PathParam("app-id") final String appId) {
    programList(request, responder, Type.FLOW, appId);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/procedures")
  public void getProceduresByApp(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") final String appId) {
    programList(request, responder, Type.PROCEDURE, appId);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce")
  public void getMapreduceByApp(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId) {
    programList(request, responder, Type.MAPREDUCE, appId);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/workflows")
  public void getWorkflowssByApp(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") final String appId) {
    programList(request, responder, Type.WORKFLOW, appId);
  }


  private void getAppDetails(HttpRequest request, HttpResponder responder, String appid) {
    if (appid != null && appid.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "app-id is empty");
      return;
    }

    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Account accId = Id.Account.from(accountId);
      List<Map<String, String>> result = Lists.newArrayList();
      List<ApplicationSpecification> specList;
      if (appid == null) {
        specList = new ArrayList<ApplicationSpecification>(store.getAllApplications(accId));
      } else {
        ApplicationSpecification appSpec = store.getApplication(new Id.Application(accId, appid));
        if (appSpec == null) {
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          return;
        }
        specList = Collections.singletonList(store.getApplication(new Id.Application(accId, appid)));
      }

      for (ApplicationSpecification appSpec : specList) {
        result.add(makeAppRecord(appSpec));
      }

      String json;
      if (appid == null) {
        json = GSON.toJson(result);
      } else {
        json = GSON.toJson(result.get(0));
      }

      responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                              ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception : ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void programList(HttpRequest request, HttpResponder responder, Type type, String appid) {
    if (appid != null && appid.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "app-id is null or empty");
      return;
    }

    try {
      String accountId = getAuthenticatedAccountId(request);
      String list;
      if (appid == null) {
        Id.Account accId = Id.Account.from(accountId);
        list = listPrograms(accId, type);
      } else {
        Id.Application appId = Id.Application.from(accountId, appid);
        list = listProgramsByApp(appId, type);
      }

      if (list.isEmpty()) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendByteArray(HttpResponseStatus.OK, list.getBytes(Charsets.UTF_8),
                                ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private String listProgramsByApp(Id.Application appId, Type type) throws Exception {
    ApplicationSpecification appSpec;
    try {
      appSpec = store.getApplication(appId);
      if (appSpec == null) {
        return "";
      } else {
        return listPrograms(Collections.singletonList(appSpec), type);
      }
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new Exception("Could not retrieve application spec for " + appId.toString() + ", reason: " +
                            throwable.getMessage());
    }
  }

  private String listPrograms(Id.Account accId, Type type) throws Exception {
    try {
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(accId);
      if (appSpecs == null) {
        return "";
      } else {
        return listPrograms(appSpecs, type);
      }
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new Exception("Could not retrieve application spec for " + accId.toString() + ", reason: " +
                            throwable.getMessage());
    }
  }

  private String listPrograms(Collection<ApplicationSpecification> appSpecs, Type type) throws Exception {
    List<Map<String, String>> result = Lists.newArrayList();
    for (ApplicationSpecification appSpec : appSpecs) {
      if (type == Type.FLOW) {
        for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
          result.add(makeProgramRecord(appSpec.getName(), flowSpec, Type.FLOW));
        }
      } else if (type == Type.PROCEDURE) {
        for (ProcedureSpecification procedureSpec : appSpec.getProcedures().values()) {
          result.add(makeProgramRecord(appSpec.getName(), procedureSpec, Type.PROCEDURE));
        }
      } else if (type == Type.MAPREDUCE) {
        for (MapReduceSpecification mrSpec : appSpec.getMapReduce().values()) {
          result.add(makeProgramRecord(appSpec.getName(), mrSpec, Type.MAPREDUCE));
        }
      } else if (type == Type.WORKFLOW) {
        for (WorkflowSpecification wfSpec : appSpec.getWorkflows().values()) {
          result.add(makeProgramRecord(appSpec.getName(), wfSpec, Type.WORKFLOW));
        }
      } else {
        throw new Exception("Unknown program type: " + type.name());
      }
    }
    return GSON.toJson(result);
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

  @PUT
  @Path("/tables/{table-id}")
  public void createTable(HttpRequest request, final HttpResponder responder,
                          @PathParam("table-id") String tableName) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      String spec = (GSON).toJson(new Table(tableName).configure());
      Id.Program programId = Id.Program.from(accountId, "", "");
      createDataSet(programId, spec);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Caught exception ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void createDataSet(Id.Program programId, String spec) throws Exception {
    try {
      DataSetSpecification streamSpec = GSON.fromJson(spec, DataSetSpecification.class);
      store.addDataset(new Id.Account(programId.getAccountId()), streamSpec);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new Exception("Could not create dataset for " + programId.toString() + ", reason: " +
                            throwable.getMessage());
    }
  }

  @PUT
  @Path("/tables/{table-id}/rows/{row-id}")
  public void writeTableRow(HttpRequest request, final HttpResponder responder,
                            @PathParam("table-id") String tableName, @PathParam("row-id") String key) {

    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

      // Fetch table
      Table table = datasetInstantiator.getDataSet(tableName, new OperationContext(accountId));

      // decode row key using the given encoding
      String encoding = getEncoding(queryParams);
      byte [] rowKey = key == null ? null : Util.decodeBinary(key, encoding);

      boolean counter = getCounter(queryParams);

      // Read values from request body
      Map<String, String> valueMap = getValuesMap(request);
      // decode the columns and values into byte arrays
      if (valueMap == null || valueMap.isEmpty()) {
        // this happens when we have no content
        throw new IllegalArgumentException("request body has no columns to write");
      }

      byte[][] cols = new byte[valueMap.size()][];
      byte[][] vals = new byte[valueMap.size()][];
      int i = 0;
      for (Map.Entry<String, String> entry : valueMap.entrySet()) {
        cols[i] = Util.decodeBinary(entry.getKey(), encoding);
        vals[i] = Util.decodeBinary(entry.getValue(), encoding, counter);
        i++;
      }

      // now execute the write
      TransactionContext txContext = new TransactionContext(
        txClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txContext.start();
      table.put(rowKey, cols, vals);
      txContext.finish();
      responder.sendStatus(HttpResponseStatus.OK);

    } catch (DataSetInstantiationException e) {
      LOG.error("Cannot instantiate table {}", tableName, e);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
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
  @Path("/tables/{table-id}/rows/{row-id}")
  public void readTableRow(HttpRequest request, final HttpResponder responder,
                           @PathParam("table-id") String tableName, @PathParam("row-id") String key) {


    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

      // Fetch table
      Table table = datasetInstantiator.getDataSet(tableName, new OperationContext(accountId));

      // decode row key using the given encoding
      String encoding = getEncoding(queryParams);
      byte[] rowKey = key == null ? null : Util.decodeBinary(key, encoding);

      boolean counter = getCounter(queryParams);
      List<String> columns = getColumns(queryParams);
      String start = getOptionalSingletonParam(queryParams, "start");
      String stop = getOptionalSingletonParam(queryParams, "stop");
      int limit = getLimit(queryParams);

      if (columns != null && !columns.isEmpty() && (start != null || stop != null)) {
        throw new IllegalArgumentException("Read can only specify columns or range");
      }

      TransactionContext txContext = new TransactionContext(txClient,
                                                          datasetInstantiator.getInstantiator().getTransactionAware());
      txContext.start();

      Row result;
      if (columns == null || columns.isEmpty()) {
        // column range
        byte[] startCol = start == null ? null : Util.decodeBinary(start, encoding);
        byte[] stopCol = stop == null ? null : Util.decodeBinary(stop, encoding);
        result = table.get(rowKey, startCol, stopCol, limit);
      } else {
        byte[][] cols = new byte[columns.size()][];
        int i = 0;
        for (String column : columns) {
          cols[i++] = Util.decodeBinary(column, encoding);
        }
        result = table.get(rowKey, cols);
      }

      txContext.finish();

      // read successful, now respond with result
      if (result.isEmpty() || result.isEmpty()) {
        responder.sendStatus(HttpResponseStatus.NO_CONTENT);
      } else {
        // result is not empty, now construct a json response
        // first convert the bytes to strings
        Map<String, String> map = Maps.newTreeMap();
        for (Map.Entry<byte[], byte[]> entry : result.getColumns().entrySet()) {
          map.put(Util.encodeBinary(entry.getKey(), encoding), Util.encodeBinary(entry.getValue(), encoding, counter));
        }
        responder.sendJson(HttpResponseStatus.OK, map, STRING_MAP_TYPE);
      }
    } catch (DataSetInstantiationException e) {
      LOG.error("Cannot instantiate table {}", tableName, e);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
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

  @POST
  @Path("/tables/{table-id}/rows/{row-id}/increment")
  public void incrementTableRow(HttpRequest request, final HttpResponder responder,
                                @PathParam("table-id") String tableName, @PathParam("row-id") String key) {
    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

      // Fetch table
      Table table = datasetInstantiator.getDataSet(tableName, new OperationContext(accountId));

      // decode row key using the given encoding
      String encoding = getEncoding(queryParams);
      byte [] rowKey = key == null ? null : Util.decodeBinary(key, encoding);

      // Read values from request body
      Map<String, String> valueMap = getValuesMap(request);
      // decode the columns and values into byte arrays
      if (valueMap == null || valueMap.isEmpty()) {
        // this happens when we have no content
        throw new IllegalArgumentException("request body has no columns to write");
      }

      // decode the columns and values into byte arrays
      byte[][] cols = new byte[valueMap.size()][];
      long[] vals = new long[valueMap.size()];
      int i = 0;
      for (Map.Entry<String, String> entry : valueMap.entrySet()) {
        cols[i] = Util.decodeBinary(entry.getKey(), encoding);
        vals[i] = Long.parseLong(entry.getValue());
        i++;
      }
      // now execute the increment
      TransactionContext txContext = new TransactionContext(
        txClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txContext.start();
      Row result = table.increment(rowKey, cols, vals);
      txContext.finish();

      // first convert the bytes to strings
      Map<String, Long> map = Maps.newTreeMap();
      for (Map.Entry<byte[], byte[]> entry : result.getColumns().entrySet()) {
        map.put(Util.encodeBinary(entry.getKey(), encoding), Bytes.toLong(entry.getValue()));
      }
      // now write a json string representing the map
      responder.sendJson(HttpResponseStatus.OK, map, LONG_MAP_TYPE);

    } catch (DataSetInstantiationException e) {
      LOG.error("Cannot instantiate table {}", tableName, e);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @DELETE
  @Path("/tables/{table-id}/rows/{row-id}")
  public void deleteTableRow(HttpRequest request, final HttpResponder responder,
                             @PathParam("table-id") String tableName, @PathParam("row-id") String key) {
    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

      // Fetch table
      Table table = datasetInstantiator.getDataSet(tableName, new OperationContext(accountId));

      // decode row key using the given encoding
      String encoding = getEncoding(queryParams);
      byte [] rowKey = key == null ? null : Util.decodeBinary(key, encoding);

      List<String> columns = getColumns(queryParams);
      byte[][] cols = null;
      if (columns != null && !columns.isEmpty()) {
        cols = new byte[columns.size()][];
        int i = 0;
        for (String column : columns) {
          cols[i++] = Util.decodeBinary(column, encoding);
        }
      }

      // now execute the delete operation
      TransactionContext txContext = new TransactionContext(
        txClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txContext.start();
      table.delete(rowKey, cols);
      txContext.finish();
      responder.sendStatus(HttpResponseStatus.OK);

    } catch (DataSetInstantiationException e) {
      LOG.error("Cannot instantiate table {}", tableName, e);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private Map<String, String> getValuesMap(HttpRequest request) {
    // parse JSON string in the body
    try {
      InputStreamReader reader = new InputStreamReader(
        new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
      return (GSON).fromJson(reader, STRING_MAP_TYPE);
    } catch (Exception e) {
      // failed to parse json, that is a bad request
      throw new IllegalArgumentException("Failed to parse body as json");
    }
  }

  private boolean getCounter(Map<String, List<String>> queryParams) {
    // optional parameter counter - if true, column values are interpreted (and returned) as long numbers
    boolean counter = false;
    List<String> counterParams = queryParams.get("counter");
    if (counterParams != null) {
      // make sure there is at most one
      if (counterParams.size() > 1) {
        throw new IllegalArgumentException("More than one 'counter' parameter");
      }
      // make sure that if there is one, it is supported
      if (!counterParams.isEmpty()) {
        String param = counterParams.get(0);
        counter = "1".equals(param) || "true".equals(param);
      }
    }

    return counter;
  }


  private String getEncoding(Map<String, List<String>> queryParams) {
    String encoding = null;
    List<String> encodingParams = queryParams.get("encoding");

    if (encodingParams != null) {
      // make sure there is at most one
      if (encodingParams.size() > 1) {
        throw new IllegalArgumentException("More than one 'encoding' parameter");
      }

      // make sure that if there is one, it is supported
      if (!encodingParams.isEmpty()) {
        encoding = encodingParams.get(0);
        if (!Util.supportedEncoding(encoding)) {
          throw  new IllegalArgumentException("Unsupported 'encoding' parameter");
        }
      }
    }

    return encoding;
  }

  private List<String> getColumns(Map<String, List<String>> queryParams) {
    // for read and delete operations, optional parameter is columns
    List<String> columns = null;
    List<String> columnParams = queryParams.get("columns");
    if (columnParams != null && columnParams.size() > 0) {
      columns = Lists.newLinkedList();
      for (String param : columnParams) {
        Collections.addAll(columns, param.split(","));
      }
    }

    return columns;
  }

  private String getOptionalSingletonParam(Map<String, List<String>> queryParams, String name) {
    List<String> params = queryParams.get(name);
    if (params != null && params.size() > 1) {
      throw new IllegalArgumentException(String.format("More than one '%s' parameter", name));
    }
    return  (params == null || params.isEmpty()) ? null : params.get(0);
  }

  private int getLimit(Map<String, List<String>> queryParams) {
    List<String> limitParams = queryParams.get("limit");
    if (limitParams != null && limitParams.size() > 1) {
      throw new IllegalArgumentException("More than one 'limit' parameter");
    }

    try {
      return (limitParams == null || limitParams.isEmpty()) ? -1 : Integer.parseInt(limitParams.get(0));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("'limit' parameter is not an integer");
    }
  }

  private String getDataEntity(Id.Program programId, Data type, String name) throws Exception {
    try {
      Id.Account account = new Id.Account(programId.getAccountId());
      if (type == Data.DATASET) {
        DataSetSpecification spec = store.getDataSet(account, name);
        String typeName = null;
        if (spec != null) {
          typeName = spec.getType();
        } else {
          // trying to see if that is Dataset V2
          DatasetSpecification dsSpec = getDatasetSpec(name);
          if (dsSpec != null) {
            typeName = dsSpec.getType();
          }
        }
        return GSON.toJson(makeDataSetRecord(name, typeName, spec));
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
        Collection<DataSetSpecification> specs = store.getAllDataSets(new Id.Account(programId.getAccountId()));
        List<Map<String, String>> result = Lists.newArrayListWithExpectedSize(specs.size());
        for (DataSetSpecification spec : specs) {
          result.add(makeDataSetRecord(spec.getName(), spec.getType(), null));
        }
        // also add datasets2 instances
        Collection<DatasetSpecification> instances = dsFramework.getInstances();
        for (DatasetSpecification instance : instances) {
          result.add(makeDataSetRecord(instance.getName(), instance.getType(), null));
        }
        return GSON.toJson(result);
      } else if (type == Data.STREAM) {
        Collection<StreamSpecification> specs = store.getAllStreams(new Id.Account(programId.getAccountId()));
        List<Map<String, String>> result = Lists.newArrayListWithExpectedSize(specs.size());
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
        List<Map<String, String>> result = Lists.newArrayListWithExpectedSize(dataSetsUsed.size());
        for (String dsName : dataSetsUsed) {
          DataSetSpecification spec = appSpec.getDataSets().get(dsName);
          String typeName = null;
          if (spec == null) {
            spec = store.getDataSet(account, dsName);
          }

          if (spec != null) {
            // Dataset V1
            typeName = spec.getType();
          } else {
            // trying to see if that is Dataset V2
            DatasetSpecification dsSpec = getDatasetSpec(dsName);
            if (dsSpec != null) {
              typeName = dsSpec.getType();
            }
          }
          result.add(makeDataSetRecord(dsName, typeName, null));
        }
        return GSON.toJson(result);
      }
      if (type == Data.STREAM) {
        Set<String> streamsUsed = streamsUsedBy(appSpec);
        List<Map<String, String>> result = Lists.newArrayListWithExpectedSize(streamsUsed.size());
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
    result.addAll(appSpec.getDataSets().keySet());
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
    programListByDataAccess(request, responder, Type.FLOW, Data.STREAM, streamId);
  }

  /**
   * Returns all flows associated with a dataset.
   */
  @GET
  @Path("/datasets/{dataset-id}/flows")
  public void getFlowsByDataset(HttpRequest request, HttpResponder responder,
                                @PathParam("dataset-id") final String datasetId) {
    programListByDataAccess(request, responder, Type.FLOW, Data.DATASET, datasetId);
  }

  private void programListByDataAccess(HttpRequest request, HttpResponder responder,
                                       Type type, Data data, String name) {
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

  private String listProgramsByDataAccess(Id.Program programId, Type type, Data data, String name) throws Exception {
    try {
      List<Map<String, String>> result = Lists.newArrayList();
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(
        new Id.Account(programId.getAccountId()));
      if (appSpecs != null) {
        for (ApplicationSpecification appSpec : appSpecs) {
          if (type == Type.FLOW) {
            for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
              if ((data == Data.DATASET && usesDataSet(flowSpec, name))
                || (data == Data.STREAM && usesStream(flowSpec, name))) {
                result.add(makeProgramRecord(appSpec.getName(), flowSpec, Type.FLOW));
              }
            }
          } else if (type == Type.PROCEDURE) {
            for (ProcedureSpecification procedureSpec : appSpec.getProcedures().values()) {
              if (data == Data.DATASET && procedureSpec.getDataSets().contains(name)) {
                result.add(makeProgramRecord(appSpec.getName(), procedureSpec, Type.PROCEDURE));
              }
            }
          } else if (type == Type.MAPREDUCE) {
            for (MapReduceSpecification mrSpec : appSpec.getMapReduce().values()) {
              if (data == Data.DATASET && mrSpec.getDataSets().contains(name)) {
                result.add(makeProgramRecord(appSpec.getName(), mrSpec, Type.MAPREDUCE));
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

  private static Map<String, String> makeAppRecord(ApplicationSpecification appSpec) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "App");
    builder.put("id", appSpec.getName());
    builder.put("name", appSpec.getName());
    if (appSpec.getDescription() != null) {
      builder.put("description", appSpec.getDescription());
    }
    return builder.build();
  }

  private static Map<String, String> makeProgramRecord (String appId, ProgramSpecification spec, Type type) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", type.prettyName());
    builder.put("app", appId);
    builder.put("id", spec.getName());
    builder.put("name", spec.getName());
    if (spec.getDescription() != null) {
      builder.put("description", spec.getDescription());
    }
    return builder.build();
  }

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

  /**
   * DO NOT DOCUMENT THIS API.
   */
  @POST
  @Path("/unrecoverable/reset")
  public void resetReactor(HttpRequest request, HttpResponder responder) {

    try {
      if (!configuration.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET,
                                    Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET)) {
        responder.sendStatus(HttpResponseStatus.FORBIDDEN);
        return;
      }
      String account = getAuthenticatedAccountId(request);
      final Id.Account accountId = Id.Account.from(account);

      // Check if any program is still running
      boolean appRunning = checkAnyRunning(new Predicate<Id.Program>() {
        @Override
        public boolean apply(Id.Program programId) {
          return programId.getAccountId().equals(accountId.getId());
        }
      }, Type.values());

      if (appRunning) {
        throw new Exception("App Still Running");
      }

      // NOTE: deleting new datasets stuff first because old datasets system deletes all blindly by prefix
      //       which may damage metadata
      dsFramework.deleteAllInstances();
      dsFramework.deleteAllModules();

      deleteMetrics(account, null);
      // delete all meta data
      store.removeAll(accountId);
      // delete queues and streams data
      queueAdmin.dropAll();
      streamAdmin.dropAll();

      LOG.info("Deleting all data for account '" + account + "'.");
      dataSetAccessor.dropAll(DataSetAccessor.Namespace.USER);
      // Can't truncate metric entity tables because they are cached in memory by anybody who touches the metric
      // tables, and truncating will cause metrics to get incorrectly mapped to other random metrics.
      Set<String> datasetsToKeep = Sets.newHashSet();
      for (MetricsScope scope : MetricsScope.values()) {
        datasetsToKeep.add(scope.name().toLowerCase() + "." +
                             configuration.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                               MetricsConstants.DEFAULT_ENTITY_TABLE_NAME));
      }

      // Don't truncate log table too - we would like to retain logs across resets.
      datasetsToKeep.add(LoggingConfiguration.LOG_META_DATA_TABLE);
      // Don't remove datasets
      datasetsToKeep.add(DatasetMetaTableUtil.META_TABLE_NAME);
      datasetsToKeep.add(DatasetMetaTableUtil.INSTANCE_TABLE_NAME);

      // NOTE: there could be services running at the moment that rely on the system datasets to be available.
      dataSetAccessor.truncateAllExceptBlacklist(DataSetAccessor.Namespace.SYSTEM, datasetsToKeep);

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
}
