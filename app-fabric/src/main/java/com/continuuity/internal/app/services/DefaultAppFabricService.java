/*
* Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
*/

package com.continuuity.internal.app.services;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Type;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.ActiveFlow;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeployStatus;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.FlowDescriptor;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.FlowRunRecord;
import com.continuuity.app.services.FlowStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.app.services.RunIdentifier;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;
import com.continuuity.internal.app.deploy.SessionInfo;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.internal.app.services.legacy.ConnectionDefinitionImpl;
import com.continuuity.internal.app.services.legacy.FlowDefinitionImpl;
import com.continuuity.internal.app.services.legacy.FlowStreamDefinitionImpl;
import com.continuuity.internal.app.services.legacy.FlowletDefinitionImpl;
import com.continuuity.internal.app.services.legacy.FlowletStreamDefinitionImpl;
import com.continuuity.internal.app.services.legacy.FlowletType;
import com.continuuity.internal.app.services.legacy.MetaDefinitionImpl;
import com.continuuity.internal.app.services.legacy.QueryDefinitionImpl;
import com.continuuity.internal.app.services.legacy.StreamNamerImpl;
import com.continuuity.internal.filesystem.LocationCodec;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Application;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.ning.http.client.Body;
import com.ning.http.client.BodyGenerator;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This is a concrete implementation of AppFabric thrift Interface.
 */
public class DefaultAppFabricService implements AppFabricService.Iface {
  /**
   * Log handler.
   */
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAppFabricService.class);

  /**
   * Maintains a mapping of transient session state. The state is stored in memory,
   * in case of failure, all the current running sessions will be terminated. As
   * per the current implementation only connection per account is allowed to upload.
   */
  private final Map<String, SessionInfo> sessions = Maps.newConcurrentMap();

  /**
   * Metadata Service instance is used to interact with the metadata store.
   */
  private final MetadataService mds;

  /**
   * Instance of operation executor needed by MetadataService.
   */
  private final OperationExecutor opex;

  /**
   * Configuration object passed from higher up.
   */
  private final CConfiguration configuration;

  /**
   * Factory for handling the location - can do both in either Distributed or Local mode.
   */
  private final LocationFactory locationFactory;

  /**
   * DeploymentManager responsible for running pipeline.
   */
  private final ManagerFactory managerFactory;

  /**
   * Authorization Factory used to create handler used for authroizing use of endpoints.
   */
  private final AuthorizationFactory authFactory;

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  /**
   * Discovery service client to discover other services.
   */
  private final DiscoveryServiceClient discoveryServiceClient;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  /**
   * The directory where the uploaded files would be placed.
   */
  private final String archiveDir;

  /**
   * Timeout to upload to remote app fabric.
   */
  private static final long UPLOAD_TIMEOUT = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

  /**
   * Timeout to get response from metrics system.
   */
  private static final long METRICS_SERVER_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  @Inject
  public DefaultAppFabricService(CConfiguration configuration, OperationExecutor opex,
                                 LocationFactory locationFactory, ManagerFactory managerFactory,
                                 AuthorizationFactory authFactory, StoreFactory storeFactory,
                                 ProgramRuntimeService runtimeService, DiscoveryServiceClient discoveryServiceClient) {
    this.opex = opex;
    this.locationFactory = locationFactory;
    this.configuration = configuration;
    this.managerFactory = managerFactory;
    this.authFactory = authFactory;
    this.runtimeService = runtimeService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.store = storeFactory.create();
    this.archiveDir = configuration.get(Constants.CFG_APP_FABRIC_OUTPUT_DIR, System.getProperty("java.io.tmpdir"))
                                          + "/archive";
    this.mds = new MetadataService(opex);

    // Note: This is hacky to start service like this.
    if (this.runtimeService.state() != Service.State.RUNNING) {
      this.runtimeService.startAndWait();
    }
  }

  private Type entityTypeToType(FlowIdentifier identifier) {
    switch (identifier.getType()) {
      case FLOW:
        return Type.FLOW;
      case QUERY:
        return Type.PROCEDURE;
      case MAPREDUCE:
        return Type.MAPREDUCE;
    }
    // Never hit
    throw new IllegalArgumentException("Type not supported: " + identifier.getType());
  }

  private EntityType typeToEntityType(Type type) {
    switch (type) {
      case FLOW:
        return EntityType.FLOW;
      case PROCEDURE:
        return EntityType.QUERY;
      case MAPREDUCE:
        return EntityType.MAPREDUCE;
    }
    // Never hit
    throw new IllegalArgumentException("Type not suppported: " + type);
  }

  /**
   * Starts a Program
   *
   * @param token
   * @param descriptor
   */
  @Override
  public synchronized RunIdentifier start(AuthToken token, FlowDescriptor descriptor)
    throws AppFabricServiceException, TException {

    try {
      FlowIdentifier id = descriptor.getIdentifier();
      ProgramRuntimeService.RuntimeInfo existingRuntimeInfo = findRuntimeInfo(id);
      Preconditions.checkArgument(existingRuntimeInfo == null, UserMessages.getMessage(UserErrors.ALREADY_RUNNING));
      Id.Program programId = Id.Program.from(id.getAccountId(), id.getApplicationId(), id.getFlowId());

      Program program;
      try {
        program = store.loadProgram(programId, entityTypeToType(id));
      } catch (Throwable th) {
        // TODO: hack: in that case the flow is mapreduce ;)
        id.setType(EntityType.MAPREDUCE);
        program = store.loadProgram(programId, entityTypeToType(id));
      }

      BasicArguments userArguments = new BasicArguments();
      if (descriptor.isSetArguments()) {
        userArguments = new BasicArguments(descriptor.getArguments());
      }

      ProgramRuntimeService.RuntimeInfo runtimeInfo =
        runtimeService.run(program, new SimpleProgramOptions(id.getFlowId(),
                                                             new BasicArguments(),
                                                             userArguments));
      store.setStart(programId, runtimeInfo.getController().getRunId().getId(),
                     TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
      return new RunIdentifier(runtimeInfo.getController().getRunId().toString());

    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /**
   * Checks the status of a Program
   *
   * @param token
   * @param id
   */
  @Override
  public synchronized FlowStatus status(AuthToken token, FlowIdentifier id)
    throws AppFabricServiceException, TException {

    try {
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(id);
      // TODO: hack: this might be a mapreduce job ;)
      if (runtimeInfo == null && id.getType() == EntityType.FLOW) {
        id.setType(EntityType.MAPREDUCE);
        runtimeInfo = findRuntimeInfo(id);
      }

      int version = 1;  // Note, how to get version?
      if (runtimeInfo == null) {
        return new FlowStatus(id.getApplicationId(), id.getFlowId(),
                              version, null, ProgramController.State.STOPPED.toString());
      }

      Id.Program programId = runtimeInfo.getProgramId();
      RunIdentifier runId = new RunIdentifier(runtimeInfo.getController().getRunId().getId());

      // NOTE: This was a temporary hack done to map the status to something that is
      // UI friendly. Internal states of program controller are reasonable and hence
      // no point in changing them.
      String status = controllerStateToString(runtimeInfo.getController().getState());
      return new FlowStatus(programId.getApplicationId(), programId.getId(), version, runId, status);
    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
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

  /**
   * Stops a Program
   *
   * @param token
   * @param identifier
   */
  @Override
  public synchronized RunIdentifier stop(AuthToken token, FlowIdentifier identifier)
    throws AppFabricServiceException, TException {
    try {
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(identifier);
      Preconditions.checkNotNull(runtimeInfo, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
              identifier.getApplicationId(), identifier.getFlowId());
      ProgramController controller = runtimeInfo.getController();
      RunId runId = controller.getRunId();
      controller.stop().get();
      store.setStop(runtimeInfo.getProgramId(), runId.getId(),
                    TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                    runtimeInfo.getController().getState().toString());
      return new RunIdentifier(runId.getId());
    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /**
   * Set number of instance of a flowlet.
   *
   * @param token
   * @param identifier
   * @param flowletId
   * @param instances
   */
  @Override
  public void setInstances(AuthToken token, FlowIdentifier identifier, String flowletId, short instances)
    throws AppFabricServiceException, TException {
    // storing the info about instances count after increasing the count of running flowlets: even if it fails, we
    // can at least set instances count for this session
    try {
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(identifier);
      Preconditions.checkNotNull(runtimeInfo, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
              identifier.getApplicationId(), identifier.getFlowId());
      store.setFlowletInstances(Id.Program.from(identifier.getAccountId(), identifier.getApplicationId(),
                                                identifier.getFlowId()), flowletId, instances);
      runtimeInfo.getController().command("instances", ImmutableMap.of(flowletId, (int) instances)).get();
    } catch (Throwable throwable) {
      LOG.warn("Exception when setting instances for {}.{} to {}. {}",
               identifier.getFlowId(), flowletId, instances, throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /**
   * Returns the state of flows within a given account id.
   *
   * @param accountId
   */
  @Override
  public List<ActiveFlow> getFlows(String accountId) throws AppFabricServiceException, TException {

    try {
      Table<Type, Id.Program, List<RunRecord>> histories = store.getAllRunHistory(Id.Account.from(accountId));
      List<ActiveFlow> result = Lists.newLinkedList();
      for (Table.Cell<Type, Id.Program, List<RunRecord>> cell : histories.cellSet()) {
        Id.Program programId = cell.getColumnKey();
        for (RunRecord runRecord : cell.getValue()) {
          ActiveFlow activeFlow = new ActiveFlow(programId.getApplicationId(),
                                                 programId.getId(),
                                                 typeToEntityType(cell.getRowKey()),
                                                 runRecord.getStopTs(),
                                                 runRecord.getStartTs(),
                                                 null,        // TODO
                                                 0            // TODO
                                                 );
            result.add(activeFlow);
        }
      }
      return result;

    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException("Exception while retrieving the run history. " + throwable.getMessage());
    }
  }

  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(FlowIdentifier identifier) {
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = null;
    switch (identifier.getType()) {
      case FLOW:
        runtimeInfos = runtimeService.list(Type.FLOW).values();
        break;
      case QUERY:
        runtimeInfos = runtimeService.list(Type.PROCEDURE).values();
        break;
      case MAPREDUCE:
        runtimeInfos = runtimeService.list(Type.MAPREDUCE).values();
        break;
    }
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

  /**
   * Returns definition of a flow.
   *
   * @param id
   */
  @Override
  public String getFlowDefinition(FlowIdentifier id)
    throws AppFabricServiceException, TException {
    try {
      if (id.getType() == EntityType.FLOW) {
        FlowDefinitionImpl flowDef = getFlowDef(id);
        return new Gson().toJson(flowDef);
      } else if (id.getType() == EntityType.QUERY) {
        QueryDefinitionImpl queryDef = getQueryDefn(id);
        return new Gson().toJson(queryDef);
      }
    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException(throwable.getMessage());
    }
    return null;
  }

  private QueryDefinitionImpl getQueryDefn(final FlowIdentifier identifier)
    throws AppFabricServiceException {
    ApplicationSpecification appSpec = null;
    try {
      appSpec = store.getApplication(new Id.Application(new Id.Account(identifier.getAccountId()),
                                                        identifier.getApplicationId()));
    } catch (OperationException e) {
      LOG.warn(StackTraceUtil.toStringStackTrace(e));
      throw  new AppFabricServiceException("Could not retrieve application spec for " +
                                           identifier.toString() + ", reason: " + e.getMessage());
    }

    ProcedureSpecification procedureSpec = appSpec.getProcedures().get(identifier.getFlowId());
    QueryDefinitionImpl queryDef = new QueryDefinitionImpl();

    // TODO: fill values (incl. list of datasets ) once they are added to ProcedureSpecification
    queryDef.setServiceName(procedureSpec.getName());
    return queryDef;
  }

  private FlowDefinitionImpl getFlowDef(final FlowIdentifier id)
    throws AppFabricServiceException, UnsupportedTypeException {
    ApplicationSpecification appSpec = null;
    try {
      appSpec = store.getApplication(new Id.Application(new Id.Account(id.getAccountId()),
                                                        id.getApplicationId()));
    } catch (OperationException e) {
      LOG.warn(StackTraceUtil.toStringStackTrace(e));
      throw  new AppFabricServiceException("Could not retrieve application spec for " + id.toString() + "." +
                                             e.getMessage());
    }

    Preconditions.checkArgument(appSpec != null, "Not application specification found.");
    FlowSpecification flowSpec = appSpec.getFlows().get(id.getFlowId());
    if (flowSpec == null) {
      // TODO: this is hack for a mapreduce job ;)
      return getFlowDef4MapReduce(id, appSpec.getMapReduces().get(id.getFlowId()));
    } else {
      return getFlowDef4Flow(id, flowSpec);
    }
  }

  private FlowDefinitionImpl getFlowDef4Flow(FlowIdentifier id, FlowSpecification flowSpec) {
    FlowDefinitionImpl flowDef = new FlowDefinitionImpl();
    MetaDefinitionImpl metaDefinition = new MetaDefinitionImpl();
    metaDefinition.setApp(id.getApplicationId());
    metaDefinition.setName(flowSpec.getName());
    flowDef.setMeta(metaDefinition);
    fillFlowletsAndDataSets(flowSpec, flowDef);
    fillConnectionsAndStreams(id, flowSpec, flowDef);
    return flowDef;
  }

  // we re-use the ability of existing UI to display flows as a way to display and run mapreduce jobs (for now)
  private FlowDefinitionImpl getFlowDef4MapReduce(FlowIdentifier id, MapReduceSpecification spec)
    throws UnsupportedTypeException {
    FlowSpecification flowSpec = FlowSpecification.Builder.with()
      .setName(spec.getName())
      .setDescription(spec.getDescription())
      .withFlowlets()
      .add("Mapper", new AbstractFlowlet() {
        public void process(StreamEvent event) {}
        private OutputEmitter<String> output;
      })
      .add("Reducer", new AbstractFlowlet() {
        @ProcessInput
        public void process(String item) {}
      })
      .connect()
      .fromStream("Input").to("Mapper")
      .from("Mapper").to("Reducer")
      .build();

    for (FlowletDefinition def : flowSpec.getFlowlets().values()) {
      def.generateSchema(new ReflectionSchemaGenerator());
    }

    return getFlowDef4Flow(id, flowSpec);
  }

  private void fillConnectionsAndStreams(final FlowIdentifier id, final FlowSpecification spec,
                                         final FlowDefinitionImpl def) {
    List<ConnectionDefinitionImpl> connections = new ArrayList<ConnectionDefinitionImpl>();
    // we gather streams across all connections, hence we need to eliminate duplicate streams hence using map
    Map<String, FlowStreamDefinitionImpl> flowStreams = new HashMap<String, FlowStreamDefinitionImpl>();

    QueueSpecificationGenerator generator =
      new SimpleQueueSpecificationGenerator(new Id.Account(id.getAccountId()));
    Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> queues =  generator.create(spec);

    for (Table.Cell<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> conSet : queues.cellSet()) {
      for (QueueSpecification queueSpec : conSet.getValue()) {
        String srcName = conSet.getRowKey().getName();
        String destName = conSet.getColumnKey();
        FlowletStreamDefinitionImpl from;
        if (!spec.getFlowlets().containsKey(srcName)) {
          from =  new FlowletStreamDefinitionImpl(srcName);
          flowStreams.put(srcName, new FlowStreamDefinitionImpl(srcName, null));
        } else {
          from =  new FlowletStreamDefinitionImpl(srcName, queueSpec.getQueueName().getSimpleName());
        }
        FlowletStreamDefinitionImpl to = new FlowletStreamDefinitionImpl(destName,
                                                                         queueSpec.getQueueName().getSimpleName());
        connections.add(new ConnectionDefinitionImpl(from, to));
      }
    }
    def.setConnections(connections);
    def.setFlowStreams(new ArrayList<FlowStreamDefinitionImpl>(flowStreams.values()));

    new StreamNamerImpl().name(id.getAccountId(), def);
  }

  private void fillFlowletsAndDataSets(final FlowSpecification flowSpec, final FlowDefinitionImpl flowDef) {
    Set<String> datasets = new HashSet<String>();
    List<FlowletDefinitionImpl> flowlets = new ArrayList<FlowletDefinitionImpl>();

    for (FlowletDefinition flowletSpec : flowSpec.getFlowlets().values()) {
      datasets.addAll(flowletSpec.getDatasets());

      FlowletDefinitionImpl flowletDef = new FlowletDefinitionImpl();
      flowletDef.setClassName(flowletSpec.getFlowletSpec().getClassName());
      if (flowletSpec.getInputs().isEmpty()) {
        flowletDef.setFlowletType(FlowletType.SOURCE);
      } else if (flowletSpec.getOutputs().isEmpty()) {
        flowletDef.setFlowletType(FlowletType.SINK);
      } else {
        flowletDef.setFlowletType(FlowletType.COMPUTE);
      }

      flowletDef.setInstances(flowletSpec.getInstances());
      flowletDef.setName(flowletSpec.getFlowletSpec().getName());

      flowlets.add(flowletDef);
    }

    flowDef.setFlowlets(flowlets);
    flowDef.setDatasets(datasets);
  }

  /**
   * Returns run information for a given flow id.
   *
   * @param id of the program.
   */
  @Override
  public List<FlowRunRecord> getFlowHistory(FlowIdentifier id) throws AppFabricServiceException, TException {
    List<RunRecord> log;
    try {
      Id.Program programId = Id.Program.from(id.getAccountId(), id.getApplicationId(), id.getFlowId());
      try {
        log = store.getRunHistory(programId);
      } catch (OperationException e) {
        throw new AppFabricServiceException(String.format(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND),
                                                          id.toString(), e.getMessage()));
      }
      List<FlowRunRecord> history = new ArrayList<FlowRunRecord>();
      for (RunRecord runRecord : log) {
        history.add(new FlowRunRecord(runRecord.getPid(), runRecord.getStartTs(),
                                      runRecord.getStopTs(), runRecord.getEndStatus())
        );
      }
      return history;
    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /**
   * Returns run information for a given flow id.
   *
   * @param id
   */
  @Override
  public void stopAll(String id) throws AppFabricServiceException, TException {
    // Note: Is id application id?
    try {
      List<ListenableFuture<?>> futures = Lists.newLinkedList();
      for (Type type : Type.values()) {
        for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry : runtimeService.list(type).entrySet()) {
          ProgramRuntimeService.RuntimeInfo runtimeInfo = entry.getValue();
          if (runtimeInfo.getProgramId().getApplicationId().equals(id)) {
            futures.add(runtimeInfo.getController().stop());
          }
        }
      }
      if (!futures.isEmpty()) {
        try {
          Futures.successfulAsList(futures).get();
        } catch (Exception e) {
          LOG.warn(StackTraceUtil.toStringStackTrace(e));
          throw new AppFabricServiceException(e.getMessage());
        }
      }
    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /**
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
   * @param info ResourceInfo
   * @return ResourceIdentifier instance containing the resource id and
   * resource version.
   */
  @Override
  public ResourceIdentifier init(AuthToken token, ResourceInfo info) throws AppFabricServiceException {
    LOG.debug("Init deploying application " + info.toString());
    ResourceIdentifier identifier = new ResourceIdentifier(info.getAccountId(), "appId", "resourceId", 1);

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
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /**
   * Writes chunk of data transmitted from the client. Along with data, there is the session id also
   * being returned.
   *
   * @param resource identifier.
   * @param chunk binary data of the resource transmitted from the client.
   * @throws AppFabricServiceException
   */
  @Override
  public void chunk(AuthToken token, ResourceIdentifier resource, ByteBuffer chunk) throws AppFabricServiceException {
    LOG.debug("Receiving chunk of application " + resource.toString());
    if (!sessions.containsKey(resource.getAccountId())) {
      throw new AppFabricServiceException("A session id has not been created for upload. Please call #init");
    }

    SessionInfo info = sessions.get(resource.getAccountId()).setStatus(DeployStatus.UPLOADING);
    try {
      OutputStream stream = info.getOutputStream();
      // Read the chunk from ByteBuffer and write it to file
      if (chunk != null) {
        byte[] buffer = new byte[chunk.remaining()];
        chunk.get(buffer);
        stream.write(buffer);
      } else {
        sessions.remove(resource.getAccountId());
        throw new AppFabricServiceException("Invalid chunk received.");
      }
    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      sessions.remove(resource.getAccountId());
      throw new AppFabricServiceException("Failed to write archive chunk.");
    }
  }

  /**
   * Finalizes the deployment of a archive. Once upload is completed, it will
   * start the pipeline responsible for verification and registration of archive resources.
   *
   * @param resource identifier to be finalized.
   */
  @Override
  public void deploy(AuthToken token, final ResourceIdentifier resource) throws AppFabricServiceException {
    LOG.debug("Finishing deploy of application " + resource.toString());
    if (!sessions.containsKey(resource.getAccountId())) {
      throw new AppFabricServiceException("No information about archive being uploaded is available.");
    }

    final SessionInfo sessionInfo = sessions.get(resource.getAccountId());
    try {
      Id.Account id = Id.Account.from(resource.getAccountId());
      Location archiveLocation = sessionInfo.getArchiveLocation();
      sessionInfo.getOutputStream().close();
      sessionInfo.setStatus(DeployStatus.VERIFYING);
      Manager<Location, ApplicationWithPrograms> manager = managerFactory.create();
      ListenableFuture<ApplicationWithPrograms> future = manager.deploy(id, archiveLocation);
      Futures.addCallback(future, new FutureCallback<ApplicationWithPrograms>() {
        @Override
        public void onSuccess(ApplicationWithPrograms result) {
          save(sessionInfo.setStatus(DeployStatus.DEPLOYED));
          sessions.remove(resource.getAccountId());
        }

        @Override
        public void onFailure(Throwable t) {
          LOG.warn(StackTraceUtil.toStringStackTrace(t));

          DeployStatus status = DeployStatus.FAILED;
          Throwable cause = t.getCause();

          if (cause instanceof ClassNotFoundException) {
            status.setMessage(String.format(UserMessages.getMessage(UserErrors.CLASS_NOT_FOUND), t.getMessage()));

          } else if (cause instanceof IllegalArgumentException) {
            status.setMessage(String.format(UserMessages.getMessage(UserErrors.SPECIFICATION_ERROR), t.getMessage()));
          } else {
            status.setMessage(t.getMessage());
          }

          save(sessionInfo.setStatus(status));
          sessions.remove(resource.getAccountId());
        }
      });

    } catch (Throwable e) {
      LOG.warn(StackTraceUtil.toStringStackTrace(e));

      DeployStatus status = DeployStatus.FAILED;
      status.setMessage(e.getMessage());
      save(sessionInfo.setStatus(status));

      sessions.remove(resource.getAccountId());
      throw new AppFabricServiceException(e.getMessage());

    }
  }

  /**
   * Returns status of deployment of archive.
   *
   * @param resource identifier
   * @return status of resource processing.
   * @throws AppFabricServiceException
   */
  @Override
  public DeploymentStatus dstatus(AuthToken token, ResourceIdentifier resource) throws AppFabricServiceException {
    try {
      if (!sessions.containsKey(resource.getAccountId())) {
        SessionInfo info = retrieve(resource.getAccountId());
        DeploymentStatus status = new DeploymentStatus(info.getStatus().getCode(),
                                                       info.getStatus().getMessage(), null);
        return status;
      } else {
        SessionInfo info = sessions.get(resource.getAccountId());
        DeploymentStatus status = new DeploymentStatus(info.getStatus().getCode(),
                                                       info.getStatus().getMessage(), null);
        return status;
      }
    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /**
   * Promotes a FAR from single node to cloud.
   *
   * @param id of the flow.
   * @return true if successful; false otherwise.
   * @throws AppFabricServiceException
   */
  @Override
  public boolean promote(AuthToken authToken, ResourceIdentifier id, String hostname)
    throws AppFabricServiceException {

    try {
      Preconditions.checkArgument(authToken.isSetToken(), "API key is not set");
      Preconditions.checkArgument(!hostname.isEmpty(), "Empty hostname passed.");

      final Location appArchive = store.getApplicationArchiveLocation(Id.Application.from(id.getAccountId(),
                                                                                          id.getApplicationId()));
      if (!appArchive.exists()) {
        throw new AppFabricServiceException("Unable to locate the application.");
      }

      String schema = "https";
      if ("localhost".equals(hostname)) {
        schema = "http";
      }

      int port = configuration.getInt("app.rest.port", 10007);
      String url = String.format("%s://%s:%d/app", schema, hostname, port);
      SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
        .setUrl(url)
        .setRequestTimeoutInMs((int) UPLOAD_TIMEOUT)
        .setHeader("X-Archive-Name", appArchive.getName())
        .setHeader("X-Continuuity-ApiKey", authToken.getToken())
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
    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException(throwable.getLocalizedMessage());
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
   * Deletes a program specified by {@code FlowIdentifier}.
   *
   * @param identifier of a flow.
   * @throws AppFabricServiceException when there is an issue deactivating the flow.
   */
  @Override
  public void remove(AuthToken token, FlowIdentifier identifier) throws AppFabricServiceException {
    try {
      Preconditions.checkNotNull(identifier, "No application id provided.");


      Id.Program programId = Id.Program.from(identifier.getAccountId(),
                                             identifier.getApplicationId(),
                                             identifier.getFlowId());

      // Make sure it is not running
      Preconditions.checkState(!anyRunning(new Predicate<Id.Program>() {
        @Override
        public boolean apply(Id.Program programId) {
          return programId.equals(programId);
        }
      }, Type.values()), "Program still running for application %s, %s.",
                               programId.getApplication(), programId.getId());


      Type programType = entityTypeToType(identifier);
      for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry : runtimeService.list(programType).entrySet()) {
        Preconditions.checkState(!programId.equals(entry.getValue().getProgramId()),
                                 "Program still running: application=%s, type=%s, program=%s",
                                 programId.getApplication(), programType, programId.getId());
      }
      // Delete the program from store.
      store.remove(programId);
    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException("Fail to delete program " + throwable.getMessage());
    }
  }

  @Override
  public void removeApplication(AuthToken token, FlowIdentifier identifier) throws AppFabricServiceException {
    try {
      Preconditions.checkNotNull(identifier, "No application id provided.");

      Id.Account accountId = Id.Account.from(identifier.getAccountId());
      final Id.Application appId = Id.Application.from(accountId, identifier.getApplicationId());

      // Check if all are stopped.
      Preconditions.checkState(!anyRunning(new Predicate<Id.Program>() {
        @Override
        public boolean apply(Id.Program programId) {
          return programId.getApplication().equals(appId);
        }
      }, Type.values()), "There are program still running for application " + appId.getId());

      Location appArchive = store.getApplicationArchiveLocation(appId);
      Preconditions.checkNotNull(appArchive, "Could not find the location of application", appId.getId());
      appArchive.delete();
      deleteMetrics(identifier.getAccountId(), identifier.getApplicationId());
      store.removeApplication(appId);
    } catch (Throwable  throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException("Fail to delete program " + throwable.getMessage());
    }
  }

  /**
   * Check if any program that satisfy the given {@link Predicate} is running
   * @param predicate Get call on each running {@link Id.Program}.
   * @param types Types of program to check
   * @return true if any of the running program satisfy the predicate, false otherwise.
   */
  private boolean anyRunning(Predicate<Id.Program> predicate, Type...types) {
    for (Type type : types) {
      for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry :  runtimeService.list(type).entrySet()) {
        if (predicate.apply(entry.getValue().getProgramId())) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void removeAll(AuthToken token, String account) throws AppFabricServiceException {
    Preconditions.checkNotNull(account);
    // TODO: Is it the same as reset??
  }

  @Override
  public void reset(AuthToken token, String account) throws AppFabricServiceException {
    final Id.Account accountId;
    try {
      Preconditions.checkNotNull(account);
      accountId = Id.Account.from(account);

      // Check if any program is still running
      Preconditions.checkState(!anyRunning(new Predicate<Id.Program>() {
        @Override
        public boolean apply(Id.Program programId) {
          return programId.getAccountId().equals(accountId.getId());
        }
      }, Type.values()), "There are programs still running on the Reactor. Please stop them first.");

      deleteMetrics(account);
      // delete all meta data
      store.removeAll(accountId);
      LOG.info("Deleting all data for account '" + account + "'.");
      opex.execute(
        new OperationContext(account),
        new ClearFabric(ClearFabric.ToClear.ALL)
      );
      LOG.info("All data for account '" + account + "' deleted.");
    } catch (Throwable throwable) {
      LOG.warn(StackTraceUtil.toStringStackTrace(throwable));
      throw new AppFabricServiceException(String.format(UserMessages.getMessage(UserErrors.RESET_FAIL),
                                                        throwable.getMessage()));
    }
  }

  /**
   * Deletes metrics for a given account.
   *
   * @param accountId for which the metrics need to be reset.
   * @throws IOException throw due to issue in reseting metrics for
   * @throws TException on thrift errors while talking to thrift service
   * @throws MetadataServiceException on errors from metadata service
   */
  private void deleteMetrics(String accountId) throws IOException, TException, MetadataServiceException {

    List<Application> applications = this.mds.getApplications(new Account(accountId));
    Discoverable discoverable = this.discoveryServiceClient.discover(Constants.SERVICE_METRICS).iterator().next();

    for (Application application : applications){
      String url = String.format("http://%s:%d/metrics/app/%s",
                                 discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort(),
                                 application.getId());
      SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
        .setUrl(url)
        .setRequestTimeoutInMs((int) METRICS_SERVER_RESPONSE_TIMEOUT)
        .build();

      client.delete();
    }

    String url = String.format("http://%s:%d/metrics",
                               discoverable.getSocketAddress().getHostName(),
                               discoverable.getSocketAddress().getPort());

    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) METRICS_SERVER_RESPONSE_TIMEOUT)
      .build();
    client.delete();
  }


  private void deleteMetrics(String account, String application) throws IOException {
    Discoverable discoverable = this.discoveryServiceClient.discover(Constants.SERVICE_METRICS).iterator().next();
    String url = String.format("http://%s:%d/metrics/app/%s",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    application);
    LOG.debug("Deleting metrics for application {}", application);
    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) METRICS_SERVER_RESPONSE_TIMEOUT)
      .build();

    client.delete();
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
      String accountId = info.getResourceIdenitifier().getAccountId();
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
      LOG.warn(StackTraceUtil.toStringStackTrace(e));
      return false;
    }
    return true;
  }

  /**
   * Retrieves a {@link SessionInfo} from the file system.
   *
   * @param accountId to which the
   * @return
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
        SessionInfo info = gson.fromJson(r, SessionInfo.class);
        return info;
      } finally {
        Closeables.closeQuietly(r);
      }
    } catch (IOException e) {
      LOG.warn("Failed to retrieve session info for account.");
    }
    return null;
  }
}
