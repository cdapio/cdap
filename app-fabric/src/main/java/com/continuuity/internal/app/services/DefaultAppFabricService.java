/*
* Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
*/

package com.continuuity.internal.app.services;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.ArchiveId;
import com.continuuity.app.services.ArchiveInfo;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DataType;
import com.continuuity.app.services.DeployStatus;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.ProgramDescriptor;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramRunRecord;
import com.continuuity.app.services.ProgramStatus;
import com.continuuity.app.services.RunIdentifier;
import com.continuuity.app.services.ScheduleId;
import com.continuuity.app.services.ScheduleRunTime;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.StreamAdmin;
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
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.ning.http.client.Body;
import com.ning.http.client.BodyGenerator;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

  /**
   * Number of seconds for timing out a service endpoint discovery.
   */
  private static final long DISCOVERY_TIMEOUT_SECONDS = 3;

  /**
   * Maintains a mapping of transient session state. The state is stored in memory,
   * in case of failure, all the current running sessions will be terminated. As
   * per the current implementation only connection per account is allowed to upload.
   */
  private final Map<String, SessionInfo> sessions = Maps.newConcurrentMap();

  /**
   * Used to manage datasets. TODO: implement and use DataSetService instead
   */
  private final DataSetAccessor dataSetAccessor;
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
   * App fabric output directory.
   */
  private final String appFabricDir;

  // We need it here now to be able to reset queues data
  private final QueueAdmin queueAdmin;
  // We need it here now to be able to reset queues data
  private final StreamAdmin streamAdmin;

  /**
   * Timeout to upload to remote app fabric.
   */
  private static final long UPLOAD_TIMEOUT = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

  /**
   * Timeout to get response from metrics system.
   */
  private static final long METRICS_SERVER_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  private final Scheduler scheduler;
  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  @Inject
  public DefaultAppFabricService(CConfiguration configuration, DataSetAccessor dataSetAccessor,
                                 LocationFactory locationFactory,
                                 ManagerFactory managerFactory, AuthorizationFactory authFactory,
                                 StoreFactory storeFactory, ProgramRuntimeService runtimeService,
                                 DiscoveryServiceClient discoveryServiceClient,
                                 QueueAdmin queueAdmin, StreamAdmin streamAdmin,
                                 @Assisted Scheduler scheduler) {
    this.dataSetAccessor = dataSetAccessor;
    this.locationFactory = locationFactory;
    this.configuration = configuration;
    this.managerFactory = managerFactory;
    this.authFactory = authFactory;
    this.runtimeService = runtimeService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.store = storeFactory.create();
    this.appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR,
                                          System.getProperty("java.io.tmpdir"));
    this.archiveDir = this.appFabricDir + "/archive";
    this.scheduler = scheduler;

    // Note: This is hacky to start service like this.
    if (this.runtimeService.state() != Service.State.RUNNING) {
      this.runtimeService.startAndWait();
    }
  }

  private Type entityTypeToType(ProgramId identifier) {
    return Type.valueOf(identifier.getType().name());
  }

  /**
   * Starts a Program.
   */
  @Override
  public synchronized RunIdentifier start(AuthToken token, ProgramDescriptor descriptor)
    throws AppFabricServiceException, TException {

    try {
      ProgramId id = descriptor.getIdentifier();
      ProgramRuntimeService.RuntimeInfo existingRuntimeInfo = findRuntimeInfo(id);
      Preconditions.checkArgument(existingRuntimeInfo == null, UserMessages.getMessage(UserErrors.ALREADY_RUNNING));
      final Id.Program programId = Id.Program.from(id.getAccountId(), id.getApplicationId(), id.getFlowId());

      Program program = store.loadProgram(programId, entityTypeToType(id));

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
      return new RunIdentifier(runId);

    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /**
   * Checks the status of a Program.
   */
  @Override
  public synchronized ProgramStatus status(AuthToken token, ProgramId id)
    throws AppFabricServiceException, TException {

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

      // NOTE: This was a temporary hack done to map the status to something that is
      // UI friendly. Internal states of program controller are reasonable and hence
      // no point in changing them.
      String status = controllerStateToString(runtimeInfo.getController().getState());
      return new ProgramStatus(programId.getApplicationId(), programId.getId(), runId, status);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
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
   * Stops a Program.
   */
  @Override
  public synchronized RunIdentifier stop(AuthToken token, ProgramId identifier)
    throws AppFabricServiceException, TException {
    try {
      return doStop(identifier);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  private RunIdentifier doStop(ProgramId identifier) throws ExecutionException, InterruptedException {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(identifier);
    Preconditions.checkNotNull(runtimeInfo, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               identifier.getApplicationId(), identifier.getFlowId());
    ProgramController controller = runtimeInfo.getController();
    RunId runId = controller.getRunId();
    controller.stop().get();
    return new RunIdentifier(runId.getId());
  }

  /**
   * Set number of instance of a flowlet.
   */
  @Override
  public void setFlowletInstances(AuthToken token, ProgramId identifier, String flowletId, short instances)
    throws AppFabricServiceException, TException {
    // storing the info about instances count after increasing the count of running flowlets: even if it fails, we
    // can at least set instances count for this session
    try {
      store.setFlowletInstances(Id.Program.from(identifier.getAccountId(), identifier.getApplicationId(),
                                                identifier.getFlowId()), flowletId, instances);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(identifier);
      if (runtimeInfo != null) {
        runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                          ImmutableMap.of(flowletId, (int) instances)).get();
      }
    } catch (Throwable throwable) {
      LOG.warn("Exception when setting instances for {}.{} to {}. {}",
               identifier.getFlowId(), flowletId, instances, throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /**
   * Get number of instance of a flowlet.
   */
  @Override
  public int getFlowletInstances(AuthToken token, ProgramId identifier, String flowletId)
    throws AppFabricServiceException, TException {
    try {
      return store.getFlowletInstances(Id.Program.from(identifier.getAccountId(), identifier.getApplicationId(),
                                       identifier.getFlowId()), flowletId);
    } catch (Throwable throwable) {
      LOG.warn("Exception when getting instances for {}.{} to {}. {}",
               identifier.getFlowId(), flowletId, throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  @Override
  public int getProgramInstances(AuthToken token, ProgramId identifier)
    throws AppFabricServiceException, TException {
    Type type = Type.valueOf(identifier.getType().name());
    Preconditions.checkArgument(type.equals(Type.PROCEDURE), "Can only get instances for procedure");

    try {
      return store.getProcedureInstances(Id.Program.from(identifier.getAccountId(),
                                                       identifier.getApplicationId(),
                                                       identifier.getFlowId()));
    } catch (Throwable throwable) {
      LOG.warn("Exception when getting instances for {}.{} to {}. {}",
               identifier.getFlowId(), type.name(), throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  @Override
  public void setProgramInstances(AuthToken token, ProgramId identifier, short instances)
    throws AppFabricServiceException, TException {
    Type type = Type.valueOf(identifier.getType().name());
    Preconditions.checkArgument(type.equals(Type.PROCEDURE), "Can only increase instance of procedure");

    try {
      store.setProcedureInstances(Id.Program.from(identifier.getAccountId(), identifier.getApplicationId(),
                                                identifier.getFlowId()), instances);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(identifier);
      if (runtimeInfo != null) {
        runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                            ImmutableMap.of(identifier.getFlowId(), (int) instances)).get();
      }
    } catch (Throwable throwable) {
      LOG.warn("Exception when getting instances for {}.{} to {}. {}",
               identifier.getFlowId(), type.name(), throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
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

  /**
   * Returns definition of a flow.
   */
  @Override
  public String getSpecification(ProgramId id)
    throws AppFabricServiceException, TException {

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

  @Override
  public String listPrograms(ProgramId id, EntityType type) throws AppFabricServiceException, TException {
    try {
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(new Id.Account(id.getAccountId()));
      if (appSpecs == null) {
        return "";
      } else {
        return listPrograms(appSpecs, type);
      }
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw  new AppFabricServiceException("Could not retrieve application spec for " +
                                             id.toString() + ", reason: " + e.getMessage());
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  @Override
  public String listProgramsByApp(ProgramId id, EntityType type) throws AppFabricServiceException, TException {

    ApplicationSpecification appSpec;
    try {
      appSpec = store.getApplication(new Id.Application(new Id.Account(id.getAccountId()), id.getApplicationId()));
      if (appSpec == null) {
        return "";
      } else {
        return listPrograms(Collections.singletonList(appSpec), type);
      }
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw  new AppFabricServiceException("Could not retrieve application spec for " +
                                             id.toString() + ", reason: " + e.getMessage());
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  private String listPrograms(Collection<ApplicationSpecification> appSpecs, EntityType type)
    throws AppFabricServiceException {

    List<Map<String, String>> result = Lists.newArrayList();
    for (ApplicationSpecification appSpec : appSpecs) {
      if (type == EntityType.FLOW) {
        for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
          result.add(makeFlowRecord(appSpec.getName(), flowSpec));
        }
      } else if (type == EntityType.PROCEDURE) {
        for (ProcedureSpecification procedureSpec : appSpec.getProcedures().values()) {
          result.add(makeProcedureRecord(appSpec.getName(), procedureSpec));
        }
      } else if (type == EntityType.MAPREDUCE) {
        for (MapReduceSpecification mrSpec : appSpec.getMapReduce().values()) {
          result.add(makeMapReduceRecord(appSpec.getName(), mrSpec));
        }
      } else if (type == EntityType.WORKFLOW) {
        for (WorkflowSpecification wfSpec : appSpec.getWorkflows().values()) {
          result.add(makeWorkflowRecord(appSpec.getName(), wfSpec));
        }
      } else if (type == EntityType.APP) {
         result.add(makeAppRecord(appSpec));
      } else {
        throw new AppFabricServiceException("Unknown program type: " + type.name());
      }
    }
    return new Gson().toJson(result);
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

  private static Set<String> dataSetsUsedBy(FlowSpecification flowSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowletDefinition flowlet : flowSpec.getFlowlets().values()) {
      result.addAll(flowlet.getDatasets());
    }
    return result;
  }

  private static Set<String> dataSetsUsedBy(ApplicationSpecification appSpec) {
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

  private static Set<String> streamsUsedBy(FlowSpecification flowSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowletConnection con : flowSpec.getConnections()) {
      if (FlowletConnection.Type.STREAM == con.getSourceType()) {
        result.add(con.getSourceName());
      }
    }
    return result;
  }

  private static Set<String> streamsUsedBy(ApplicationSpecification appSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
      result.addAll(streamsUsedBy(flowSpec));
    }
    result.addAll(appSpec.getStreams().keySet());
    return result;
  }

  @Override
  public String listProgramsByDataAccess(ProgramId id, EntityType type, DataType data, String name)
    throws AppFabricServiceException, TException {

    try {
      List<Map<String, String>> result = Lists.newArrayList();
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(new Id.Account(id.getAccountId()));
      if (appSpecs != null) {
        for (ApplicationSpecification appSpec : appSpecs) {
          if (type == EntityType.FLOW) {
            for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
              if ((data == DataType.DATASET && usesDataSet(flowSpec, name))
                || (data == DataType.STREAM && usesStream(flowSpec, name))) {
                result.add(makeFlowRecord(appSpec.getName(), flowSpec));
              }
            }
          } else if (type == EntityType.PROCEDURE) {
            for (ProcedureSpecification procedureSpec : appSpec.getProcedures().values()) {
              if (data == DataType.DATASET && procedureSpec.getDataSets().contains(name)) {
                result.add(makeProcedureRecord(appSpec.getName(), procedureSpec));
              }
            }
          } else if (type == EntityType.MAPREDUCE) {
            for (MapReduceSpecification mrSpec : appSpec.getMapReduce().values()) {
              if (data == DataType.DATASET && mrSpec.getDataSets().contains(name)) {
                result.add(makeMapReduceRecord(appSpec.getName(), mrSpec));
              }
            }
          }
        }
      }
      return new Gson().toJson(result);
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw  new AppFabricServiceException("Could not retrieve application specs for " +
                                             id.toString() + ", reason: " + e.getMessage());
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  @Override
  public void createStream(ProgramId id, String spec) throws AppFabricServiceException, TException {
    try {
      StreamSpecification streamSpec = new Gson().fromJson(spec, StreamSpecification.class);
      store.addStream(new Id.Account(id.getAccountId()), streamSpec);
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw  new AppFabricServiceException("Could not create stream for " +
                                             id.toString() + ", reason: " + e.getMessage());
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  @Override
  public void createDataSet(ProgramId id, String spec) throws AppFabricServiceException, TException {
    try {
      DataSetSpecification streamSpec = new Gson().fromJson(spec, DataSetSpecification.class);
      store.addDataset(new Id.Account(id.getAccountId()), streamSpec);
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw  new AppFabricServiceException("Could not create dataset for " +
                                             id.toString() + ", reason: " + e.getMessage());
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  @Override
  public String getDataEntity(ProgramId id, DataType type, String name) throws AppFabricServiceException, TException {
    try {
      if (type == DataType.DATASET) {
        DataSetSpecification spec = store.getDataSet(new Id.Account(id.getAccountId()), name);
        return spec == null ? "" : new Gson().toJson(makeDataSetRecord(spec.getName(), spec.getType(), spec));
      }
      if (type == DataType.STREAM) {
        StreamSpecification spec = store.getStream(new Id.Account(id.getAccountId()), name);
        return spec == null ? "" : new Gson().toJson(makeStreamRecord(spec.getName(), spec));
      }
      return "";
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw  new AppFabricServiceException("Could not retrieve data specs for " +
                                             id.toString() + ", reason: " + e.getMessage());
    }
  }

  @Override
  public String listDataEntities(ProgramId id, DataType type) throws AppFabricServiceException, TException {
    try {
      if (type == DataType.DATASET) {
        Collection<DataSetSpecification> specs = store.getAllDataSets(new Id.Account(id.getAccountId()));
        List<Map<String, String>> result = Lists.newArrayListWithExpectedSize(specs.size());
        for (DataSetSpecification spec : specs) {
          result.add(makeDataSetRecord(spec.getName(), spec.getType(), null));
        }
        return new Gson().toJson(result);
      }
      if (type == DataType.STREAM) {
        Collection<StreamSpecification> specs = store.getAllStreams(new Id.Account(id.getAccountId()));
        List<Map<String, String>> result = Lists.newArrayListWithExpectedSize(specs.size());
        for (StreamSpecification spec : specs) {
          result.add(makeStreamRecord(spec.getName(), null));
        }
        return new Gson().toJson(result);
      }
      return "";
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw  new AppFabricServiceException("Could not retrieve data specs for " +
                                             id.toString() + ", reason: " + e.getMessage());
    }
  }

  @Override
  public String listDataEntitiesByApp(ProgramId id, DataType type) throws AppFabricServiceException, TException {
    try {
      Id.Account account = new Id.Account(id.getAccountId());
      ApplicationSpecification appSpec = store.getApplication(new Id.Application(account, id.getApplicationId()));
      if (type == DataType.DATASET) {
        Set<String> dataSetsUsed = dataSetsUsedBy(appSpec);
        List<Map<String, String>> result = Lists.newArrayListWithExpectedSize(dataSetsUsed.size());
        for (String dsName : dataSetsUsed) {
          DataSetSpecification spec = appSpec.getDataSets().get(dsName);
          if (spec == null) {
            spec = store.getDataSet(account, dsName);
          }
          result.add(makeDataSetRecord(dsName, spec == null ? null : spec.getType(), null));
        }
        return new Gson().toJson(result);
      }
      if (type == DataType.STREAM) {
        Set<String> streamsUsed = streamsUsedBy(appSpec);
        List<Map<String, String>> result = Lists.newArrayListWithExpectedSize(streamsUsed.size());
        for (String streamName : streamsUsed) {
          result.add(makeStreamRecord(streamName, null));
        }
        return new Gson().toJson(result);
      }
      return "";
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw  new AppFabricServiceException("Could not retrieve data specs for " +
                                             id.toString() + ", reason: " + e.getMessage());
    }
  }

  /**
   * Returns run information for a given Runnable id.
   *
   * @param id        of the program.
   * @param startTime fetch run history that has started after the startTime.
   * @param endTime   fetch run history that has started before the endTime.
   * @param limit     maxEntries to fetch for the history call.
   */
  @Override
  public List<ProgramRunRecord> getHistory(ProgramId id, long startTime, long endTime, int limit)
        throws AppFabricServiceException, TException {
    List<RunRecord> log;
    try {
      Id.Program programId = Id.Program.from(id.getAccountId(), id.getApplicationId(), id.getFlowId());
      try {
        log = store.getRunHistory(programId, startTime, endTime, limit);
      } catch (OperationException e) {
        throw new AppFabricServiceException(String.format(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND),
                                                          id.toString(), e.getMessage()));
      }
      List<ProgramRunRecord> history = new ArrayList<ProgramRunRecord>();
      for (RunRecord runRecord : log) {
        history.add(new ProgramRunRecord(runRecord.getPid(), runRecord.getStartTs(),
                                      runRecord.getStopTs(), runRecord.getEndStatus())
        );
      }
      return history;
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(throwable.getMessage());
    }
  }

  /**
   * Returns run information for a given flow id.
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
          LOG.warn(e.getMessage(), e);
          throw new AppFabricServiceException(e.getMessage());
        }
      }
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
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
   * @param info ArchiveInfo
   * @return ArchiveId instance containing the resource id and
   * resource version.
   */
  @Override
  public ArchiveId init(AuthToken token, ArchiveInfo info) throws AppFabricServiceException {
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

  /**
   * Writes chunk of data transmitted from the client. Along with data, there is the session id also
   * being returned.
   *
   * @param resource identifier.
   * @param chunk binary data of the resource transmitted from the client.
   * @throws AppFabricServiceException
   */
  @Override
  public void chunk(AuthToken token, ArchiveId resource, ByteBuffer chunk) throws AppFabricServiceException {
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
      LOG.warn(throwable.getMessage(), throwable);
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
  public void deploy(AuthToken token, final ArchiveId resource) throws AppFabricServiceException {
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

      ApplicationWithPrograms applicationWithPrograms = manager.deploy(id, archiveLocation).get();
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
      throw new AppFabricServiceException(e.getMessage());
    } finally {
      save(sessionInfo.setStatus(status));
      sessions.remove(resource.getAccountId());
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
          doStop(new ProgramId(id.getId(), programId.getApplicationId(), programId.getId()));
        }
        break;
      case PROCEDURE:
        //Stop the procedure if it not running
        ProgramRuntimeService.RuntimeInfo procedureRunInfo = findRuntimeInfo(new ProgramId(programId.getAccountId(),
                                                                                           programId.getApplicationId(),
                                                                                           programId.getId()));
        if (procedureRunInfo != null) {
          doStop(new ProgramId(id.getId(), programId.getApplicationId(), programId.getId()));
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
   * Returns status of deployment of archive.
   *
   * @param resource identifier
   * @return status of resource processing.
   * @throws AppFabricServiceException
   */
  @Override
  public DeploymentStatus dstatus(AuthToken token, ArchiveId resource) throws AppFabricServiceException {
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

  /**
   * Promotes a FAR from single node to cloud.
   *
   * @param id of the flow.
   * @return true if successful; false otherwise.
   * @throws AppFabricServiceException
   */
  @Override
  public boolean promote(AuthToken authToken, ArchiveId id, String hostname)
    throws AppFabricServiceException {

    try {
      Preconditions.checkArgument(!hostname.isEmpty(), "Empty hostname passed.");

      final Location appArchive = store.getApplicationArchiveLocation(Id.Application.from(id.getAccountId(),
                                                                                          id.getApplicationId()));
      if (appArchive == null || !appArchive.exists()) {
        throw new AppFabricServiceException("Unable to locate the application.");
      }

      String schema = "https";
      if ("localhost".equals(hostname)) {
        schema = "http";
      }

      String url = String.format("%s://%s:%d/v2/apps", schema, hostname, Constants.Gateway.DEFAULT_PORT);
      SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
        .setUrl(url)
        .setRequestTimeoutInMs((int) UPLOAD_TIMEOUT)
        .setHeader("X-Archive-Name", appArchive.getName())
        .setHeader("X-Continuuity-ApiKey", authToken.getToken())
        .build();

      try {
        Future<Response> future = client.post(new LocationBodyGenerator(appArchive));
        Response response = future.get(UPLOAD_TIMEOUT, TimeUnit.MILLISECONDS);
        if (response.getStatusCode() != 200) {
          throw new RuntimeException(response.getResponseBody());
        }
        return true;
      } finally {
        client.close();
      }
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
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

  @Override
  public void removeApplication(AuthToken token, ProgramId identifier) throws AppFabricServiceException {
    try {
      Preconditions.checkNotNull(identifier, "No application id provided.");

      Id.Account accountId = Id.Account.from(identifier.getAccountId());
      final Id.Application appId = Id.Application.from(accountId, identifier.getApplicationId());

      // Check if all are stopped.
      checkAnyRunning(new Predicate<Id.Program>() {
        @Override
        public boolean apply(Id.Program programId) {
          return programId.getApplication().equals(appId);
        }
      }, Type.values());

      //Delete the schedules
      ApplicationSpecification spec = store.getApplication(appId);
      for (WorkflowSpecification workflowSpec : spec.getWorkflows().values()){
        Id.Program workflowProgramId = Id.Program.from(appId, workflowSpec.getName());
        List<String> schedules = scheduler.getScheduleIds(workflowProgramId, Type.WORKFLOW);
        if (!schedules.isEmpty()) {
          scheduler.deleteSchedules(workflowProgramId, Type.WORKFLOW, schedules);
        }
      }
      deleteProgramLocations(appId);

      // also delete all queue state of each flow
      for (FlowSpecification flowSpecification : spec.getFlows().values()) {
        queueAdmin.dropAllForFlow(identifier.getApplicationId(), flowSpecification.getName());
      }

      Location appArchive = store.getApplicationArchiveLocation(appId);
      Preconditions.checkNotNull(appArchive, "Could not find the location of application", appId.getId());
      appArchive.delete();
      deleteMetrics(identifier.getAccountId(), identifier.getApplicationId());
      store.removeApplication(appId);
    } catch (Throwable  throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException("Fail to delete program " + throwable.getMessage());
    }
  }

  /**
   * Check if any program that satisfy the given {@link Predicate} is running.
   *
   * @param predicate Get call on each running {@link Id.Program}.
   * @param types Types of program to check
   * @throws IllegalStateException if a program is running as defined by the predicate.
   */
  private void checkAnyRunning(Predicate<Id.Program> predicate, Type... types) {
    for (Type type : types) {
      for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry :  runtimeService.list(type).entrySet()) {
        Id.Program programId = entry.getValue().getProgramId();
        if (predicate.apply(programId)) {
          throw new IllegalStateException(String.format("Program still running: %s %s %s %s",
                                                        programId.getApplicationId(), type, programId.getId(),
                                                        entry.getValue().getController().getRunId()));
        }
      }
    }
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

    for (ProgramSpecification spec : programSpecs){
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
      checkAnyRunning(new Predicate<Id.Program>() {
        @Override
        public boolean apply(Id.Program programId) {
          return programId.getAccountId().equals(accountId.getId());
        }
      }, Type.values());

      deleteMetrics(account);
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
        datasetsToKeep.add(scope.name().toLowerCase() + "." + configuration.get(
          MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME, MetricsConstants.DEFAULT_ENTITY_TABLE_NAME));
      }
      // NOTE: there could be services running at the moment that rely on the system datasets to be available.
      dataSetAccessor.truncateAllExceptBlacklist(DataSetAccessor.Namespace.SYSTEM, datasetsToKeep);

      LOG.info("All data for account '" + account + "' deleted.");
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new AppFabricServiceException(String.format(UserMessages.getMessage(UserErrors.RESET_FAIL),
                                                        throwable.getMessage()));
    }
  }


  @Override
  public void resumeSchedule(AuthToken token, ScheduleId identifier)
                                      throws AppFabricServiceException, TException {
    Preconditions.checkNotNull(identifier, "No program id provided.");
    scheduler.resumeSchedule(identifier.getId());
  }

  @Override
  public void suspendSchedule(AuthToken token, ScheduleId identifier)
                                       throws AppFabricServiceException, TException {
    Preconditions.checkNotNull(identifier, "No program id provided.");
    scheduler.suspendSchedule(identifier.getId());
  }

  @Override
  public List<ScheduleId> getSchedules(AuthToken token, ProgramId identifier)
                                       throws AppFabricServiceException, TException {
    Preconditions.checkNotNull(identifier, "No program id provided.");
    Id.Program programId = Id.Program.from(identifier.getAccountId(),
                                           identifier.getApplicationId(),
                                           identifier.getFlowId());
    Type programType = entityTypeToType(identifier);

    List<ScheduleId> scheduleIds = Lists.newArrayList();
    for (String id : scheduler.getScheduleIds(programId, programType)) {
      scheduleIds.add(new ScheduleId(id));
    }
    return scheduleIds;
  }

  @Override
  public List<ScheduleRunTime> getNextScheduledRunTime(AuthToken token, ProgramId identifier)
                                                       throws TException {
    Preconditions.checkNotNull(identifier, "No program id provided.");
    Id.Program programId = Id.Program.from(identifier.getAccountId(),
                                           identifier.getApplicationId(),
                                           identifier.getFlowId());
    Type programType = entityTypeToType(identifier);

    List<ScheduledRuntime> runtimes = scheduler.nextScheduledRuntime(programId, programType);
    List<ScheduleRunTime> r = Lists.newArrayList();
    for (ScheduledRuntime runtime : runtimes) {
      r.add(new ScheduleRunTime(new ScheduleId(runtime.getScheduleId()), runtime.getTime()));
    }
    return r;
  }

  @Override
  public void storeRuntimeArguments(AuthToken token, ProgramId identifier, Map<String, String> arguments)
                                    throws AppFabricServiceException, TException {
    Preconditions.checkNotNull(identifier, "No program id provided.");
    Id.Program programId = Id.Program.from(identifier.getAccountId(),
                                           identifier.getApplicationId(),
                                           identifier.getFlowId());
    try {
      store.storeRunArguments(programId, arguments);
    } catch (OperationException e) {
      LOG.warn("Error storing runtime args {}", e.getMessage(), e);
      throw new AppFabricServiceException(e.getMessage());
    }
  }


  @Override
  public String getScheduleState(ScheduleId scheduleId)
                                 throws AppFabricServiceException, TException {
    return scheduler.scheduleState(scheduleId.getId()).toString();
  }


  @Override
  public Map<String, String> getRuntimeArguments(AuthToken token, ProgramId identifier)
                                                 throws AppFabricServiceException, TException {
    Preconditions.checkNotNull(identifier, "No program id provided.");
    Id.Program programId = Id.Program.from(identifier.getAccountId(),
                                           identifier.getApplicationId(),
                                           identifier.getFlowId());
    try {
      return store.getRunArguments(programId);
    } catch (OperationException e) {
      LOG.warn("Error getting runtime args {}", e.getMessage(), e);
      throw new AppFabricServiceException(e.getMessage());
    }
  }

  /**
   * Deletes metrics for a given account.
   *
   * @param accountId for which the metrics need to be reset.
   * @throws IOException throw due to issue in reseting metrics for
   * @throws TException on thrift errors while talking to thrift service
   */
  private void deleteMetrics(String accountId)
    throws IOException, TException, OperationException {

    Collection<ApplicationSpecification> applications = this.store.getAllApplications(new Id.Account(accountId));
    Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(Constants.Service.GATEWAY);
    Discoverable discoverable = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoverables),
                                                              DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS).pick();

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      return;
    }

    for (MetricsScope scope : MetricsScope.values()) {
      for (ApplicationSpecification application : applications){
        String url = String.format("http://%s:%d%s/metrics/%s/apps/%s",
                                   discoverable.getSocketAddress().getHostName(),
                                   discoverable.getSocketAddress().getPort(),
                                   Constants.Gateway.GATEWAY_VERSION,
                                   scope.name().toLowerCase(),
                                   application.getName());
        sendMetricsDelete(url);
      }
    }

    String url = String.format("http://%s:%d%s/metrics",
                               discoverable.getSocketAddress().getHostName(),
                               discoverable.getSocketAddress().getPort(),
                               Constants.Gateway.GATEWAY_VERSION);
    sendMetricsDelete(url);
  }

  private void deleteMetrics(String account, String application) throws IOException {
    Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(Constants.Service.GATEWAY);
    Discoverable discoverable = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoverables),
                                                              DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS).pick();

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      return;
    }

    LOG.debug("Deleting metrics for application {}", application);
    for (MetricsScope scope : MetricsScope.values()) {
      String url = String.format("http://%s:%d%s/metrics/%s/apps/%s",
                                 discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort(),
                                 Constants.Gateway.GATEWAY_VERSION,
                                 scope.name().toLowerCase(),
                                 application);
      sendMetricsDelete(url);
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

  /**
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

  /* -----------------  helpers to return Jsion consistently -------------- */

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

  private static Map<String, String> makeFlowRecord(String app, FlowSpecification spec) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Flow");
    builder.put("app", app);
    builder.put("id", spec.getName());
    builder.put("name", spec.getName());
    if (spec.getDescription() != null) {
      builder.put("description", spec.getDescription());
    }
    return builder.build();
  }

  private static Map<String, String> makeProcedureRecord(String app, ProcedureSpecification spec) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Procedure");
    builder.put("app", app);
    builder.put("id", spec.getName());
    builder.put("name", spec.getName());
    if (spec.getDescription() != null) {
      builder.put("description", spec.getDescription());
    }
    return builder.build();
  }

  private static Map<String, String> makeMapReduceRecord(String app, MapReduceSpecification spec) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Mapreduce");
    builder.put("app", app);
    builder.put("id", spec.getName());
    builder.put("name", spec.getName());
    if (spec.getDescription() != null) {
      builder.put("description", spec.getDescription());
    }
    return builder.build();
  }

  private static Map<String, String> makeWorkflowRecord(String app, WorkflowSpecification spec) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Workflow");
    builder.put("app", app);
    builder.put("id", spec.getName());
    builder.put("name", spec.getName());
    if (spec.getDescription() != null) {
      builder.put("description", spec.getDescription());
    }
    return builder.build();
  }
}
