///*
// * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
// */
//
//package com.continuuity.app.services;
//
//import com.google.common.io.Closeables;
//import org.slf4j.Logger;
//
//import com.continuuity.common.conf.CConfiguration;
//import com.continuuity.common.conf.Constants;
//import com.continuuity.common.discovery.ServiceDiscoveryClient;
//import com.continuuity.common.discovery.ServiceDiscoveryClientException;
//import com.continuuity.common.discovery.ServicePayload;
//import com.continuuity.common.utils.Copyright;
//import com.continuuity.common.utils.ImmutablePair;
//import com.continuuity.common.utils.UsageException;
//import com.continuuity.data.operation.OperationContext;
//import com.continuuity.internal.app.BufferFileInputStream;
//import com.continuuity.metrics2.thrift.Counter;
//import com.continuuity.metrics2.thrift.CounterRequest;
//import com.continuuity.metrics2.thrift.FlowArgument;
//import com.continuuity.metrics2.thrift.MetricTimeseriesLevel;
//import com.continuuity.metrics2.thrift.MetricsFrontendService;
//import com.continuuity.metrics2.thrift.MetricsServiceException;
//import com.continuuity.metrics2.thrift.Point;
//import com.continuuity.metrics2.thrift.Points;
//import com.continuuity.metrics2.thrift.TimeseriesRequest;
//import com.google.common.base.Preconditions;
//import com.google.common.collect.Lists;
//import com.netflix.curator.x.discovery.ProviderStrategy;
//import com.netflix.curator.x.discovery.ServiceInstance;
//import com.netflix.curator.x.discovery.strategies.RandomStrategy;
//import org.apache.commons.lang.StringUtils;
//import org.apache.thrift.TException;
//import org.apache.thrift.protocol.TBinaryProtocol;
//import org.apache.thrift.protocol.TProtocol;
//import org.apache.thrift.transport.TFramedTransport;
//import org.apache.thrift.transport.TSocket;
//import org.apache.thrift.transport.TTransport;
//import org.apache.thrift.transport.TTransportException;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.PrintStream;
//import java.nio.ByteBuffer;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Date;
//import java.util.List;
//import java.util.Map;
//
///**
//* Client for interacting with Flow Manager, FAR Manager &
//* Metrics Frontend service.
//*/
//public class AppFabricClient {
//  private static final Logger Log = LoggerFactory.getLogger(AppFabricClient.class);
//
//  /**
//   * Instance of service discovery client used for discovering
//   * flow and far services.
//   */
//  private final ServiceDiscoveryClient serviceDiscoveryClient;
//
//  /**
//   * Instance of configuration.
//   */
//  private final CConfiguration configuration;
//
//  /**
//   * Instance of client for Deployment service.
//   */
//  private AppFabricService.Client client = null;
//
//  private boolean remoteDeploymentService = false;
//
//  public AppFabricClient(CConfiguration configuration, boolean remoteDeploymentService)
//    throws Exception {
//    this.remoteDeploymentService = remoteDeploymentService;
//    this.configuration = configuration;
//
//    // Add a shutdown hook.
//    Runtime.getRuntime().addShutdownHook(
//      new Thread(new Runnable() {
//        @Override
//        public void run() {
//          if(client != null) {
//            client.getInputProtocol().getTransport().close();
//            client.getOutputProtocol().getTransport().close();
//          }
//        }
//      })
//    );
//  }
//
//  public AppFabricClient(CConfiguration configuration) throws Exception {
//    this(configuration, false);
//  }
//
//  /**
//   * Promote a given flow to cloud.
//   *
//   * @param accountId     for the flow.
//   * @param applicationId the flow belongs to.
//   * @param entityId        id of the flow.
//   * @param version     of the flow.
//   * @return true if the promotion was successful, false otherwise.
//   */
//  public boolean promote(String accountId, String applicationId, String entityId,
//                         int version, EntityType type) throws IOException {
//    Preconditions.checkNotNull(accountId);
//    Preconditions.checkNotNull(applicationId);
//    Preconditions.checkNotNull(entityId);
//    Preconditions.checkNotNull(version);
//
//    // Use HTTPS
//    return false;
//  }
//
//  /**
//   * Deploys a given local flow resource file to a remote server.
//   *
//   * @param file path to the file to be deployed
//   * @return pair consisting of status and message description.
//   * @throws java.io.IOException
//   */
//  public ImmutablePair<Boolean, String> deploy(File file, String accountId,
//                                     String applicationId) throws IOException {
//
//    // Check if the local resource exists
//    if(! file.exists()) {
//      return new ImmutablePair<Boolean, String>(false,
//        String.format("File %s does not exists.", file.toString()));
//    }
//
//    // Connects to local FAR service.
//    connectToFARService(remoteDeploymentService);
//
//    // Construct resource info
//    ResourceInfo info = new ResourceInfo();
//    info.setAccountId(accountId);
//    info.setApplicationId(applicationId);
//    info.setFilename(file.getName());
//    info.setModtime(file.lastModified());
//    info.setSize((int) file.getTotalSpace());
//
//    try {
//      // Initialize the resource deployment
//      ResourceIdentifier identifier = client.init(getAuthToken(), info);
//      if(identifier == null) {
//        return new ImmutablePair<Boolean, String>(false,
//          "Unable to initialize deployment phase.");
//      }
//
//      // Break resource into chunks are transfer them
//      BufferFileInputStream is
//        = new BufferFileInputStream(file.getAbsolutePath(), 100*1024);
//      while(true) {
//        byte[] toSubmit = is.read();
//        if(toSubmit.length==0) break;
//        client.chunk(getAuthToken(),
//          identifier, ByteBuffer.wrap(toSubmit));
//      }
//      is.close();
//
//      // Finalize the resource deployment
//      client.deploy(getAuthToken(), identifier);
//
//
//      // If a flow within the resource is running, then we don't
//      // successfully deploy the resource
//      DeploymentStatus status = client.status(getAuthToken(), identifier);
//      if(status.getOverall()
//        == DeployStatus.ALREADY_RUNNING) {
//        return new ImmutablePair<Boolean, String>(false, "One of the flow " +
//          "from the resource being deployed is already running. " +
//          "Stop it first.");
//      }
//
//      int count = 0;
//      while(status.getOverall()
//        == DeployStatus.VERIFYING) {
//        if(count > 5) {
//          return new ImmutablePair<Boolean, String>(false,
//            "Verification took more than 5 seconds. Timing out. " +
//              "Please check contents of the resource file to make sure " +
//              "you have not included libs that are not packaged.");
//        }
//        Thread.sleep(1000);
//        count++;
//        status = client.status(getAuthToken(), identifier);
//      }
//      if(status.getOverall()
//        == DeployStatus.VERIFICATION_FAILURE) {
//        return new ImmutablePair<Boolean, String>(false, status.getMessage());
//      }
//    } catch (DeploymentServiceException e) {
//      return new ImmutablePair<Boolean, String>(false, e.getMessage());
//    } catch (TException e) {
//      throw new IOException(e);
//    } catch (InterruptedException e) {
//      throw new RuntimeException(e);
//    }
//    return new ImmutablePair<Boolean, String>(true, "OK");
//  }
//
//  /**
//   * Connect to flow service
//   */
//  private void connectToFlowService() throws IOException {
//
//    if(RuntimeServiceClient == null ||
//       ! RuntimeServiceClient.getInputProtocol().getTransport().isOpen()) {
//
//      String serverAddress = configuration.get(
//        Constants.CFG_FLOW_MANAGER_SERVER_ADDRESS,
//        Constants.DEFAULT_FLOW_MANAGER_SERVER_ADDRESS
//      );
//
//      int serverPort = configuration.getInt(
//        Constants.CFG_FLOW_MANAGER_SERVER_PORT,
//        Constants.DEFAULT_FLOW_MANAGER_SERVER_PORT
//      );
//
//      TTransport transport = new TFramedTransport(
//        new TSocket(serverAddress, serverPort));
//      try {
//        transport.open();
//      } catch (TTransportException e) {
//        throw new IOException("Unable to connect to flow service", e);
//      }
//      TProtocol protocol = new TBinaryProtocol(transport);
//      RuntimeServiceClient = new RuntimeService.Client(protocol);
//    }
//  }
//
//  /**
//   * Connects to local FAR service.
//   */
//  private void connectToFARService(boolean remote) throws IOException {
//    // If client is not connected or if it was connected,
//    // but is no more connected now.
//    if(client == null ||
//       ! client.getInputProtocol().getTransport().isOpen()) {
//
//      // TODO: Need to figure out how this integrates with
//      // service discovery.
//      String serverAddress = null;
//      int serverPort = -1;
//
//      if(! remote) {
//        serverAddress = configuration.get(
//          Constants.CFG_RESOURCE_MANAGER_SERVER_ADDRESS,
//          Constants.DEFAULT_RESOURCE_MANAGER_SERVER_ADDRESS
//        );
//
//        serverPort =
//        configuration.getInt(
//          Constants.CFG_RESOURCE_MANAGER_SERVER_PORT,
//          Constants.DEFAULT_RESOURCE_MANAGER_SERVER_PORT
//        );
//      } else {
//        serverAddress = configuration.get(
//          Constants.CFG_RESOURCE_MANAGER_CLOUD_HOST,
//          Constants.DEFAULT_RESOURCE_MANAGER_CLOUD_HOST
//        );
//
//        serverPort =
//          configuration.getInt(
//            Constants.CFG_RESOURCE_MANAGER_CLOUD_PORT,
//            Constants.DEFAULT_RESOURCE_MANAGER_CLOUD_PORT
//          );
//      }
//
//      TTransport transport = new TFramedTransport(
//        new TSocket(serverAddress, serverPort));
//
//      try {
//        transport.open();
//      } catch (TTransportException e) {
//        throw new IOException("Unable to connect to service", e);
//      }
//
//      TProtocol protocol = new TBinaryProtocol(transport);
//      client = new DeploymentService.Client(protocol);
//    }
//  }
//
//  /**
//   * Connects to local metrics service.
//   */
//  private void connectToMetricsService() throws IOException {
//    // If client is not connected or if it was connected,
//    // but is no more connected now.
//    if(metricsServiceClient == null ||
//      ! metricsServiceClient.getInputProtocol().getTransport().isOpen()) {
//
//      // TODO: Need to figure out how this integrates with
//      // service discovery.
//      String serverAddress = null;
//      int serverPort = -1;
//
//      serverAddress = configuration.get(
//        Constants.CFG_METRICS_FRONTEND_SERVER_ADDRESS,
//        Constants.DEFAULT_METRICS_FRONTEND_SERVER_ADDRESS
//      );
//
//      serverPort =
//        configuration.getInt(
//          Constants.CFG_METRICS_FRONTEND_SERVER_PORT,
//          Constants.DEFAULT_METRICS_FRONTEND_SERVER_PORT
//        );
//
//      TTransport transport = new TFramedTransport(
//        new TSocket(serverAddress, serverPort));
//
//      try {
//        transport.open();
//      } catch (TTransportException e) {
//        throw new IOException("Unable to connect to service", e);
//      }
//
//      TProtocol protocol = new TBinaryProtocol(transport);
//      metricsServiceClient = new MetricsFrontendService.Client(protocol);
//    }
//  }
//
//  /**
//   * Starts a flow that is deployed.
//   *
//   * @param accountId the flow is associated with
//   * @param applicationId the flow belongs to
//   * @param entityId of the flow
//   * @param version of flow
//   * @return {@link RunIdentifier} of started flow
//   * @throws java.io.IOException
//   */
//  public RunIdentifier start(String accountId, String applicationId,
//                             String entityId, int version, EntityType type)
//    throws IOException {
//    Preconditions.checkNotNull(accountId);
//    Preconditions.checkNotNull(applicationId);
//    Preconditions.checkNotNull(entityId);
//    Preconditions.checkArgument(version != 0);
//
//    // Connects to flow service.
//    connectToFlowService();
//
//    // Create a flow Identifier.
//    FlowIdentifier identifier
//      = new FlowIdentifier(accountId, applicationId, entityId, version);
//    identifier.setType(type);
//    identifier.setAccountId(accountId);
//    try {
//      return RuntimeServiceClient.start(getAuthToken(),
//        new FlowDescriptor(identifier, new ArrayList<String>()));
//    } catch (RuntimeServiceException e) {
//      throw new IOException(e);
//    } catch (TException e) {
//      throw new IOException(e);
//    }
//  }
//
//  /**
//   * Stops a flow that is deployed.
//   *
//   * @param accountId the flow is associated with
//   * @param applicationId the flow belongs to
//   * @param entityId of the flow
//   * @param version of flow
//   * @return {@link RunIdentifier} of started flow
//   * @throws java.io.IOException
//   */
//  public RunIdentifier stop(String accountId, String applicationId,
//                            String entityId, int version, EntityType type) throws IOException {
//    Preconditions.checkNotNull(accountId);
//    Preconditions.checkNotNull(applicationId);
//    Preconditions.checkNotNull(entityId);
//
//    // Connects to flow service.
//    connectToFlowService();
//
//    FlowIdentifier identifier
//      = new FlowIdentifier(accountId, applicationId, entityId, version);
//    identifier.setType(type);
//    identifier.setAccountId(accountId);
//    try {
//      return RuntimeServiceClient.stop(getAuthToken(), identifier);
//    } catch (RuntimeServiceException e) {
//      throw new IOException(e);
//    } catch (TException e) {
//      throw new IOException(e);
//    }
//  }
//
//  /**
//   * Status of a flow
//   *
//   * @param accountId the flow is associated with
//   * @param applicationId the flow belongs to
//   * @param entityId of the flow
//   * @param version of flow
//   * @return status of flow
//   * @throws java.io.IOException
//   */
//  public FlowStatus status(String accountId, String applicationId,
//                           String entityId, int version, EntityType type) throws IOException {
//    Preconditions.checkNotNull(applicationId);
//    Preconditions.checkNotNull(entityId);
//
//    // Connects to flow service.
//    connectToFlowService();
//
//    FlowIdentifier identifier
//      = new FlowIdentifier(accountId, applicationId, entityId, version);
//    identifier.setType(type);
//    identifier.setAccountId(accountId);
//    try {
//      FlowStatus status = RuntimeServiceClient.status(getAuthToken(), identifier);
//      return status;
//    } catch (RuntimeServiceException e) {
//      throw new IOException(e);
//    } catch (TException e) {
//      throw new IOException(e);
//    }
//  }
//
//  /**
//   * List of flows currently active for the account.
//   *
//   * @param accountId for which the active flow list should be returned.
//   * @return list of flows.
//   * @throws java.io.IOException
//   */
//  public List<ActiveFlow> getFlows(String accountId) throws IOException {
//    Preconditions.checkNotNull(accountId);
//
//    // Connects to flow service.
//    connectToFlowService();
//
//    try {
//      return RuntimeServiceClient.getFlows(accountId);
//    } catch (RuntimeServiceException e) {
//      throw new IOException(e);
//    } catch (TException e) {
//      throw new IOException(e);
//    }
//  }
//
//  /**
//   * Returns a definition of a flow.
//   *
//   * @param accountId the flow is associated with
//   * @param applicationId the flow belongs to
//   * @param entityId of the flow
//   * @param version of flow
//   * @return definition of flow
//   * @throws java.io.IOException
//   */
//  public String getFlowDefinition(String accountId, String applicationId,
//                                  String entityId, int version, EntityType type)
//    throws IOException {
//    Preconditions.checkNotNull(applicationId);
//    Preconditions.checkNotNull(accountId);
//    Preconditions.checkNotNull(entityId);
//
//    // Connects to flow service.
//    connectToFlowService();
//
//    FlowIdentifier identifier
//      = new FlowIdentifier(accountId, applicationId, entityId, version);
//    identifier.setType(type);
//    identifier.setAccountId(accountId);
//    try {
//      return RuntimeServiceClient.getFlowDefinition(identifier);
//    } catch (RuntimeServiceException e) {
//      throw new IOException(e);
//    } catch (TException e) {
//      throw new IOException(e);
//    }
//  }
//
//  /**
//   * Returns run history of a flow.
//   *
//   * @param accountId the flow is associated with.
//   * @param applicationId the flow belongs to
//   * @param entityId of the flow
//   * @param version of flow
//   * @return list of run records of a flow.
//   * @throws java.io.IOException
//   */
//  public List<FlowRunRecord> getHistory(String accountId, String applicationId,
//                                        String entityId, int version,
//                                        EntityType type)
//    throws IOException, RuntimeServiceException {
//    Preconditions.checkNotNull(applicationId);
//    Preconditions.checkNotNull(accountId);
//    Preconditions.checkNotNull(entityId);
//
//    // Connects to flow service.
//    connectToFlowService();
//
//    FlowIdentifier identifier
//      = new FlowIdentifier(accountId, applicationId, entityId, version);
//    identifier.setType(type);
//    identifier.setAccountId(accountId);
//    try {
//      return RuntimeServiceClient.getFlowHistory(identifier);
//    } catch (TException e) {
//      throw new IOException(e);
//    }
//  }
//
//  /**
//   * Returns counters associated with flow.
//   *
//   * @param accountId the flow is associated with.
//   * @param applicationId the flow belongs to
//   * @param flowId of the flow
//   * @param version of flow
//   * @param metrics list of metrics to be retrieved.
//   * @return list of {@Counter}s
//   * @throws java.io.IOException
//   */
//  public List<Counter> getCounters(String accountId, String applicationId,
//                                   String flowId, int version,
//                                   List<String> metrics) throws IOException {
//    // Connects to metrics service.
//    connectToMetricsService();
//
//    CounterRequest request = new CounterRequest(
//      new FlowArgument(accountId, applicationId, flowId)
//    );
//    request.setName(metrics);
//
//    try {
//      return metricsServiceClient.getCounters(request);
//    } catch (MetricsServiceException e) {
//      throw new IOException(e);
//    } catch (TException e) {
//      throw new IOException(e);
//    }
//  }
//
//  /**
//   * Returns timeseries for a given metric.
//   *
//   * @param accountId the flow is associated with.
//   * @param applicationId the flow belongs to
//   * @param flowId of the flow
//   * @param version of flow
//   * @param metrics list of metrics for whom the time series data needs to be
//   *                retrieved.
//   * @param offset offset from current time in seconds.
//   * @param level defines the level at which metric is requested.
//   * @return collection of {@link com.continuuity.metrics2.thrift.Points}
//   * @throws java.io.IOException
//   */
//  public Points getTimeseries(String accountId, String applicationId,
//                                  String flowId, int version,
//                                  List<String> metrics,
//                                  long offset,
//                                  MetricTimeseriesLevel level)
//    throws IOException {
//
//    // Connects to metrics service.
//    connectToMetricsService();
//
//    TimeseriesRequest request = new TimeseriesRequest();
//    request.setArgument(
//      new FlowArgument(accountId, applicationId, flowId)
//    );
//
//    request.setMetrics(metrics);
//    request.setLevel(level);
//    long timestamp = System.currentTimeMillis()/1000;
//    request.setStartts(timestamp - offset);
//    request.setEndts(timestamp);
//
//    try {
//      return metricsServiceClient.getTimeSeries(request);
//    } catch (MetricsServiceException e) {
//      throw new IOException(e);
//    } catch (TException e) {
//      throw new IOException(e);
//    }
//  }
//
//  /**
//   * Removes a flow.
//   *
//   * @param accountId the flow is associated with.
//   * @param applicationId the flow belongs to
//   * @param entityId of the flow
//   * @param version of flow
//   * @throws java.io.IOException
//   */
//  public void remove(String accountId, String applicationId, String entityId,
//                        int version, EntityType type) throws IOException {
//    connectToFARService(remoteDeploymentService);
//    try {
//      FlowIdentifier identifier = new FlowIdentifier(
//        accountId, applicationId, entityId, version);
//      identifier.setType(type);
//      identifier.setAccountId(accountId);
//      client.remove(getAuthToken(), identifier);
//    } catch (DeploymentServiceException e) {
//      throw new IOException(e);
//    } catch (TException e) {
//      throw new IOException(e);
//    }
//  }
//
//  /**
//   * Returns the auto-discovered service endpoint. Talks to service discovery
//   * system to find the endpoint that's available to connect to.
//   *
//   * @return pair of hostname and port the service is running on.
//   */
//  private final ImmutablePair<String, Integer>
//    getServiceEndpoint(String service) throws IOException {
//
//    // We use the random selection strategy.
//    ProviderStrategy<ServicePayload> strategy =
//      new RandomStrategy<ServicePayload>();
//
//    ServiceDiscoveryClient.ServiceProvider provider = null;
//    try {
//      provider = serviceDiscoveryClient.getServiceProvider(service);
//      if (provider.getInstances().size() < 1) {
//        return null;
//      }
//    } catch (Exception e) {
//      Log.error("Issue retrieving service list for service '{}' " +
//        "from service discovery.", service);
//      return null;
//    }
//
//    ServiceInstance<ServicePayload> instance = null;
//    try {
//      instance = strategy.getInstance(provider);
//      if (instance != null) {
//        return new ImmutablePair<String, Integer>(instance.getAddress(),
//                                                  instance.getPort());
//      }
//    } catch (Exception e) {
//      Log.error("Unable to retrieve an instance to connect to for " +
//        "service 'flow-monitor'. Reason : {}",
//        e.getMessage());
//    }
//    return null;
//  }
//
//  private AuthToken getAuthToken() {
//    return new AuthToken();
//  }
//
//  public String getUserAccountId() {
//    return System.getenv("USER");
//  }
//
//  boolean help = false;          // whether --help was there
//  boolean verbose = false;       // for debug output
//  String command = null;         // the command to run
//  String resource = null;        // the resource to be deployed.
//  String appid = null;           // the appid the flow belongs to.
//  String entityid = null;          // the id of a flow.
//  int version = -1;              // version of the flow.
//  String acctid = null;          // the account id to which flow is associated.
//  boolean list = false;          // the list enabled.
//  long offset = 30;              // Specifies offset from current in seconds.
//  String authToken = null;       // auth token.
//  MetricTimeseriesLevel level = MetricTimeseriesLevel.FLOW_LEVEL;
//                                 // Specifies the metric level.
//  List<String> metrics = Lists.newArrayList();
//                                 // Specifies the list of metrics.
//  EntityType type = EntityType.FLOW; // Specifies the type.
//
//
//  public boolean doMain(String[] args) throws Exception {
//    validateArguments(args);
//
//    // help then return.
//    if(help) {
//      return true;
//    }
//
//    // Deploys a flow
//    if("deploy".equals(command)) {
//      ImmutablePair<Boolean, String> status = deploy(new File(resource),
//                                                     acctid, appid);
//      if(!status.getFirst()) {
//        System.err.println(
//          String.format(
//            "Failed deploying the resource %s. Reason : %s",
//            resource, status.getSecond()
//          )
//        );
//        return false;
//      } else {
//        System.out.println("Successfully deployed.");
//      }
//    }
//
//    // Lists the current active flows.
//    if("list".equals(command)) {
//      List<ActiveFlow> allActiveEntities = getFlows(acctid);
//      List<ActiveFlow> relevantActiveEntities = Lists.newArrayList();
//      for (ActiveFlow activeFlow : allActiveEntities) {
//        if (activeFlow.getType()==type) relevantActiveEntities.add(activeFlow);
//      }
//
//      if(relevantActiveEntities.size() < 1) {
//        if (type==EntityType.FLOW)
//          System.out.println("Currently no flows are active for account " + acctid);
//        else
//          System.out.println("Currently no queries are active for account " + acctid);
//        return true;
//      }
//
//      int length = 166;
//      System.out.println(StringUtils.repeat("=", length));
//      System.out.println(
//        String.format(
//          "|  %-32s | %-32s | %20s | %20s | %20s | %10s | %-10s|",
//          "Application",
//          "Entity",
//          "Type",
//          "Last Started",
//          "Last Stopped",
//          "Runs",
//          "State"
//        )
//      );
//      System.out.println(StringUtils.repeat("=", length));
//
//      for(ActiveFlow activeFlow : relevantActiveEntities) {
//        Date lastStarted = new Date(activeFlow.getLastStarted());
//        Date lastStopped = new Date(activeFlow.getLastStopped());
//        System.out.println(
//          String.format(
//            "|  %-32s | %-32s | %20s | %20s | %20s | %10d | %-10s|",
//            activeFlow.getApplicationId(),
//            activeFlow.getFlowId(),
//            activeFlow.getType().name(),
//            new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(lastStarted),
//            new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(lastStopped),
//            activeFlow.getRuns(),
//            activeFlow.getCurrentState()
//          )
//        );
//      }
//      System.out.println(StringUtils.repeat("=", length));
//    }
//
//    // Starts a flow.
//    if("start".equals(command)) {
//      RunIdentifier identifier = start(acctid, appid, entityid, version, type);
//      System.out.println("Successfully started.");
//    }
//
//    // Stops a flow.
//    if("stop".equals(command)) {
//      stop(acctid, appid, entityid, version, type);
//      System.out.println("Successfully stopped.");
//    }
//
//    // Provides definition of a flow.
//    if("definition".equals(command)) {
//      System.out.println(getFlowDefinition(acctid, appid, entityid, version, type));
//    }
//
//    // Provides history of a flow.
//    if("history".equals(command)) {
//      List<FlowRunRecord> runRecords =
//        getHistory(acctid, appid, entityid, version, type);
//      if(runRecords.size() < 1) {
//        System.out.println("Currently no run history present");
//        return true;
//      }
//
//      System.out.println(StringUtils.repeat("=", 143));
//      System.out.println(
//        String.format(
//          "|  %-32s | %20s | %20s | %-10s|",
//          "Run ID",
//          "Start",
//          "End",
//          "State"
//        )
//      );
//      System.out.println(StringUtils.repeat("=", 143));
//
//      for(FlowRunRecord runRecord : runRecords) {
//        Date startTime = new Date(runRecord.getStartTime());
//        Date endTime = new Date(runRecord.getEndTime());
//        System.out.println(
//          String.format(
//            "|  %-32s | %20s | %20s | %-10s|",
//            runRecord.getRunId(),
//            new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(startTime),
//            new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(endTime),
//            runRecord.getEndStatus()
//          )
//        );
//      }
//    }
//
//    // Promote a flow to cloud.
//    if("promote".equals(command)) {
//      if(promote(acctid, appid, entityid, version, type)) {
//        System.out.println("Successfully deployed to cloud.");
//      } else {
//        System.err.println("Failed to deploy to cloud. Please see logs.");
//      }
//    }
//
//    // Get metrics and print them.
//    if("counters".equals(command)) {
//      List<Counter> counters = getCounters(
//        acctid, appid, entityid, version, metrics
//      );
//      if(counters.size() < 1) {
//        System.out.println("No metrics available.");
//      } else {
//        System.out.println(String.format(
//          "%20s : %-45s   %-8s", "Qualifier", "Metric", "Value")
//        );
//        for(Counter counter : counters ) {
//          System.out.println(
//            String.format(
//              "%20s : %-45s = %6.2f",
//              counter.getQualifier(),
//              counter.getName(),
//              counter.getValue()
//            )
//          );
//        }
//      }
//    }
//
//    // Get time series data.
//    if("timeseries".equals(command)) {
//      Points datapoints = getTimeseries(
//        acctid, appid, entityid, version, metrics, offset, level
//      );
//      Map<String, List<Point>> points = datapoints.getPoints();
//      if(points.size() > 0) {
//        System.out.println(StringUtils.repeat("=", 80));
//        System.out.println(
//          String.format("| %-10s | %-32s | %-28s |",
//                        "Timestamp", "Metric", "Value")
//        );
//        System.out.println(StringUtils.repeat("=", 80));
//        for(Map.Entry<String, List<Point>> entry : points.entrySet()) {
//          String metric = entry.getKey();
//          for(Point point : entry.getValue()) {
//            System.out.println(String.format(
//              "| %d | %-32s | %-28s |",
//              point.getTimestamp(),
//              metric,
//              String.format("%3.2f", point.getValue())
//            ));
//          }
//        }
//        System.out.println(StringUtils.repeat("=", 80));
//      } else {
//        System.out.println("No datapoints.");
//      }
//    }
//
//    // Remove a flow.
//    if("remove".equals(command)) {
//      remove(acctid, appid, entityid, version, type);
//      System.out.println("Successfully removed.");
//    }
//
//    // stop all flow and queries
//    if("stop-all".equals(command)) {
//      // Connects to flow service.
//      connectToFlowService();
//      try {
//        RuntimeServiceClient.stopAll(acctid);
//      } catch (RuntimeServiceException e) {
//        throw new IOException(e);
//      } catch (TException e) {
//        throw new IOException(e);
//      }
//      System.out.println("All flows and queries stopped.");
//    }
//
//    // Remove all flows and queries
//    if("remove-all".equals(command)) {
//      connectToFARService(remoteDeploymentService);
//      try {
//        client.removeAll(getAuthToken(), acctid);
//      } catch (DeploymentServiceException e) {
//        throw new IOException(e);
//      } catch (TException e) {
//        throw new IOException(e);
//      }
//      System.out.println("All flows and queries removed.");
//    }
//
//    // Reset the account
//    if("reset".equals(command)) {
//      // Connects to flow service.
//      connectToFARService(remoteDeploymentService);
//      try {
//        client.reset(getAuthToken(), acctid);
//      } catch (DeploymentServiceException e) {
//        throw new IOException(e);
//      } catch (TException e) {
//        throw new IOException(e);
//      }
//      System.out.println("Account reset successful.");
//    }
//
//    return true;
//  }
//
//  /**
//   * Print the usage statement and return null (or empty string if this is not
//   * an error case). See getValue() for an explanation of the return type.
//   *
//   * @param error indicates whether this was invoked as the result of an error
//   * @throws IllegalArgumentException in case of error, an empty string in case
//   * of success
//   */
//  void usage(boolean error) {
//    PrintStream out = (error ? System.err : System.out);
//    String name = "flow-client";
//    if (System.getProperty("script") != null) name = System.getProperty("script").replaceAll("[./]", "");
//    Copyright.print(out);
//    out.println("Usage: ");
//    out.println("  " + name + " deploy     --resource <jar> --account <account> --application <app> --type FLOW|QUERY");
//    out.println("  " + name + " list       --account <account>");
//    out.println("  " + name + " start      --application <app> --entity <id> --type FLOW|QUERY");
//    out.println("  " + name + " stop       --application <app> --entity <id> --type FLOW|QUERY");
//    out.println("  " + name + " stop-all");
//    out.println("  " + name + " definition --application <app> --entity <id> --type FLOW|QUERY");
//    out.println("  " + name + " history    --application <app> --entity <id> --type FLOW|QUERY");
//    out.println("  " + name + " counters   --application <app> " +
//                  "--entity <id> --type FLOW|QUERY");
//    out.println("  " + name + " timeseries --application <app> " +
//                  "--entity <id> --type FLOW|QUERY \n                  --metric <name> [ --offset <seconds>] " +
//                  "[ --level [ACCOUNT|APP|FLOW] ]");
//    out.println("  " + name + " remove     --application <app> --entity <id> --type FLOW|QUERY ");
//    out.println("  " + name + " remove-all");
//    out.println("  " + name + " reset");
//    out.println("Additional options:");
//    out.println("  --account <acctid>      " +
//                  "To specify the account id (default: demo)");
//    out.println("  --application <appid>   " +
//                  "To specify the application id");
//    out.println("  --entity <id>           " +
//                  "To specify the entity id");
//    out.println("  --type <flow|query>     " +
//                  "To specify the entity type flow or query (default: flow).");
//    out.println("  --resource <jar>        " +
//      "To specify jar file to be deployed.");
//    out.println("  --version <number>      " +
//      "To specify the version of flow to be used(default: -1 picks the latest)");
//    out.println("  --metric <name>         " +
//                  "To specify the name of metric");
//    out.println("  --offset <offset>       " +
//                  "To specify an offset from current time in seconds");
//    out.println("  --verbose               To see more verbose output");
//    out.println("  --help                  To print this message");
//    if(error) {
//      throw new UsageException();
//    }
//    return;
//  }
//
//  /**
//   * Print an error message followed by the usage statement
//   * @param errorMessage the error message
//   */
//  void usage(String errorMessage) {
//    if (errorMessage != null) System.err.println("Error: " + errorMessage);
//    usage(true);
//  }
//
//  /**
//   * Parse the command line arguments
//   */
//  void parseArguments(String[] args) {
//    if (args.length == 0) {
//      usage(false);
//      help = true;
//      return;
//    }
//    if ("--help".equals(args[0])) {
//      usage(false);
//      help = true;
//      return;
//    } else {
//      command = args[0];
//    }
//
//    if("list".equals(command)) {
//      list = true;
//    }
//
//    // go through all the arguments
//    for (int pos = 1; pos < args.length; pos++) {
//      String arg = args[pos];
//      if ("--resource".equals(arg)) {
//        if (++pos >= args.length) usage(true);
//        resource = args[pos];
//      } else if ("--application".equals(arg)) {
//        if (++pos >= args.length) usage(true);
//        appid = args[pos];
//      } else if ("--entity".equals(arg)) {
//        if (++pos >= args.length) usage(true);
//        entityid = args[pos];
//      } else if ("--type".equals(arg)) {
//        if (++pos >= args.length) usage(true);
//        String typeStr = args[pos];
//        if(typeStr.equals("flow")) {
//          type = EntityType.FLOW;
//        } else if(typeStr.equals("query")){
//          type = EntityType.QUERY;
//        }
//      } else if ("--account".equals(arg)) {
//        if (++pos >= args.length) usage(true);
//        acctid = args[pos];
//      } else if("--metric".equals(arg)) {
//        if (++pos >= args.length) usage(true);
//        String metric = args[pos];
//        metrics.add(metric);
//      } else if("--level".equals(arg)) {
//        if (++pos >= args.length) usage(true);
//        if("ACCT".equals(args[pos])) {
//          level = MetricTimeseriesLevel.ACCOUNT_LEVEL;
//        } else if("APP".equals(args[pos])) {
//          level = MetricTimeseriesLevel.APPLICATION_LEVEL;
//        } else if("FLOW".equals(args[pos])) {
//          level = MetricTimeseriesLevel.FLOW_LEVEL;
//        }
//      } else if("--offset".equals(arg)) {
//        if (++pos >= args.length) usage(true);
//        try {
//          offset = Integer.valueOf(args[pos]);
//        } catch(NumberFormatException e) {
//          usage(true);
//        }
//      } else if ("--version".equals(arg)) {
//        if (++pos >= args.length) usage(true);
//        try {
//          version = Integer.valueOf(args[pos]);
//        } catch (NumberFormatException e) {
//          usage(true);
//        }
//      } else if ("--verbose".equals(arg)) {
//        verbose = true;
//      } else if ("--help".equals(arg)) {
//        help = true;
//        usage(false);
//        return;
//      } else {  // unkown argument
//        usage(true);
//      }
//    }
//  }
//
//  static List<String> supportedCommands =
//    Arrays.asList(
//        "deploy", "list", "start", "stop", "stop-all", "definition", "history",
//        "counters", "timeseries", "promote", "remove", "remove-all", "reset");
//
//  void validateArguments(String[] args) {
//    // first parse command arguments
//    parseArguments(args);
//
//    // default account id if not defined.
//    if(acctid == null) {
//      acctid = OperationContext.DEFAULT_ACCOUNT_ID;
//    }
//
//    if (help) return;
//    if(list) return;
//
//    // first validate the command
//    if (!supportedCommands.contains(command))
//      usage("Unsupported command '" + command + "'.");
//
//    if(resource == null && "deploy".equals(command)) {
//      usage("--resource should be specified with command 'deploy'");
//    }
//
//    if(list) {
//      if(resource != null || appid != null || entityid != null) {
//        usage("no options need to specified for list");
//      }
//    }
//
//    if("start".equals(command) || "stop".equals(command) ||
//       "definition".equals(command) || "history".equals(command) ||
//       "metrics".equals(command) || "remove".equals(command)) {
//      if(appid == null || entityid == null) {
//        usage(command + " requires --appid and --entityid");
//      }
//    }
//  }
//
//  /**
//   * Static main of flow client.
//   */
//  public static void main(String[] args) {
//    try {
//      new AppFabricClient(CConfiguration.create()).doMain(args);
//    } catch (UsageException e) {
//      System.exit(1);
//    } catch (Exception e) {
//      System.out.println(e.getMessage());
//      System.exit(1);
//    }
//  }
//
//}