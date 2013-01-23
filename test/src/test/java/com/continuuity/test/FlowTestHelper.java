package com.continuuity.test;

import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.BatchCollector;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.DataLib;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.WriteOperation;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;
import com.continuuity.api.flow.MasterRunnerException;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FailureHandlingPolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.SinkFlowlet;
import com.continuuity.api.flow.flowlet.SourceFlowlet;
import com.continuuity.api.flow.flowlet.StreamsConfigurator;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.query.QueryProvider;
import com.continuuity.api.query.QuerySpecifier;
import com.continuuity.common.builder.Builder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.common.zookeeper.OnDemandInMemoryZookeeperServer;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.flow.common.BufferFileInputStream;
import com.continuuity.flow.common.GenericDataSetRegistry;
import com.continuuity.flow.common.ResourceStatus;
import com.continuuity.flow.common.StreamType;
import com.continuuity.flow.definition.api.FlowDefinition;
import com.continuuity.flow.definition.api.FlowStreamDefinition;
import com.continuuity.flow.definition.api.FlowletDefinition;
import com.continuuity.flow.definition.api.QueryDefinition;
import com.continuuity.flow.definition.impl.SpecifierFactory;
import com.continuuity.flow.flowlet.runner.FlowletExecutionContext;
import com.continuuity.flow.flowlet.runner.FlowletRef;
import com.continuuity.flow.flowlet.runner.FlowletRunner;
import com.continuuity.flow.flowlet.runner.FlowletRunnerFactory;
import com.continuuity.flow.flowlet.runner.InMemoryFlowletRunner;
import com.continuuity.flow.flowlet.runner.InMemoryQueryRunner;
import com.continuuity.flow.manager.server.ResourceValidator;
import com.continuuity.flow.manager.server.StreamNamer;
import com.continuuity.flow.manager.server.internal.FARServiceHandler;
import com.continuuity.flow.manager.server.internal.FlowResourceValidator;
import com.continuuity.flow.manager.server.internal.Storage;
import com.continuuity.flow.manager.server.internal.StreamNamerImpl;
import com.continuuity.flow.manager.stubs.EntityType;
import com.continuuity.flow.manager.stubs.FlowIdentifier;
import com.continuuity.flow.manager.stubs.ResourceIdentifier;
import com.continuuity.flow.manager.stubs.ResourceInfo;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.curator.framework.CuratorFramework;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

/**
 *
 */
@SuppressWarnings("unchecked")
public class FlowTestHelper {
  private static final Logger Log = LoggerFactory.getLogger(FlowTestHelper.class);
  private static ExecutorService executor = Executors.newCachedThreadPool();
  private static Storage storage = null;
  private static FARServiceHandler farHandler = null;
  private static String account = OperationContext.DEFAULT_ACCOUNT_ID;

  public static FlowDefinition createFlow(Flow flow, OperationExecutor opex) {
    MetadataService mds = new MetadataService(opex);

    /** This is what we will do */
    FlowSpecifier specifier = SpecifierFactory.newFlowSpecifier();

    /** This what user will specify. */
    flow.configure(specifier);

    /** This is what we will do */
    Builder<FlowDefinition> flowDefinitionBuilder = (Builder<FlowDefinition>) specifier;
    FlowDefinition definition = flowDefinitionBuilder.build();

    ResourceValidator validator = new FlowResourceValidator();
    ImmutablePair<Boolean, String> validationStatus = validator.validate(definition, null);
    if(validationStatus.getFirst() != true) {
      Log.error("Failed validation of flow. Reason {}.", validationStatus.getSecond());
      return null;
    }

    List<String> streams = Lists.newArrayList();
    for(FlowStreamDefinition streamDefinition : definition.getFlowStreams()) {
      streams.add(streamDefinition.getName());
    }
    com.continuuity.metadata.thrift.Flow flowReg = new
      com.continuuity.metadata.thrift.Flow(definition.getMeta().getName(),
                                           definition.getMeta().getApp());
      flowReg.setName(definition.getMeta().getName());
      flowReg.setStreams(streams);
      flowReg.setDatasets(new ArrayList<String>());

    try {
      mds.createFlow(account, flowReg);
    } catch (MetadataServiceException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }

    StreamNamer qualifier = new StreamNamerImpl();
    boolean status = qualifier.name(account, definition);
    if(! status) {
      Log.error("Failed qualifying streams with queue names.");
      return null;
    }
    return definition;
  }

  private static ImmutablePair<Map<String, FlowletTestInfo>, Map<String, FlowletRunner>> createInstrumentedFlowletRunners(
    FlowDefinition definition, OperationExecutor executor, CConfiguration configuration) throws MasterRunnerException {

    Map<String, FlowletTestInfo> instrumentedFlowletHolders = Maps.newHashMap();
    Map<String, FlowletRunner> flowletRunners = Maps.newHashMap();
    Collection<? extends FlowletDefinition> flowlets = definition.getFlowlets();

    for(FlowletDefinition flowlet : flowlets) {
      try {
        /** Create an instance of instrumented flowlet */
        InstrumentedFlowletHolder instrumentedFlowletHolder = createInstrumentedFlowlet(flowlet.getClazz());
        instrumentedFlowletHolders.put(flowlet.getName(), instrumentedFlowletHolder.getFlowletTestInfo());

        /** Construct the Flowlet context that will be passed to all parts of the flowlet */
        FlowletExecutionContext context = new FlowletExecutionContext(definition.getMeta().getName(), flowlet.getName(), false);
        final Map<String, URI> myInputStreams = Maps.newHashMap();
        final Map<String, URI> myOutputStreams = Maps.newHashMap();
        for(Map.Entry<String, ImmutablePair<URI, StreamType>> entry : definition.getFlowletStreams(flowlet.getName()).entrySet()) {
          if(entry.getValue().getSecond() == StreamType.IN) {
            myInputStreams.put(entry.getKey(), entry.getValue().getFirst());
          } else {
            myOutputStreams.put(entry.getKey(), entry.getValue().getFirst());
          }
        }

        FlowIdentifier identifier = new FlowIdentifier();
        identifier.setAccountId("demo");
        identifier.setApplicationId(definition.getMeta().getApp());
        identifier.setFlowId(definition.getMeta().getName());
        identifier.setVersion(1);

        context.setFlowIdentifier(identifier);
        context.setOperationExecutor(executor);
        configuration.setBoolean("execution.mode.singlenode", true);
        context.setConfiguration(configuration);
        Class<?> clazz = Class.forName(flowlet.getClassName());
        context.setSource(isSource((Class<? extends Flowlet>)clazz));
        context.setInputStreamURIs(myInputStreams);
        context.setOutputStreamURIs(myOutputStreams);

        FlowletRef ref = FlowletRunnerFactory.createFlowletReference(instrumentedFlowletHolder.getFlowlet(), context);
        FlowletRunner runner = new InMemoryFlowletRunner(ref, context);

        if(runner == null) {
          if(!instrumentedFlowletHolder.getFlowletTestInfo().getExceptions().isEmpty()) {
            throw new MasterRunnerException("Unable to create flowlet runner for flowlet " + flowlet.getName(), instrumentedFlowletHolder.getFlowletTestInfo().getExceptions().get(0));
          } else {
            throw new MasterRunnerException("Unable to create flowlet runner for flowlet " + flowlet.getName());
          }
        }
        flowletRunners.put(flowlet.getName(), runner);

      } catch (ClassNotFoundException e) {
        Log.error("Class not found. Reason : {}.", e.getMessage());
        throw new MasterRunnerException(e);
      }
    }

    return new ImmutablePair<Map<String, FlowletTestInfo>, Map<String, FlowletRunner>>(instrumentedFlowletHolders, flowletRunners);
  }

  /**
   * Starts all the flowlets
   *
   * @param flowletRunners
   * @return
   */
  private static boolean startFlowlets(Map<String, FlowletRunner> flowletRunners) {
    Log.info("Starting flow ...");
    try {
      for(Map.Entry<String, FlowletRunner> runner : flowletRunners.entrySet()) {
        runner.getValue().start(executor);
      }
    } catch (Exception e) {
      Log.error("Failed to start flow. Reason : {}", e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Stop all the flowlets
   *
   * @param flowletRunners
   * @return
   */
  private static boolean stopFlowlets(Map<String, FlowletRunner> flowletRunners) {
    try {
      for(Map.Entry<String, FlowletRunner> runner : flowletRunners.entrySet()) {
        runner.getValue().stop("Normal shutdown flowlet " + runner.getValue().getName());
      }
    } catch (Exception e) {
      Log.error("Failed to start flow. Reason : {}", e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Returns true if flowlet is a source.
   *
   * @param flowlet class of the flowlet being instantiated.
   * @return true if {@link com.continuuity.api.flow.flowlet.SourceFlowlet}; false otherwise.
   */
  private static final boolean isSource(Class<? extends Flowlet> flowlet) {
    return SourceFlowlet.class.isAssignableFrom(flowlet);
  }

  /**
   * Configures and instantiates the specified flow and runs it in-memory.
   * 
   * Returns a handle that contains success/failure code and reason.
   * 
   * If successful, contains a reference to the flowlet runners.  Handle must
   * @param flow
   * @return
   */
  public static TestFlowHandle startFlow(Flow flow, CConfiguration conf,
      OperationExecutor executor) {

    // Generate a flow definition
    FlowDefinition flowDefinition = createFlow(flow, executor);
    if (flowDefinition == null) {
      return new TestFlowHandle("Failed to generate flow definition");
    }
    
    // Create instrumented flowlets and flowlet runners
    ImmutablePair<Map<String, FlowletTestInfo>, Map<String, FlowletRunner>> instrmentedFlowletRunners = null;
    try {
      instrmentedFlowletRunners = createInstrumentedFlowletRunners(flowDefinition, executor, conf);
    } catch (MasterRunnerException e) {
      Log.error("Failed to create flow.", e);
    }
    if (instrmentedFlowletRunners == null || instrmentedFlowletRunners.getSecond().isEmpty()) {
      return new TestFlowHandle("Failed to create flowlet runners");
    }
    
    // Start flowlets
    if (!startFlowlets(instrmentedFlowletRunners.getSecond())) {
      return new TestFlowHandle("Failed to start flowlet runners");
    }
    
    // Started!
    return new TestFlowHandle(instrmentedFlowletRunners.getSecond(), instrmentedFlowletRunners.getFirst());
  }

  /**
   * Stops the in-memory flow
   *
   * @param flow
   * @return
   */
  public static boolean stopFlow(TestFlowHandle flow) {
    assertTrue(flow.isSuccess());
    return stopFlowlets(flow.flowletRunners);
  }
  
  /**
   * Handle of a running Flow.
   */
  public static class TestFlowHandle {
    Map<String, FlowletRunner> flowletRunners;
    Map<String, FlowletTestInfo> flowletTestInfoMap = ImmutableMap.of();
    final boolean success;
    String reason;
    boolean stopped;
    
    /**
     * Constructs a successful flow start.
     * @param flowletRunners all flowlet runners of flow
     */
    TestFlowHandle(Map<String, FlowletRunner> flowletRunners, Map<String, FlowletTestInfo> flowletTestInfoMap) {
      this.flowletRunners = flowletRunners;
      this.flowletTestInfoMap = flowletTestInfoMap;
      this.success = true;
      this.stopped = false;
    }
    
    /**
     * Constructs a failed flow start with a reason.
     * @param reason reason flow did not start
     */
    TestFlowHandle(String reason) {
      this.reason = reason;
      this.success = false;
      this.stopped = true;
    }
    
    /**
     * Returns true if the flow was started successfully.
     * <p>
     * if the flow was not successful, use {@link #getReason()} for an
     * explanation of why.
     * @return true if flow started successfully, false if not.
     */
    public boolean isSuccess() {
      return this.success;
    }
    
    /**
     * Returns the reason the flow was not started, if it was not successful.
     * @return the reason the flow did not start, or null if it started
     */
    public String getReason() {
      return this.reason;
    }
    
    /**
     * Returns true if the flow and all flowlets are currently running.
     * @return true if flow is running, false if not
     */
    public boolean isRunning() {
      if (!isSuccess() || stopped) return false;
      for (Map.Entry<String,FlowletRunner> runner : flowletRunners.entrySet()) {
        if (!runner.getValue().ruok()) {
          Log.warn("Flowlet " + runner.getKey() + " not running properly");
          return false;
        }
      }
      return true;
    }

    public void stop() {
      if (stopped || !isSuccess()) return;
      stopFlowlets(flowletRunners);
      stopped = true;
    }

    public <T extends FlowletTestInfo> T getFlowletTestInfo(String flowletName) {
      return (T) flowletTestInfoMap.get(flowletName);
    }
  }

  public static FARServiceHandler getFARServiceHandler(OperationExecutor opex,
      CuratorFramework client, CConfiguration configuration) throws Exception {
    if(farHandler == null) {
      storage = new Storage(Storage.localFileSystem(configuration), configuration);
      farHandler = new FARServiceHandler(client, storage, opex, configuration);
    }
    return farHandler;
  }

  public static ResourceIdentifier deploy(File far) throws Exception {
    if(farHandler == null) {
      throw new Exception("FAR Handler has not been initialized. Call #getFARServiceHandler before calling deploy");
    }

    /** Register resource */
    ResourceInfo info = new ResourceInfo("demo", "demo", far.getName(),
      (int) far.getTotalSpace(), far.lastModified());
    info.setType(EntityType.FLOW);
    ResourceIdentifier id = farHandler.registerResource(info);
    Assert.assertNotNull(id);

    /** Get the status of resource */
    ResourceStatus status = farHandler.getResourceStatus(id);

    /** Send jar to resource */
    BufferFileInputStream is = new BufferFileInputStream(far.getAbsolutePath(), 100*1024);

    while(true) {
      byte[] toSubmit = is.read();
      if(toSubmit.length==0) break;
      farHandler.acceptChunk(id, ByteBuffer.wrap(toSubmit));
      status = farHandler.getResourceStatus(id);
      Assert.assertTrue(status.getCode() == ResourceStatus.UPLOADING);
    }

    is.close();

    /** Finalize the resource deployment */
    farHandler.deploy(id);
    status = farHandler.getResourceStatus(id);

    /** Wait for validation of flow */
    Thread.sleep(2000);

    return id;
  }

  public static TestQueryHandle startQuery(QueryProvider queryProvider, CConfiguration configuration,
                                           OperationExecutor operationExecutor) {
    // Get the configuration for the query
    QuerySpecifier querySpecifier =  SpecifierFactory.newQuerySpecifier();
    queryProvider.configure(querySpecifier);
    QueryDefinition queryDefinition = ((Builder<QueryDefinition>) querySpecifier).build();


    FlowIdentifier identifier = new FlowIdentifier();
    identifier.setAccountId("demo");
    identifier.setApplicationId(queryDefinition.getServiceName());
    identifier.setFlowId(queryDefinition.getServiceName());
    identifier.setVersion(1);

    // Start Zookeeper server
    InMemoryZookeeper zookeeperServer = null;
    try {
      zookeeperServer = OnDemandInMemoryZookeeperServer.getSingletonInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Setup configuration
    configuration.setBoolean("execution.mode.singlenode", true);
    configuration.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zookeeperServer.getConnectionString());

    // Create the context
    FlowletExecutionContext context = new FlowletExecutionContext(queryDefinition.getServiceName(),
                                                                  queryDefinition.getQueryProvider(), false);
    context.setFlowIdentifier(identifier);
    context.setOperationExecutor(operationExecutor);
    context.setConfiguration(configuration);

    // Create in memory query runner
    FlowletRunner queryRunner = new InMemoryQueryRunner(queryProvider, context);
    try {
      queryRunner.start(executor);
    } catch (Exception e) {
      Log.error("Failed to start QueryProvider.", e);
      return new TestQueryHandle("Failed to start QueryProvider");
    }

    Map<String, FlowletRunner> runners = ImmutableMap.of(queryDefinition.getServiceName(), queryRunner);
    return new TestQueryHandle(runners);
  }

  /**
   * Collector class that applies operations synchronously.
   *  TODO: needs to be replaced after DataSet redesign
   */
  public static class SyncCollector implements BatchCollector {
    private final OperationExecutor opex;

    public SyncCollector(OperationExecutor opex) {
      this.opex = opex;
    }

    @Override
    public void add(WriteOperation writeOperation) {
      try {
        // TODO: do we need other operation contexts?
        opex.execute(OperationContext.DEFAULT, Lists.newArrayList(writeOperation));
      } catch (OperationException e) {
        // TODO: add exception to Exception list
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }

  /**
   * DataSet registry that also sets collector into the DataSet.
   * TODO: Needs to go away on redesign of DataSets
   */
  public static class TestDataSetRegistry extends GenericDataSetRegistry {
    private OperationExecutor operationExecutor;

    public TestDataSetRegistry(OperationExecutor executor, DataFabric dataFabric,
                               BatchCollectionRegistry registry, String accountId) {
      super(executor, dataFabric, registry, accountId);
      this.operationExecutor = executor;
    }

    public TestDataSetRegistry(OperationExecutor executor, DataFabric dataFabric, BatchCollectionRegistry registry,
                               String entityId, String accountId, String applicationId) {
      super(executor, dataFabric, registry, entityId, accountId, applicationId);
    }

    @Override
    public DataLib registerDataSet(DataLib dataLib) {
      DataLib lib = super.registerDataSet(dataLib);
      dataLib.setCollector(new SyncCollector(operationExecutor));
      return lib;
    }
  }

  /**
   * Simple wrapper around TestFlowHandle for Queries
   */
  public static class TestQueryHandle extends TestFlowHandle {
    public TestQueryHandle(Map<String, FlowletRunner> flowletRunners) {
      // TODO: add test info for queries
      super(flowletRunners, null);
    }

    public TestQueryHandle(String reason) {
      super(reason);
    }
  }

  static class InstrumentedFlowletHolder {
    private final Flowlet flowlet;
    private final FlowletTestInfo flowletTestInfo;

    InstrumentedFlowletHolder(Flowlet flowlet, FlowletTestInfo flowletTestInfo) {
      this.flowlet = flowlet;
      this.flowletTestInfo = flowletTestInfo;
    }

    public Flowlet getFlowlet() {
      return flowlet;
    }

    public FlowletTestInfo getFlowletTestInfo() {
      return flowletTestInfo;
    }
  }

  static <T extends Flowlet> InstrumentedFlowletHolder createInstrumentedFlowlet(Class<T> flowletClassName) {
    Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(flowletClassName);

    if(ComputeFlowlet.class.isAssignableFrom(flowletClassName)) {
      ComputeFlowletTestInfo computeFlowletTestInfo = new ComputeFlowletTestInfo();
      enhancer.setCallback(new ComputeFlowletMethodInterceptor(computeFlowletTestInfo));
      InstrumentedFlowletHolder instrumentedFlowletHolder = new InstrumentedFlowletHolder(
        (T) enhancer.create(), computeFlowletTestInfo);
      return instrumentedFlowletHolder;
    } else if(SourceFlowlet.class.isAssignableFrom(flowletClassName)) {
      FlowletTestInfo flowletTestInfo = new FlowletTestInfo();
      enhancer.setCallback(new FlowletMethodInterceptor(flowletTestInfo));
      InstrumentedFlowletHolder instrumentedFlowletHolder = new InstrumentedFlowletHolder(
        (T) enhancer.create(), flowletTestInfo);
      return instrumentedFlowletHolder;
    } else if(SinkFlowlet.class.isAssignableFrom(flowletClassName)) {
      FlowletTestInfo flowletTestInfo = new FlowletTestInfo();
      enhancer.setCallback(new FlowletMethodInterceptor(flowletTestInfo));
      InstrumentedFlowletHolder instrumentedFlowletHolder = new InstrumentedFlowletHolder(
        (T) enhancer.create(), flowletTestInfo);
      return instrumentedFlowletHolder;
    } else {
      throw new IllegalArgumentException(String.format("Don't know how to instrument flowlet class %s!", flowletClassName.getCanonicalName()));
    }
  }

  public static class FlowletTestInfo {
    private boolean initialized = false;
    private boolean configured = false;
    private boolean destroyed = false;
    private List<Throwable> exceptions = Lists.newArrayList();

    public boolean isInitialized() {
      return initialized;
    }

    public boolean isConfigured() {
      return configured;
    }

    public boolean isDestroyed() {
      return destroyed;
    }

    public List<Throwable> getExceptions() {
      return exceptions;
    }

    void setInitialized(boolean initialized) {
      this.initialized = initialized;
    }

    void setConfigured(boolean configured) {
      this.configured = configured;
    }

    void setDestroyed(boolean destroyed) {
      this.destroyed = destroyed;
    }

    void addException(Throwable e) {
      exceptions.add(e);
    }
  }

  static class FlowletProxy extends Flowlet {
    private final FlowletTestInfo flowletTestInfo;

    FlowletProxy(FlowletTestInfo flowletTestInfo) {
      this.flowletTestInfo = flowletTestInfo;
    }

    @Override
    public void configure(StreamsConfigurator configurator) {
      flowletTestInfo.setConfigured(true);
    }

    @Override
    public void initialize() {
      flowletTestInfo.setInitialized(true);
    }

    @Override
    public void destroy() {
      flowletTestInfo.setDestroyed(true);
    }
  }

  static class FlowletMethodInterceptor<T> implements MethodInterceptor {
    private final FlowletTestInfo flowletTestInfo;
    private final T flowletProxy;

    public FlowletMethodInterceptor(FlowletTestInfo flowletTestInfo, T flowletProxy) {
      this.flowletTestInfo = flowletTestInfo;
      this.flowletProxy = flowletProxy;
    }

    public FlowletMethodInterceptor(FlowletTestInfo flowletTestInfo) {
      this.flowletTestInfo = flowletTestInfo;
      this.flowletProxy = (T) new FlowletProxy(flowletTestInfo);
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
      doBookKeeping(flowletProxy, method, args);

      // Record all exceptions thrown
      try {
        return proxy.invokeSuper(obj, args);
      } catch(Throwable e) {
        flowletTestInfo.addException(e);
        throw e;
      }
    }

    protected void doBookKeeping(T flowletProxy, Method method, Object[] args)
      throws Throwable {
      try {
        Method flowletProxyMethod = flowletProxy.getClass().getDeclaredMethod(method.getName(), method.getParameterTypes());
        flowletProxyMethod.invoke(flowletProxy, args);
      } catch(NoSuchMethodException e) {
        // Method not found in the flowlet interface, nothing to do for book-keeping
      } catch (Throwable t) {
        Log.error(String.format("Exception during book-keeping of flowlet method %s", method.getName()), t);
        throw t;
      }
    }

  }

  static class ComputeFlowletMethodInterceptor extends FlowletMethodInterceptor<ComputeFlowletProxy> {

    public ComputeFlowletMethodInterceptor(ComputeFlowletTestInfo computeFlowletTestInfo) {
      super(computeFlowletTestInfo, new ComputeFlowletProxy(computeFlowletTestInfo));
    }
  }

  public static class ComputeFlowletTestInfo extends FlowletTestInfo {
    private int numProcessed = 0;
    private int numSuccessful = 0;
    private int numFailures = 0;
    private final List<Tuple> processedTuples = Lists.newArrayList();
    private final List<Tuple> successfulTuples = Lists.newArrayList();
    private final List<Tuple> failedTuples = Lists.newArrayList();

    public int getNumProcessed() {
      return numProcessed;
    }

    public int getNumSuccessful() {
      return numSuccessful;
    }

    public int getNumFailures() {
      return numFailures;
    }

    public List<Tuple> getProcessedTuples() {
      return processedTuples;
    }

    public List<Tuple> getSuccessfulTuples() {
      return successfulTuples;
    }

    public List<Tuple> getFailedTuples() {
      return failedTuples;
    }

    int incNumProcessed() {
      return ++numProcessed;
    }

    int incNumSuccessful() {
      return ++numSuccessful;
    }

    int incNumFailures() {
      return ++numFailures;
    }

    void addProcessedTuple(Tuple tuple) {
      processedTuples.add(tuple);
    }

    void addSuccessfulTuple(Tuple tuple) {
      successfulTuples.add(tuple);
    }

    void addFailedTuple(Tuple tuple) {
      failedTuples.add(tuple);
    }
  }

  static class ComputeFlowletProxy extends ComputeFlowlet {
    private final ComputeFlowletTestInfo computeFlowletTestInfo;
    private final FlowletProxy flowletProxy;

    ComputeFlowletProxy(ComputeFlowletTestInfo computeFlowletTestInfo) {
      this.computeFlowletTestInfo = computeFlowletTestInfo;
      this.flowletProxy = new FlowletProxy(computeFlowletTestInfo);
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      computeFlowletTestInfo.incNumSuccessful();
      computeFlowletTestInfo.addSuccessfulTuple(tuple);
    }

    @Override
    public FailureHandlingPolicy onFailure(Tuple tuple, TupleContext context, FailureReason reason) {
      computeFlowletTestInfo.incNumFailures();
      computeFlowletTestInfo.addFailedTuple(tuple);
      return null;
    }

    @Override
    public void process(Tuple tuple, TupleContext context, OutputCollector collector) {
      computeFlowletTestInfo.incNumProcessed();
      computeFlowletTestInfo.addProcessedTuple(tuple);
    }

    @Override
    public void configure(StreamsConfigurator configurator) {
      flowletProxy.configure(configurator);
    }

    @Override
    public void initialize() {
      flowletProxy.initialize();
    }

    @Override
    public void destroy() {
      flowletProxy.destroy();
    }
  }
}
