package com.continuuity.test;

import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.DataSetRegistry;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.query.QueryProvider;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServicePayload;
import com.continuuity.common.logging.LocalLogDispatcher;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.service.ServerException;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.ClearFabric.ToClear;
import com.continuuity.data.operation.SimpleBatchCollectionClient;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.flow.common.FlowLogTag;
import com.continuuity.flow.definition.api.FlowDefinition;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.FlowletContextImpl;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.continuuity.gateway.Gateway;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.test.FlowTestHelper.TestDataSetRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Base class for running AppFabric and Data Fabric in-memory unit tests.
 * <p/>
 * To utilize this class, simply create a junit4 class that extends this class:
 * <pre>
 *   public class TestSimpleReadWrite extends AppFabricTestBase {
 *
 *     &#64;Test
 *     public void testMyFlow() throws Exception {
 *       Flow flow = new MyFlow();
 *       assertTrue(verifyFlow(flow));
 *       TestFlowHandle flowHandle = startFlow(flow);
 *       assertTrue(flowHandle.getReason(), flowHandle.isSuccess());
 *       // Flow started successfully
 *       flowHandle.stop();
 *     }
 *
 *   }
 * </pre>
 * <p/>
 * Within your tests, you can access various facilities of the fabric and
 * perform useful tasks, such as:
 * <ul>
 * <li>get a global handle to the data fabric to perform operations and
 * instantiate datalibs with {@link #getDataFabric()}</li>
 * <li>verify and run flows with {@link #verifyFlow(Class)} and
 * {@link #startFlow(Class)}</li>
 * <li>write data to streams with
 * {@link #writeToStream(String, byte[])}</li>
 * </ul>
 */
public abstract class AppFabricTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricTestBase.class);

  // TODO: Fix this when we deal with accounts
  private static final String ACCOUNT = "demo";

  private static final String APPLICATION = "demo";

  private static final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules());

  private static final OmidTransactionalOperationExecutor executor = (OmidTransactionalOperationExecutor) injector
    .getInstance(OperationExecutor.class);

  private static final CConfiguration conf = CConfiguration.create();

  private static final String group = String.format("%s.%s.%s.%s.%s.%d", ACCOUNT, APPLICATION, "flow", "runid",
                                                    "flowlet", 1);

  private static final FlowletContext context = new FlowletContextImpl(executor, OperationContext.DEFAULT, 1, "id",
                                                                       Collections.<String>emptySet(), // no data sets...
                                                                       new FlowLogTag(ACCOUNT, APPLICATION, "flow",
                                                                                      "runid"),
                                                                       new LocalLogDispatcher(conf),
                                                                       new CMetrics(MetricType.FlowUser, group), null);

  private static final DataFabric fabric = context.getDataFabric();

  private static final MetadataService mds = new MetadataService(executor);

  private static Gateway queryGateway = null;

  private static DataSetRegistry dataSetRegistry = null;

  private final static BlockingQueue<TestFlowHandle> flowHandles = new LinkedBlockingQueue<TestFlowHandle>();
  private final static BlockingQueue<TestQueryHandle> queryHandles = new LinkedBlockingQueue<TestQueryHandle>();

  private static final DataSetInstantiator instantiator =
    new DataSetInstantiator(fabric, new SimpleBatchCollectionClient(), null);

  @BeforeClass
  public final static void clearFabricBeforeTestClass() throws OperationException {
    executor.execute(OperationContext.DEFAULT, new ClearFabric(ToClear.ALL));
  }

  @AfterClass
  public final static void stopGatewayIfRunning() throws OperationException, ServerException {
    if (queryGateway != null) {
      queryGateway.stop(true);
      queryGateway = null;
    }
  }

  @AfterClass
  public final static void stopFlowsAndQueries() {
    Map<String, List<Throwable>> exceptionsMap = Maps.newHashMap();
    for(TestFlowHandle flowHandle : flowHandles) {
      // Verify there are no exceptions thrown in the Flowlets
      List<Throwable> exceptions = FlowTestHelper.getAllExceptions(flowHandle);
      if(!exceptions.isEmpty()) {
        exceptionsMap.put(flowHandle.getFlowName(), exceptions);
      }
      flowHandle.stop();
    }
    for(TestQueryHandle queryHandle : queryHandles) {
      // Verify there are no exceptions thrown in QueryProvider
      List<Throwable> exceptions = FlowTestHelper.getAllExceptions(queryHandle);
      if(!exceptions.isEmpty()) {
        exceptionsMap.put(queryHandle.getQueryName(), exceptions);
      }
      queryHandle.stop();
    }
    // If any exceptions found, fail test
    failTestsIfExceptions(exceptionsMap, String.format("Test failed due to unhandled exceptions!%n"));
  }

  /**
   * Returns a reference to the in-memory data fabric.
   *
   * @return data fabric reference
   */
  protected DataFabric getDataFabric() {
    return fabric;
  }

  /**
   * Returns a reference to dataset registry.
   *
   * @return dataset registry reference.
   */
  protected DataSetRegistry getDataSetRegistry() {
    if (dataSetRegistry == null) {
      dataSetRegistry = new TestDataSetRegistry(executor, fabric, null, ACCOUNT);
    }
    return dataSetRegistry;
  }

  /**
   * Clears everything in the data fabric, including all streams and queues.
   * <p/>
   * It can be convenient to call this method in a &#64;Before method in your
   * test class to start every test method with a clean slate:
   * <pre>
   *   &#64;Before
   *   public void clearFabricBetweenTests() {
   *     super.clearDataFabric();
   *   }
   * </pre>
   *
   * @throws OperationException
   */
  protected void clearDataFabric() throws OperationException {
    executor.execute(OperationContext.DEFAULT, new ClearFabric(ToClear.ALL));
  }

  /**
   * Verifies the specified flow by building the graph, configuring each
   * flowlet, wiring up the tuple schemas, and ensuring the flow is in a valid
   * state for deployment and running.
   *
   * @param flowClass the flow to be verified
   * @return true if the flow has been successfully verified, false if not
   */
  protected boolean verifyFlow(Class<? extends Flow> flowClass) {
    FlowDefinition def = FlowTestHelper.createFlow(flowClass, executor);
    if (def == null) return false;
    return true;
  }

  /**
   * Starts the specified flow.
   * <p/>
   * Check the status of the flow and whether it started properly using
   * {@link TestFlowHandle#isSuccess()}.
   * <p/>
   * Always stop the flow using {@link TestFlowHandle#stop()}.
   *
   * @param flowClass the flow to start
   * @return handle to running flow
   */
  protected TestFlowHandle startFlow(Class<? extends Flow> flowClass) {
    TestFlowHandle flowHandle = FlowTestHelper.startFlow(flowClass, conf, executor);
    flowHandles.offer(flowHandle);
    return flowHandle;
  }

  /**
   * Writes the specified bytes to the specified stream and flow.
   *
   * @throws OperationException
   */
  protected void writeToStream(String streamName, byte[] bytes) throws OperationException {
    Map<String, String> headers = new HashMap<String, String>();
    TupleSerializer serializer = new TupleSerializer(false);
    Tuple tuple = new TupleBuilder().set("headers", headers).set("body", bytes).create();
    String uri = FlowStream.buildStreamURI(ACCOUNT, streamName).toString();
    LOG.debug("Writing event to stream: " + uri);
    executor.execute(OperationContext.DEFAULT, new QueueEnqueue(Bytes.toBytes(uri), serializer.serialize(tuple)));
  }

  /**
   * Returns the batch collection registry, which is often used in instantiating
   * tables and datalibs.
   *
   * @return reference to the batch collection registry
   */
  protected BatchCollectionRegistry getRegistry() {
    return context;
  }

  /**
   * Register a data set in the meta data service. This must be done for every data set used in a flow or query
   * @param dataset the data set to be registered
   * @throws Exception if something goes wrong
   */
  public void registerDataSet(DataSet dataset) throws Exception {
    DataSetSpecification spec = dataset.configure();
    Dataset ds = new Dataset(spec.getName());
    ds.setName(spec.getName());
    ds.setType(spec.getType());
    String json = new Gson().toJson(spec);
    ds.setSpecification(json);
    mds.assertDataset(new Account(ACCOUNT), ds);
    instantiator.addDataSet(spec);
  }

  /**
   * Get a runtime instance of a named data set. This does exactly the same as the method of the same name in
   * QueryProviderContext and FlowletContext.
   * @param dataSetName The data set name
   * @param <T> The type of the data set
   * @return a new instance of the data set with data fabric injected
   * @throws DataSetInstantiationException if some of the instantiation magic goes wrong.
   */
  public
  <T extends DataSet> T getDataSet(String dataSetName)
    throws DataSetInstantiationException {
    return instantiator.getDataSet(dataSetName);
  }

  /**
   * Starts the specified query.
   * <p/>
   * Check the status of the query and whether it started properly using
   * {@link TestQueryHandle#isSuccess()}.
   * <p/>
   * Always stop the query using {@link TestQueryHandle#stop()}.
   *
   * @param queryProviderClass the query to start
   * @return handle to running query
   */
  protected TestQueryHandle startQuery(Class<? extends QueryProvider> queryProviderClass) {
    TestQueryHandle queryHandle = FlowTestHelper.startQuery(queryProviderClass, conf, executor);
    queryHandles.offer(queryHandle);
    return queryHandle;
  }

  /**
   * Runs the specified methodName on the query represented by queryHandle with the given parameters
   *
   * @return result of the query execution
   */
  protected QueryResult runQuery(TestQueryHandle queryHandle, String methodName, Multimap<String,
    String> parameters) throws OperationException {
    String connectionString = conf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
    if (connectionString == null) {
      throw new IllegalStateException(String.format("Not able to get Zookeeper server information. Is the query %s " +
                                                      "started?", queryHandle.getQueryName()));
    }
    try {
      ServiceDiscoveryClient discoveryClient = new ServiceDiscoveryClient(connectionString);
      ServiceInstance<ServicePayload> serviceInstance = discoveryClient.getInstance(
        String.format("query.%s",queryHandle.getQueryName()), new RandomStrategy<ServicePayload>());
      if (serviceInstance == null) {
        throw new IllegalStateException(String.format("Not able to get query information. Is the query %s started?",
                                                      queryHandle.getQueryName()));
      }
      RequestBuilder requestBuilder = new RequestBuilder("GET").setUrl(
        String.format("http://%s:%d/rest-query/%s/%s", serviceInstance.getAddress(), serviceInstance.getPort(),
                                                                              queryHandle.getQueryName(), methodName));
      for (Map.Entry<String, String> parameter : parameters.entries()) {
        requestBuilder.addQueryParameter(parameter.getKey(), parameter.getValue());
      }
      Request request = requestBuilder.build();
      LOG.info(String.format("Running query - %s", request.getUrl()));
      AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
      Future<Response> future = asyncHttpClient.executeRequest(request);

      Throwable asyncHttpClientException = null;
      Response response = null;
      try {
        response = future.get();
      } catch (Throwable t) {
        // If the HTTP connection gets closed due to QueryProvider throwing an exception then
        // AsyncHttpClient throws java.util.concurrent.ExecutionException which is unrelated to our context.
        // Lets first check if QueryProvider has thrown any exceptions. If so we need to report them to user first.
        // If QueryProvider has not thrown any exceptions then we'll re-throw AsyncHttpClient's exception.
        // Save the AsyncHttpClient's exception
        asyncHttpClientException = t;
      }

      // Check if any exceptions were thrown when query was run in QueryProvider
      List<Throwable> exceptions = FlowTestHelper.getAllExceptions(queryHandle);
      if(!exceptions.isEmpty()) {
        failTestsIfExceptions(ImmutableMap.of(queryHandle.getQueryName(), exceptions), null);
      }

      // If the QueryProvider did not throw any exceptions, then throw AsyncHttpClient's exception if any
      if(asyncHttpClientException != null) {
        throw new RuntimeException(asyncHttpClientException);
      }

      // Response should not be null if AsyncHttpClient has not thrown any exception
      if(response == null) {
        throw new IllegalStateException(
          String.format("Internal Error! Not able to fetch response from query %s.", queryHandle.getQueryName()));
      }

      return new QueryResult(response.getStatusCode(), response.getResponseBody());
    } catch (Exception e) {
      LOG.error(String.format("Exception while trying to run query %s.%s: ", queryHandle.getQueryName(), methodName), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Encapsulates the result of a query execution
   */
  public static class QueryResult {
    final int returnCode;
    final String content;

    public QueryResult(int returnCode, String content) {
      this.returnCode = returnCode;
      this.content = content;
    }

    /**
     * Returns HTTP return code obtained while running the query
     *
     * @return HTTP return code
     */
    public int getReturnCode() {
      return returnCode;
    }

    /**
     * Returns the JSON respresentation of query output
     *
     * @return Query output
     */
    public String getContent() {
      return content;
    }
  }

  public interface Condition {
    boolean evaluate();
  }

  public void waitForCondition(TestFlowHandle flowHandle, String message, long timeoutMs, Condition condition) throws InterruptedException {
    long execTime = 0L;
    long sleep = 500L;
    do {
      List<Throwable> exceptions = FlowTestHelper.getAllExceptions(flowHandle);
      if(!exceptions.isEmpty()) {
        failTestsIfExceptions(ImmutableMap.of(flowHandle.getFlowName(), exceptions), null);
      }
      if(timeoutMs != -1L &&  execTime >= timeoutMs) {
        Assert.fail(String.format("Time-out while - %s%n", message));
      }
      System.out.println(message);
      Thread.sleep(sleep);
      execTime += sleep;
    } while(!condition.evaluate());
  }

  public void waitForCondition(TestFlowHandle flowHandle, String message, Condition condition) throws InterruptedException {
    waitForCondition(flowHandle, message, -1L, condition);
  }
  static void failTestsIfExceptions(Map<String, List<Throwable>> exceptionsMap, String message) {
    if(!exceptionsMap.isEmpty()) {
      StringWriter stringWriter = new StringWriter();
      if(message != null) {
        stringWriter.write(message);
      }
      for(Map.Entry<String, List<Throwable>> entry : exceptionsMap.entrySet()) {
        for(Throwable t : entry.getValue()) {
          stringWriter.write(String.format("Exception thrown during execution of %s:%n", entry.getKey()));
          PrintWriter printWriter = new PrintWriter(stringWriter);
          t.printStackTrace(printWriter);
          printWriter.close();
        }
      }
      Assert.fail(stringWriter.toString());
    }
  }

//  private TestGatewayHandle queryGatewayHandle = null;
//
//  /**
//   * Starts a query gateway, or returns an existing gateway if one has already
//   * been started.
//   * gateway
//   * @return
//   * @throws IOException 
//   */
//  protected TestGatewayHandle startQueryGateway() throws IOException {
//    if (this.queryGatewayHandle != null) return this.queryGatewayHandle;
//    if (AppFabricTestBase.queryGateway == null) {
//      AppFabricTestBase.queryGateway = internalStartQueryGateway();
//    }
//    this.queryGatewayHandle =
//        new TestGatewayHandle(AppFabricTestBase.queryGateway);
//    return this.queryGatewayHandle;
//  }
//
//  private static final String GW_QUERY_NAME = "access.query";
//  private static final String GW_PATH_PREFIX = "/";
//  private static final String GW_PATH_SUFFIX = "query/";
//  
//  private Gateway internalStartQueryGateway() throws IOException {
//
//    // Look for a free port
//    int port = PortDetector.findFreePort();
//
//    CConfiguration configuration = CConfiguration.create();
//    ZooKeeper zookeeper = new InMemoryZookeeper();
//    zkclient = ZookeeperClientProvider.getClient(
//        zookeeper.getConnectionString(),
//        1000
//    );
//    configuration.set(com.continuuity.common.conf.Constants.
//        CFG_ZOOKEEPER_ENSEMBLE, zookeeper.getConnectionString());
//    configuration.set(com.continuuity.common.conf.Constants.
//        CFG_STATE_STORAGE_CONNECTION_URL, "jdbc:hsqldb:mem:InmemoryZK?user=sa");
//
//    // Create and populate a new config object
//    CConfiguration configuration = new CConfiguration();
//
//    configuration.set(Constants.CONFIG_CONNECTORS, GW_QUERY_NAME);
//    configuration.set(Constants.buildConnectorPropertyName(GW_QUERY_NAME,
//        Constants.CONFIG_CLASSNAME),
//        QueryRestAccessor.class.getCanonicalName());
//    configuration.setInt(Constants.buildConnectorPropertyName(GW_QUERY_NAME,
//        Constants.CONFIG_PORT), port);
//    configuration.set(Constants.buildConnectorPropertyName(GW_QUERY_NAME,
//        Constants.CONFIG_PATH_PREFIX), prefix);
//    configuration.set(Constants.buildConnectorPropertyName(GW_QUERY_NAME,
//        Constants.CONFIG_PATH_MIDDLE), path);
//
//    // Now create our Gateway
//    Gateway theGateway = new Gateway();
//    theGateway.setExecutor(this.executor);
//    theGateway.setConsumer(new NoopConsumer());
//    theGateway.start(null, configuration);
//
//    return theGateway;
//  }
//
//  public static class TestGatewayHandle {
//    
//    TestGatewayHandle(Gateway gateway) {
//      
//    }
//  }
}
