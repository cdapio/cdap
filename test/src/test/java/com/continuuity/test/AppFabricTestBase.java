package com.continuuity.test;

import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.DataSetRegistry;
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
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.ClearFabric.ToClear;
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
import com.continuuity.test.FlowTestHelper.TestDataSetRegistry;
import com.continuuity.test.FlowTestHelper.TestFlowHandle;
import com.continuuity.test.FlowTestHelper.TestQueryHandle;
import com.google.common.collect.Multimap;
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
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

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
                                                                       new FlowLogTag(ACCOUNT, APPLICATION, "flow",
                                                                                      "runid"),
                                                                       new LocalLogDispatcher(conf),
                                                                       new CMetrics(MetricType.FlowUser, group));

  private static final DataFabric fabric = context.getDataFabric();

  private static Gateway queryGateway = null;

  private static DataSetRegistry dataSetRegistry = null;

  @BeforeClass
  public static void clearFabricBeforeTestClass() throws OperationException {
    executor.execute(OperationContext.DEFAULT, new ClearFabric(ToClear.ALL));
  }

  @AfterClass
  public static void stopGatewayIfRunning() throws OperationException, ServerException {
    if (queryGateway != null) {
      queryGateway.stop(true);
      queryGateway = null;
    }
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
    return FlowTestHelper.startFlow(flowClass, conf, executor);
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
   * Starts the specified query.
   * <p/>
   * Check the status of the query and whether it started properly using
   * {@link FlowTestHelper.TestQueryHandle#isSuccess()}.
   * <p/>
   * Always stop the query using {@link FlowTestHelper.TestQueryHandle#stop()}.
   *
   * @param queryProviderClass the query to start
   * @return handle to running query
   */
  protected TestQueryHandle startQuery(Class<? extends QueryProvider> queryProviderClass) {
    return FlowTestHelper.startQuery(queryProviderClass, conf, executor);
  }

  /**
   * Runs the specified methodName on the query queryName with the given parameters
   *
   * @return result of the query execution
   */
  protected QueryResult runQuery(String queryName, String methodName, Multimap<String,
    String> parameters) throws OperationException {
    String connectionString = conf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
    if (connectionString == null) {
      throw new IllegalStateException(String.format("Not able to get Zookeeper server information. Is the query %s " +
                                                      "started?", queryName));
    }
    try {
      ServiceDiscoveryClient discoveryClient = new ServiceDiscoveryClient(connectionString);
      ServiceInstance<ServicePayload> serviceInstance = discoveryClient.getInstance(String.format("query.%s",
                                                                                                  queryName),
                                                                                    new
                                                                                      RandomStrategy<ServicePayload>());
      if (serviceInstance == null) {
        throw new IllegalStateException(String.format("Not able to get query information. Is the query %s started?",
                                                      queryName));
      }
      RequestBuilder requestBuilder = new RequestBuilder("GET").setUrl(String.format("http://%s:%d/rest-query/%s/%s",
                                                                                     serviceInstance.getAddress(),
                                                                                     serviceInstance.getPort(),
                                                                                     queryName, methodName));
      for (Map.Entry<String, String> parameter : parameters.entries()) {
        requestBuilder.addQueryParameter(parameter.getKey(), parameter.getValue());
      }
      Request request = requestBuilder.build();
      LOG.info(String.format("Running query - %s", request.getUrl()));
      AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
      Future<Response> future = asyncHttpClient.executeRequest(request);
      Response response = future.get();
      return new QueryResult(response.getStatusCode(), response.getResponseBody());
    } catch (Exception e) {
      LOG.error(String.format("Exception while trying to run query %s.%s: ", queryName, methodName), e);
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
