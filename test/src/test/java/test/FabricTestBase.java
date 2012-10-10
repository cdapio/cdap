package com.continuuity.test;

import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.ClearFabric.ToClear;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.flow.FlowTestHelper;
import com.continuuity.flow.FlowTestHelper.TestFlowHandle;
import com.continuuity.flow.definition.api.FlowDefinition;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.FlowletContextImpl;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for running AppFabric and Data Fabric in-memory unit tests.
 * <p>
 * To utilize this class, simply create a junit4 class that extends this class:
 * <pre>
 *   public class TestSimpleReadWrite extends FabricTestBase {
 *
 *     &#64;Test
 *     public void testMyFlow() throws Exception {
 *       Flow flow = new MyFlow();
 *       assertTrue(verifyFlow(flow));
 *       TestFlowHandle flowHandle = startFlow(flow);
 *       assertTrue(flowHandle.getReason(), flowHandle.isSuccess());
 *       // Flow started successfully
 *       flowHandle.stopFlow();
 *     }
 *
 *   }
 * </pre>
 * <p>
 * Within your tests, you can access various facilities of the fabric and
 * perform useful tasks, such as:
 * <ul>
 *    <li>get a global handle to the data fabric to perform operations and
 *        instantiate datalibs with {@link #getDataFabric()}</li>
 *    <li>verify and run flows with {@link #verifyFlow(Flow)} and
 *        {@link #startFlow(Flow)}</li>
 *    <li>write data to streams with
 *        {@link #writeToStream(String, String, byte[])}</li>
 * </ul>
 */
public abstract class FabricTestBase {

  private static final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules());

  private static final OmidTransactionalOperationExecutor executor =
      (OmidTransactionalOperationExecutor)injector.getInstance(
          OperationExecutor.class);

  private static final FlowletContext context =
      new FlowletContextImpl(executor, OperationContext.DEFAULT, 1);

  private static final DataFabric fabric = context.getDataFabric();

  private static final CConfiguration conf = CConfiguration.create();

  @BeforeClass
  public static void clearFabricBeforeTestClass() throws OperationException {
    executor.execute(OperationContext.DEFAULT, new ClearFabric(ToClear.ALL));
  }

  /**
   * Returns a reference to the in-memory data fabric.
   * @return data fabric reference
   */
  protected DataFabric getDataFabric() {
    return fabric;
  }

  /**
   * Clears everything in the data fabric, including all streams and queues.
   * <p>
   * It can be convenient to call this method in a &#64;Before method in your
   * test class to start every test method with a clean slate:
   * <pre>
   *   &#64;Before
   *   public void clearFabricBetweenTests() {
   *     super.clearDataFabric();
   *   }
   * </pre>
   * @throws OperationException
   */
  protected void clearDataFabric() throws OperationException {
    executor.execute(OperationContext.DEFAULT, new ClearFabric(ToClear.ALL));
  }

  /**
   * Verifies the specified flow by building the graph, configuring each
   * flowlet, wiring up the tuple schemas, and ensuring the flow is in a valid
   * state for deployment and running.
   * @param flow the flow to be verified
   * @return true if the flow has been successfully verified, false if not
   */
  protected boolean verifyFlow(Flow flow) {
    FlowDefinition def = FlowTestHelper.createFlow(flow);
    if (def == null) return false;
    return true;
  }

  /**
   * Starts the specified flow.
   * <p>
   * Check the status of the flow and whether it started properly using
   * {@link TestFlowHandle#isSuccess()}.
   * <p>
   * Always stop the flow using {@link TestFlowHandle#stopFlow()}.
   * @param flow the flow to start
   * @return handle to running flow
   */
  protected TestFlowHandle startFlow(Flow flow) {
    return FlowTestHelper.startFlow(flow, conf, executor);
  }

  /**
   * Writes the specified bytes to the specified stream and flow.
   * @throws OperationException
   */
  protected void writeToStream(String accountName, String streamName,
      byte [] bytes) throws OperationException {
    Map<String,String> headers = new HashMap<String,String>();
    TupleSerializer serializer = new TupleSerializer(false);
    Tuple tuple = new TupleBuilder()
    .set("headers", headers)
    .set("body", bytes)
    .create();
    String uri = FlowStream.buildStreamURI(accountName, streamName).toString();
    System.out.println("Writing event to stream: " + uri);
    executor.execute(OperationContext.DEFAULT,
        new QueueEnqueue(Bytes.toBytes(uri),
            serializer.serialize(tuple)));
  }

  /**
   * Returns the batch collection registry, which is often used in instantiating
   * tables and datalibs.
   * @return reference to the batch collection registry
   */
  protected BatchCollectionRegistry getRegistry() {
    return context;
  }
}
