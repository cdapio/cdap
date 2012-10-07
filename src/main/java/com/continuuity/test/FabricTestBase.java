package com.continuuity.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;

import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.ClearFabric.ToClear;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.flow.FlowTestHelper;
import com.continuuity.flow.FlowTestHelper.TestFlowHandle;
import com.continuuity.flow.definition.api.FlowDefinition;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.payvment.continuuity.util.Bytes;

/**
 * Base class for running AppFabric and Data Fabric in-memory unit tests.
 * <p>
 * To utilize this class, simply create a junit4 class that extends this class:
 * <pre>
 *   public class TestSimpleReadWrite extends FabricTestBase {
 * 
 *     &#64;Test
 *     public void testWriteThenReadFromFabric() throws Exception {
 *       DataFabric dataFabric = getDataFabric();
 *       fabric.
 *   }
 * </pre>
 * <p>
 * Within your tests, you can access various facilities of the fabric and
 * perform useful tasks, such as:
 * <ul>
 *    <li>get a global handle to the data fabric to perform operations and
 *        instantiate datalibs with {@link #getDataFabric()}</li>
 *    <li>verify a flow, run a flow, stop a flow</li>
 *    <li>write data to streams</li>
 * </ul>
 */
public abstract class FabricTestBase {

  private static final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules());

  private static final OmidTransactionalOperationExecutor executor =
      (OmidTransactionalOperationExecutor)injector.getInstance(
          OperationExecutor.class);

  private static final DataFabric fabric = new DataFabricImpl(executor,
      OperationContext.DEFAULT);

  private static final CConfiguration conf = CConfiguration.create();

  private static final OVCTableHandle handle = executor.getTableHandle();

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
   * @param flowName
   * @param streamName
   * @param bytes
   * @throws OperationException
   */
  protected void writeToStream(String flowName, String streamName,
      byte [] bytes) throws OperationException {
    Map<String,String> headers = new HashMap<String,String>();
    TupleSerializer serializer = new TupleSerializer(false);
    Tuple tuple = new TupleBuilder()
        .set("headers", headers)
        .set("body", bytes)
        .create();
    System.out.println("Writing event to stream: " +
        FlowStream.defaultURI(flowName, streamName).toString());
    executor.execute(OperationContext.DEFAULT,
        new QueueEnqueue(Bytes.toBytes(
            FlowStream.defaultURI(flowName, streamName).toString()),
            serializer.serialize(tuple)));
  }
}
