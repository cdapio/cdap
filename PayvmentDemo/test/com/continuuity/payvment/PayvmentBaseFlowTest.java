package com.continuuity.payvment;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.continuuity.payvment.util.Bytes;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Base class for running Payvment Flow tests.
 */
public class PayvmentBaseFlowTest {

  protected static final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules());

  protected static final OmidTransactionalOperationExecutor executor =
      (OmidTransactionalOperationExecutor)injector.getInstance(
          OperationExecutor.class);

  protected static final OVCTableHandle handle = executor.getTableHandle();
  
  protected static FlowletContext flowletContext;
  
  protected static CConfiguration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = CConfiguration.create();
    flowletContext = new FlowletContext() {
      @Override public void register(BatchCollectionClient client) {}
      @Override public int getInstanceId() { return 0; }
      @Override public DataFabric getDataFabric() {
        return new DataFabricImpl(executor, OperationContext.DEFAULT);
      }
    };
  }

  @Before
  public void clearFabricBeforeEachTest() throws Exception {
    executor.execute(OperationContext.DEFAULT,
        new ClearFabric(true, true, true));
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {}
  
  /**
   * Asserts that the specified product meta objects contain all the same
   * field values.
   * @param productMeta1
   * @param productMeta2
   */
  protected static void assertEqual(ProductMeta productMeta1,
      ProductMeta productMeta2) {
    assertEquals(productMeta1.product_id, productMeta2.product_id);
    assertEquals(productMeta1.store_id, productMeta2.store_id);
    assertEquals(productMeta1.date, productMeta2.date);
    assertEquals(productMeta1.category, productMeta2.category);
    assertEquals(productMeta1.name, productMeta2.name);
    assertEquals(productMeta1.score, productMeta2.score);
  }
  
  /**
   * Writes the specified bytes to the specified stream and flow.
   * @param flowName
   * @param streamName
   * @param bytes
   * @throws OperationException
   */
  protected static void writeToStream(String flowName, String streamName,
      byte [] bytes)
      throws OperationException {
    Map<String,String> headers = new HashMap<String,String>();
    TupleSerializer serializer = new TupleSerializer(false);
    Tuple tuple = new TupleBuilder()
        .set("headers", headers)
        .set("body", bytes)
        .create();
    System.out.println("Writing event to stream: " +
        FlowStream.defaultURI(flowName, streamName).toString());
    executor.execute(OperationContext.DEFAULT,
        new QueueEnqueue(
            Bytes.toBytes(
                FlowStream.defaultURI(flowName, streamName).toString()),
            serializer.serialize(tuple)));
  }
}
