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
import com.continuuity.payvment.entity.ProductFeedEntry;
import com.continuuity.payvment.entity.SocialAction;
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
   * Asserts that the specified product feed entries contain all the same
   * field values.
   * @param productEntry1
   * @param productEntry2
   */
  protected static void assertEqual(ProductFeedEntry productEntry1,
      ProductFeedEntry productEntry2) {
    assertEquals(productEntry1.product_id, productEntry2.product_id);
    assertEquals(productEntry1.store_id, productEntry2.store_id);
    assertEquals(productEntry1.date, productEntry2.date);
    assertEquals(productEntry1.category, productEntry2.category);
    assertEquals(productEntry1.name, productEntry2.name);
    assertEquals(productEntry1.score, productEntry2.score);
  }
  
  /**
   * Asserts that the specified social actions contain all the same
   * field values.
   * @param socialAction1
   * @param socialAction1
   */
  protected static void assertEqual(SocialAction socialAction1,
      SocialAction socialAction2) {
    assertEquals(socialAction1.id, socialAction2.id);
    assertEquals(socialAction1.product_id, socialAction2.product_id);
    assertEquals(socialAction1.store_id, socialAction2.store_id);
    assertEquals(socialAction1.actor_id, socialAction2.actor_id);
    assertEquals(socialAction1.date, socialAction2.date);
    assertEquals(socialAction1.category, socialAction2.category);
    assertEquals(socialAction1.type, socialAction2.type);
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
