package com.continuuity.payvment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.flow.FlowTestHelper;
import com.continuuity.flow.FlowTestHelper.TestFlowHandle;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.continuuity.payvment.data.ProductTable;
import com.continuuity.payvment.util.Bytes;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestSocialActionFlow {

  private static final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules());

  private static final OmidTransactionalOperationExecutor executor =
      (OmidTransactionalOperationExecutor)injector.getInstance(
          OperationExecutor.class);

  @SuppressWarnings("unused")
  private static final OVCTableHandle handle = executor.getTableHandle();
  
  private static FlowletContext flowletContext;
  
  private static CConfiguration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = CConfiguration.create();
    flowletContext = new FlowletContext() {

      @Override
      public void register(BatchCollectionClient client) {}

      @Override
      public DataFabric getDataFabric() {
        return new DataFabricImpl(executor, OperationContext.DEFAULT);
      }

      @Override
      public int getInstanceId() {
        return 0;
      }
      
    };
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Test(timeout = 10000000)
  public void testSocialActionFlow_Basic() throws Exception {
    
    // Instantiate product feed flow
    SocialActionFlow socialActionFlow = new SocialActionFlow();
    
    // Start the flow
    TestFlowHandle flowHandle =
        FlowTestHelper.startFlow(socialActionFlow, conf, executor);
    assertTrue(flowHandle.isSuccess());
    
    // Generate a single social action event
    long now = System.currentTimeMillis();
    Long event_id = 1L;
    Long product_id = 2L;
    Long user_id = 3L;
    SocialAction socialAction =
        new SocialAction(event_id, now, "yay-exp-action", product_id, user_id);
    String socialActionJson = socialAction.toJson();
    
    // Write json to input stream
    writeToStream(SocialActionFlow.flowName, SocialActionFlow.inputStream,
        Bytes.toBytes(socialActionJson));
    
    // Wait for parsing flowlet to process the tuple
    while (SocialActionParserFlowlet.numProcessed < 1) {
      System.out.println("Waiting for parsing flowlet to process tuple");
      Thread.sleep(100);
    }
    
    // This tuple should error out because it's an unknown product
    while (SocialActionFlow.SocialActionProcessorFlowlet.numErrors < 1) {
      System.out.println("Waiting for processor flowlet to error on tuple");
      Thread.sleep(100);
    }
    
    // Store the product -> category mapping
    Long store_id = 15L;
    String category = "Sports";
    String name = "Tennis Shoes";
    ProductMeta productMeta = new ProductMeta(product_id, store_id, now,
        category, name, 1.0);
    ProductTable productTable = new ProductTable(flowletContext);
    productTable.writeObject(Bytes.toBytes(productMeta.product_id),
        productMeta);
    
    // Write same json to input stream
    writeToStream(SocialActionFlow.flowName, SocialActionFlow.inputStream,
        Bytes.toBytes(socialActionJson));
    
    // Wait for parsing flowlet to process the second tuple
    while (SocialActionParserFlowlet.numProcessed < 2) {
      System.out.println("Waiting for parsing flowlet to process 2nd tuple");
      Thread.sleep(100);
    }
    
    // Wait for processor flowlet to process the tuple
    while (SocialActionFlow.SocialActionProcessorFlowlet.numProcessed < 1) {
      System.out.println("Waiting for processor flowlet to process tuple");
      Thread.sleep(100);
    }
    
    // Wait for processor flowlet to process the tuple
    while (SocialActionFlow.ActivityFeedUpdaterFlowlet.numProcessed < 1) {
      System.out.println("Waiting for updater flowlet to process tuple");
      Thread.sleep(100);
    }
    
    // Wait for processor flowlet to process the tuple
    while (SocialActionFlow.PopularFeedUpdaterFlowlet.numProcessed < 1) {
      System.out.println("Waiting for updater flowlet to process tuple");
      Thread.sleep(100);
    }
    
    System.out.println("Tuple processed to the end!");

    // If we are here, flow ran successfully!
    assertTrue(FlowTestHelper.stopFlow(flowHandle));
    
    // Verify the product action count has been incremented
    
    // Verify the product total score has incremented
    
    // Verify the hourly score has been incremented
    
    // Verify a new entry has been made in activity feed
    
    
  }
  
  private void writeToStream(String flowName, String streamName, byte[] bytes)
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

  @Test
  public void testSocialActionSerialization() throws Exception {

    String exampleSocialActionJson =
        "{\"@id\":\"6709879\"," +
        "\"type\":\"yay-exp-action\"," +
        "\"date\":\"1348074421000\"," +
        "\"product_id\":\"164341\"," +
        "\"actor_id\":\"54321\"}";
    
    String processedJsonString =
        SocialActionParserFlowlet.preProcessSocialActionJSON(
            exampleSocialActionJson);
    
    Gson gson = new Gson();
    SocialAction socialAction =
        gson.fromJson(processedJsonString, SocialAction.class);
    
    assertEquals(new Long(6709879), socialAction.id);
    assertEquals(new Long(164341), socialAction.product_id);
    assertEquals(new Long(1348074421000L), socialAction.date);
    assertEquals("yay-exp-action", socialAction.type);
    assertEquals(new Long(54321), socialAction.actor_id);
    
    // Try with JSON generating helper method
    
    exampleSocialActionJson = socialAction.toJson();
    processedJsonString =
        SocialActionParserFlowlet.preProcessSocialActionJSON(
            exampleSocialActionJson);
    SocialAction socialAction2 =
        gson.fromJson(processedJsonString, SocialAction.class);

    assertEquals(socialAction.id, socialAction2.id);
    assertEquals(socialAction.product_id, socialAction2.product_id);
    assertEquals(socialAction.date, socialAction2.date);
    assertEquals(socialAction.type, socialAction2.type);
    assertEquals(socialAction.actor_id, socialAction2.actor_id);
    
    // Try to serialize/deserialize in a tuple
    TupleSerializer serializer = new TupleSerializer(false);
    serializer.register(SocialAction.class);
    Tuple sTuple = new TupleBuilder().set("action", socialAction).create();
    byte [] bytes = serializer.serialize(sTuple);
    assertNotNull(bytes);
    Tuple dTuple = serializer.deserialize(bytes);
    SocialAction socialAction3 = dTuple.get("action");
    assertNotNull(socialAction);

    assertEquals(socialAction.id, socialAction3.id);
    assertEquals(socialAction.product_id, socialAction3.product_id);
    assertEquals(socialAction.date, socialAction3.date);
    assertEquals(socialAction.type, socialAction3.type);
    assertEquals(socialAction.actor_id, socialAction3.actor_id);
    
  }
}
