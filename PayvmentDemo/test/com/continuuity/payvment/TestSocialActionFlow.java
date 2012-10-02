package com.continuuity.payvment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.flow.FlowTestHelper;
import com.continuuity.flow.FlowTestHelper.TestFlowHandle;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
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
  
  private static CConfiguration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = CConfiguration.create();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Test(timeout = 10000)
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
    Long user_id = 3l;
    SocialAction socialAction =
        new SocialAction(event_id, now, "yay-exp-action", product_id, user_id);
    String socialActionJson = socialAction.toJson();
    
    // Write json to input stream
    writeToStream(SocialActionFlow.inputStream,
        Bytes.toBytes(socialActionJson));
    
    // Wait for parsing flowlet to process the tuple
    while (SocialActionParserFlowlet.numProcessed < 1) {
      System.out.println("Waiting for parsing flowlet to process tuple");
      Thread.sleep(10);
    }
    
    // Wait for processor flowlet to process the tuple
    while (SocialActionFlow.SocialActionProcessorFlowlet.numProcessed < 1) {
      System.out.println("Waiting for processor flowlet to process tuple");
      Thread.sleep(10);
    }
    
    // Wait for processor flowlet to process the tuple
    while (SocialActionFlow.ActivityFeedUpdaterFlowlet.numProcessed < 1) {
      System.out.println("Waiting for updater flowlet to process tuple");
      Thread.sleep(10);
    }
    
    // Wait for processor flowlet to process the tuple
    while (SocialActionFlow.PopularFeedUpdaterFlowlet.numProcessed < 1) {
      System.out.println("Waiting for updater flowlet to process tuple");
      Thread.sleep(10);
    }
    
    System.out.println("Tuple processed to the end!");

    // If we are here, flow ran successfully!
    assertTrue(FlowTestHelper.stopFlow(flowHandle));
    
    // Verify the product action count has been incremented
    
    // Verify the product total score has incremented
    
    // Verify the hourly score has been incremented
    
    // Verify a new entry has been made in activity feed
    
    
  }
  
  private void writeToStream(String streamName, byte[] bytes)
      throws OperationException {
    Map<String,String> headers = new HashMap<String,String>();
    TupleSerializer serializer = new TupleSerializer(false);
    Tuple tuple = new TupleBuilder()
        .set("headers", headers)
        .set("body", bytes)
        .create();
    executor.execute(OperationContext.DEFAULT,
        new QueueEnqueue(
            Bytes.toBytes(FlowStream.defaultInputURI(streamName).toString()),
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
