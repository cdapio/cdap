package com.continuuity.payvment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.ReadColumnRange;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.flow.FlowTestHelper;
import com.continuuity.flow.FlowTestHelper.TestFlowHandle;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.continuuity.payvment.SocialAction.SocialActionType;
import com.continuuity.payvment.data.ActivityFeed;
import com.continuuity.payvment.data.ActivityFeed.ActivityFeedEntry;
import com.continuuity.payvment.data.CounterTable;
import com.continuuity.payvment.data.ProductTable;
import com.continuuity.payvment.data.SortedCounterTable;
import com.continuuity.payvment.data.SortedCounterTable.Counter;
import com.continuuity.payvment.util.Bytes;
import com.continuuity.payvment.util.Constants;
import com.continuuity.payvment.util.Helpers;
import com.google.gson.Gson;

public class TestSocialActionFlow extends PayvmentBaseFlowTest {

  @Test(timeout = 20000)
  public void testSocialActionFlow() throws Exception {
    // Get references to tables
    ProductTable productTable = new ProductTable(flowletContext);
    CounterTable productActionCountTable = new CounterTable("productActions",
        flowletContext);
    CounterTable allTimeScoreTable = new CounterTable("allTimeScore",
        flowletContext);
    SortedCounterTable topScoreTable = new SortedCounterTable("topScore",
        flowletContext, new SortedCounterTable.SortedCounterConfig());
    
    // Instantiate product feed flow
    SocialActionFlow socialActionFlow = new SocialActionFlow();
    
    // Start the flow
    TestFlowHandle flowHandle =
        FlowTestHelper.startFlow(socialActionFlow, conf, executor);
    assertTrue(flowHandle.isSuccess());
    
    // Generate a single social action event
    Long now = System.currentTimeMillis();
    Long event_id = 1L;
    Long product_id = 2L;
    Long user_id = 3L;
    String type = "yay-exp-action";
    Long typeScoreIncrease = SocialActionType.fromString(type).getScore();
    SocialAction socialAction =
        new SocialAction(event_id, now, type, product_id, user_id);
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
    productTable.writeObject(Bytes.toBytes(productMeta.product_id),
        productMeta);
    
    // Verify product can be read back
    ProductMeta readProductMeta = productTable.readObject(
        Bytes.toBytes(product_id));
    assertEqual(productMeta, readProductMeta);
    
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
    assertEquals(new Long(1),
        productActionCountTable.readCounterSet(Bytes.toBytes(product_id),
        Bytes.toBytes(type)));
    
    // Verify the product total score has increased to type score increase
    assertEquals(typeScoreIncrease,
        allTimeScoreTable.readSingleKey(
            Bytes.add(Constants.PRODUCT_ALL_TIME_PREFIX,
                Bytes.toBytes(product_id))));
    
    // Verify the hourly score has been incremented to type score increase
    List<Counter> counters = topScoreTable.readTopCounters(
        Bytes.add(Bytes.toBytes(Helpers.hour(now)),
            Bytes.toBytes(productMeta.category)), 10);
    assertEquals(1, counters.size());
    assertEquals(typeScoreIncrease, counters.get(0).getCount());
    
    // Verify a new entry has been made in activity feed
    ReadColumnRange read = new ReadColumnRange(Constants.ACTIVITY_FEED_TABLE,
        ActivityFeed.makeActivityFeedRow(category), null, null);
    OperationResult<Map<byte[],byte[]>> result =
        flowletContext.getDataFabric().read(read);
    assertFalse(result.isEmpty());
    Map<byte[], byte[]> map = result.getValue();
    assertEquals(1, map.size());
    byte [] column = map.keySet().iterator().next();
    byte [] value = map.values().iterator().next();
    ActivityFeedEntry feedEntry = new ActivityFeedEntry(column, value);
    assertEquals(now, feedEntry.timestamp);
    assertEquals(store_id, feedEntry.store_id);
    assertEquals(1, feedEntry.products.size());
    assertEquals(product_id, feedEntry.products.get(0).product_id);
    assertEquals(typeScoreIncrease, feedEntry.products.get(0).score);
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
