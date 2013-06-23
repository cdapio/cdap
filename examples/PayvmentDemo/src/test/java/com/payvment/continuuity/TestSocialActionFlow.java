package com.payvment.continuuity;

import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.ReadColumnRange;
import com.continuuity.api.data.lib.CounterTable;
import com.continuuity.api.data.lib.SortedCounterTable;
import com.continuuity.api.data.lib.SortedCounterTable.Counter;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.util.Helpers;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.continuuity.test.FlowTestHelper;
import com.continuuity.test.TestFlowHandle;
import com.google.gson.Gson;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;
import com.payvment.continuuity.data.ActivityFeedTable;
import com.payvment.continuuity.data.PopularFeed;
import com.payvment.continuuity.data.ProductTable;
import com.payvment.continuuity.entity.SocialAction;
import com.payvment.continuuity.entity.SocialAction.SocialActionType;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSocialActionFlow extends PayvmentBaseFlowTest {

  @Test(timeout = 20000)
  public void testSocialActionFlow() throws Exception {
    // Get references to tables
    ProductTable productTable = new ProductTable();
    getDataSetRegistry().registerDataSet(productTable);
    CounterTable productActionCountTable = new CounterTable("productActions");
    getDataSetRegistry().registerDataSet(productActionCountTable);
    CounterTable allTimeScoreTable = new CounterTable("allTimeScores");
    getDataSetRegistry().registerDataSet(allTimeScoreTable);
    SortedCounterTable topScoreTable = new SortedCounterTable("topScores", new SortedCounterTable.SortedCounterConfig
      ());
    getDataSetRegistry().registerDataSet(topScoreTable);
    ActivityFeedTable activityFeedTable = new ActivityFeedTable();
    getDataSetRegistry().registerDataSet(activityFeedTable);

    // Start the flow
    TestFlowHandle flowHandle = startFlow(SocialActionFlow.class);
    assertTrue(flowHandle.isSuccess());

    // Generate a single social action event
    Long now = System.currentTimeMillis();
    Long event_id = 1L;
    Long product_id = 2L;
    Long user_id = 3L;
    Long store_id = 4L;
    String type = "yay-exp-action";
    String category = "Sports";
    String[] country = new String[]{"US", "UK"};
    Long typeScoreIncrease = SocialActionType.fromString(type).getScore();
    SocialAction socialAction = new SocialAction(event_id, now, type, product_id, store_id, country, category, user_id);
    String socialActionJson = socialAction.toJson();

    // Write json to input stream
    writeToStream(SocialActionFlow.inputStream, Bytes.toBytes(socialActionJson));

    // Wait for parsing flowlet to process the tuple
    while (SocialActionParserFlowlet.numProcessed < 1) {
      System.out.println("Waiting for parsing flowlet to process tuple");
      Thread.sleep(1000);
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
    assertEquals(new Long(1), productActionCountTable.readCounterSet(Bytes.toBytes(product_id), Bytes.toBytes(type)));

    // Verify the product total score has increased to type score increase
    assertEquals(typeScoreIncrease, allTimeScoreTable.readSingleKey(Bytes.toBytes(product_id)));

    // Verify the hourly score has been incremented to type score increase
    List<Counter> counters = topScoreTable.readTopCounters(PopularFeed.makeRow(Helpers.hour(now), "US", category), 10);
    assertEquals(1, counters.size());
    assertEquals(typeScoreIncrease, counters.get(0).getCount());

    // Verify a new entry has been made in activity feed
    ReadColumnRange read = new ReadColumnRange(ActivityFeedTable.ACTIVITY_FEED_TABLE,
                                               ActivityFeedTable.makeActivityFeedRow("US", category), null, null);
    OperationResult<Map<byte[], byte[]>> result = getDataFabric().read(read);
    assertFalse(result.isEmpty());
    Map<byte[], byte[]> map = result.getValue();
    assertEquals(1, map.size());
    byte[] column = map.keySet().iterator().next();
    byte[] value = map.values().iterator().next();
    ActivityFeedEntry feedEntry = new ActivityFeedEntry(column, value);
    assertEquals(now, feedEntry.timestamp);
    assertEquals(store_id, feedEntry.store_id);
    assertEquals(1, feedEntry.products.size());
    assertEquals(product_id, feedEntry.products.get(0).product_id);
    assertEquals(typeScoreIncrease, feedEntry.products.get(0).score);
  }

  @Test
  public void testSocialActionSerialization() throws Exception {

    String exampleSocialActionJson = "{\"@id\":\"6709879\"," +
      "\"type\":\"yay-exp-action\"," +
      "\"date\":\"1348074421000\"," +
      "\"product_id\":\"164341\"," +
      "\"store_id\":\"312\"," +
      "\"country\":[\"US\",\"UK\"]," +
      "\"category\":\"Sports\"," +
      "\"actor_id\":\"54321\"}";

    String processedJsonString = SocialActionParserFlowlet.preProcessSocialActionJSON(exampleSocialActionJson);

    Gson gson = new Gson();
    SocialAction socialAction = gson.fromJson(processedJsonString, SocialAction.class);

    assertEquals(new Long(6709879), socialAction.id);
    assertEquals(new Long(164341), socialAction.product_id);
    assertEquals(new Long(1348074421000L), socialAction.date);
    assertEquals("Sports", socialAction.category);
    assertEquals(new Long(312), socialAction.store_id);
    assertEquals("yay-exp-action", socialAction.type);
    assertEquals(new Long(54321), socialAction.actor_id);

    // Try with JSON generating helper method

    exampleSocialActionJson = socialAction.toJson();
    processedJsonString = SocialActionParserFlowlet.preProcessSocialActionJSON(exampleSocialActionJson);
    SocialAction socialAction2 = gson.fromJson(processedJsonString, SocialAction.class);

    assertEqual(socialAction, socialAction2);

    // Try to serialize/deserialize in a tuple
    TupleSerializer serializer = new TupleSerializer(false);
    serializer.register(SocialAction.class);
    Tuple sTuple = new TupleBuilder().set("action", socialAction).create();
    byte[] bytes = serializer.serialize(sTuple);
    assertNotNull(bytes);
    Tuple dTuple = serializer.deserialize(bytes);
    SocialAction socialAction3 = dTuple.get("action");
    assertNotNull(socialAction);

    assertEqual(socialAction, socialAction3);
  }
}
