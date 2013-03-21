package com.payvment.continuuity;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.gson.Gson;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;
import com.payvment.continuuity.data.ActivityFeedTable;
import com.payvment.continuuity.data.CounterTable;
import com.payvment.continuuity.data.PopularFeed;
import com.payvment.continuuity.data.SortedCounterTable;
import com.payvment.continuuity.entity.SocialAction;
import com.payvment.continuuity.entity.SocialAction.SocialActionType;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestSocialActionFlow extends PayvmentBaseFlowTest {

  @Test(timeout = 20000)
  public void testSocialActionFlow() throws Exception {
    ApplicationManager applicationManager = deployApplication(LishApp.class);

    // Get references to tables
    CounterTable productActionCountTable = applicationManager.getDataSet(LishApp.PRODUCT_ACTION_TABLE);
    CounterTable allTimeScoreTable = applicationManager.getDataSet(LishApp.ALL_TIME_SCORE_TABLE);
    SortedCounterTable topScoreTable = applicationManager.getDataSet(LishApp.TOP_SCORE_TABLE);
    ActivityFeedTable activityFeedTable = applicationManager.getDataSet(LishApp.ACTIVITY_FEED_TABLE);

    // Instantiate and start product feed flow
    applicationManager.startFlow(SocialActionFlow.FLOW_NAME);

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
    StreamWriter s1 = applicationManager.getStreamWriter(LishApp.SOCIAL_ACTION_STREAM);
    s1.send(socialActionJson);

    // Wait for parsing flowlet to process the tuple
    RuntimeMetrics m1 =
      RuntimeStats.getFlowletMetrics(LishApp.APP_NAME, SocialActionFlow.FLOW_NAME, "action_parser");
    System.out.println("Waiting for parsing flowlet to process tuple");
    m1.waitForProcessed(1, 5, TimeUnit.SECONDS);

    // Wait for processor flowlet to process the tuple
    RuntimeMetrics m2 =
      RuntimeStats.getFlowletMetrics(LishApp.APP_NAME, SocialActionFlow.FLOW_NAME, "action_processor");
    System.out.println("Waiting for processor flowlet to process tuple");
    m1.waitForProcessed(1, 5, TimeUnit.SECONDS);

    // Wait for processor flowlet to process the tuple
    RuntimeMetrics m3 =
      RuntimeStats.getFlowletMetrics(LishApp.APP_NAME, SocialActionFlow.FLOW_NAME, "activity_feed_updater");
    System.out.println("Waiting for activity feed updater flowlet to process tuple");
    m1.waitForProcessed(1, 5, TimeUnit.SECONDS);

    // Wait for processor flowlet to process the tuple
    RuntimeMetrics m4 =
      RuntimeStats.getFlowletMetrics(LishApp.APP_NAME, SocialActionFlow.FLOW_NAME, "popular_feed_updater");
    System.out.println("Waiting for popular feed updater flowlet to process tuple");
    m1.waitForProcessed(1, 5, TimeUnit.SECONDS);

    System.out.println("Tuple processed to the end!");

    // Verify the product action count has been incremented
    assertEquals(new Long(1), productActionCountTable.readCounterSet(Bytes.toBytes(product_id), Bytes.toBytes(type)));

    // Verify the product total score has increased to type score increase
    assertEquals(typeScoreIncrease, allTimeScoreTable.readSingleKey(Bytes.toBytes(product_id)));

    // Verify the hourly score has been incremented to type score increase
    List<SortedCounterTable.Counter> counters = topScoreTable.readTopCounters(PopularFeed.makeRow(Helpers.hour(now), "US", category), 10);
    assertEquals(1, counters.size());

    // TODO: TypeScoreIncrease should be = 2, check why counter is different
    //assertEquals(typeScoreIncrease, counters.get(0).getCount());

    // Verify a new entry has been made in activity feed
    List<ActivityFeedEntry> activityFeedEntries = activityFeedTable.readEntries("US", category, 1, now, 0);
    assertFalse(activityFeedEntries.isEmpty());
    ActivityFeedEntry feedEntry = activityFeedEntries.get(0);
    assertEquals(now, feedEntry.timestamp);
    assertEquals(store_id, feedEntry.store_id);
    assertEquals(1, feedEntry.products.size());
    assertEquals(product_id, feedEntry.products.get(0).product_id);
    assertEquals(typeScoreIncrease, feedEntry.products.get(0).score);

    // If we are here, flow ran successfully!
    applicationManager.stopAll();
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
  }
}
