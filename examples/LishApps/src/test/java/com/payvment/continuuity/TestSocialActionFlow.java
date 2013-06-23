/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.payvment.continuuity;

import com.continuuity.api.common.Bytes;
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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 *
 */
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
    Long eventId = 1L;
    Long productId = 2L;
    Long userId = 3L;
    Long storeId = 4L;
    String type = "yay-exp-action";
    String category = "Sports";
    String[] country = new String[]{"US", "UK"};
    Long typeScoreIncrease = SocialActionType.fromString(type).getScore();
    SocialAction socialAction = new SocialAction(eventId, now, type, productId, storeId, country, category, userId);

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
    assertEquals(new Long(1), productActionCountTable.readCounterSet(Bytes.toBytes(productId), Bytes.toBytes(type)));

    // Verify the product total score has increased to type score increase
    assertEquals(typeScoreIncrease, allTimeScoreTable.readSingleKey(Bytes.toBytes(productId)));

    // Verify the hourly score has been incremented to type score increase
    List<SortedCounterTable.Counter> counters =

      topScoreTable.readTopCounters(PopularFeed.makeRow(Helpers.hour(now), "US", category), 10);
    assertEquals(1, counters.size());

    // TODO: TypeScoreIncrease should be = 2, check why counter is different
    //assertEquals(typeScoreIncrease, counters.get(0).getCount());

    // Verify a new entry has been made in activity feed
    List<ActivityFeedEntry> activityFeedEntries = activityFeedTable.readEntries("US", category, 1, now, 0);
    assertFalse(activityFeedEntries.isEmpty());
    ActivityFeedEntry feedEntry = activityFeedEntries.get(0);
    assertEquals(now, feedEntry.timestamp);
    assertEquals(storeId, feedEntry.storeId);
    assertEquals(1, feedEntry.products.size());
    assertEquals(productId, feedEntry.products.get(0).productId);
    assertEquals(typeScoreIncrease, feedEntry.products.get(0).score);

    // If we are here, flow ran successfully!
    applicationManager.stopAll();
  }

  @Test
  public void testSocialActionSerialization() throws Exception {

    String exampleSocialActionJson = "{\"@id\":\"6709879\"," +
      "\"type\":\"yay-exp-action\"," +
      "\"date\":\"1348074421000\"," +
      "\"productId\":\"164341\"," +
      "\"storeId\":\"312\"," +
      "\"country\":[\"US\",\"UK\"]," +
      "\"category\":\"Sports\"," +
      "\"actor_id\":\"54321\"}";

    String processedJsonString = SocialActionParserFlowlet.preProcessSocialActionJSON(exampleSocialActionJson);

    Gson gson = new Gson();
    SocialAction socialAction = gson.fromJson(processedJsonString, SocialAction.class);

    assertEquals(new Long(6709879), socialAction.id);
    assertEquals(new Long(164341), socialAction.productId);
    assertEquals(new Long(1348074421000L), socialAction.date);
    assertEquals("Sports", socialAction.category);
    assertEquals(new Long(312), socialAction.storeId);
    assertEquals("yay-exp-action", socialAction.type);
    assertEquals(new Long(54321), socialAction.actor_id);

    // Try with JSON generating helper method
    exampleSocialActionJson = socialAction.toJson();
    processedJsonString = SocialActionParserFlowlet.preProcessSocialActionJSON(exampleSocialActionJson);
    SocialAction socialAction2 = gson.fromJson(processedJsonString, SocialAction.class);

    assertEqual(socialAction, socialAction2);
  }
}
