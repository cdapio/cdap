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

import com.continuuity.api.data.OperationException;
import com.continuuity.test.AppFabricTestBase;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.payvment.continuuity.data.ActivityFeed;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;
import com.payvment.continuuity.data.ActivityFeedTable;
import com.payvment.continuuity.data.ClusterFeedReader;
import com.payvment.continuuity.data.ClusterTable;
import com.payvment.continuuity.data.PopularFeed;
import com.payvment.continuuity.data.PopularFeed.PopularFeedEntry;
import com.payvment.continuuity.data.SortedCounterTable;
import com.payvment.continuuity.entity.SocialAction;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Complete end-to-end testing of Lish Activity and Popular Feeds.
 */
public class TestClusterFeeds extends AppFabricTestBase {

  private static final String US = "US";

  @Test(timeout = 20000)
  public void testStreamsFlowsQueries() throws InterruptedException, IOException, TimeoutException {
    ApplicationManager applicationManager = deployApplication(LishApp.class);

    // Start the cluster writer flow
    applicationManager.startFlow(ClusterWriterFlow.FLOW_NAME);
    Thread.sleep(500);

    // Start the social action flow
    applicationManager.startFlow(SocialActionFlow.FLOW_NAME);
    Thread.sleep(500);

    // Write sample-clusters.csv to stream for clusters
    StreamWriter s1 = applicationManager.getStreamWriter(LishApp.CLUSTER_STREAM);
    int numClusterEntries = 0;

    try {
      numClusterEntries = writeFileToStream("sample-clusters.csv", s1, LishApp.CLUSTER_STREAM, 1000);
      System.out.println("sample-clusters.csv entries: " + numClusterEntries);
    } catch (OperationException e) {
      System.out.println(e.getLocalizedMessage());
    }

    // Get ClusterWriter instance / metrics
    RuntimeMetrics m1 = RuntimeStats.getFlowletMetrics(LishApp.APP_NAME,
                                                       ClusterWriterFlow.FLOW_NAME,
                                                       ClusterWriterFlow.WRITER_FLOWLET_NAME);

    System.out.println("Waiting for Cluster Writer flow to process");
    m1.waitForProcessed(numClusterEntries, 5, TimeUnit.SECONDS);
    Assert.assertEquals(0L, m1.getException());
    System.out.println("ClusterWriterFlow.ClusterWriter processed: " + m1.getProcessed());

    // Write sample-actions.json to stream for social actions
    StreamWriter s2 = applicationManager.getStreamWriter(LishApp.SOCIAL_ACTION_STREAM);

    int numActions = 0;

    // Write Sample actions
    try {
      numActions = writeFileToStream("sample-actions.json", s2, LishApp.SOCIAL_ACTION_STREAM, 1000);
      System.out.println("Writing sample-actions.json to stream " + LishApp.SOCIAL_ACTION_STREAM);
    } catch (OperationException e) {
      System.out.println(e.getLocalizedMessage());
    }

    RuntimeMetrics clusterSourceParserMetrics = RuntimeStats.getFlowletMetrics(LishApp.APP_NAME,
                                                                               SocialActionFlow.FLOW_NAME,
                                                                               "action_parser");

    clusterSourceParserMetrics.waitForProcessed(numActions, 5, TimeUnit.SECONDS);
    System.out.println("SoclialActionFlow.ClusterSourceParser processed: " + clusterSourceParserMetrics.getProcessed());
    assertTrue(clusterSourceParserMetrics.getProcessed() == numActions);

    RuntimeMetrics m2 = RuntimeStats.getFlowletMetrics(LishApp.APP_NAME,
                                                       SocialActionFlow.FLOW_NAME,
                                                       "popular_feed_updater");

    System.out.println("Waiting for social action popular updater flowlet");
    m2.waitForProcessed(numActions, 5, TimeUnit.SECONDS);
    Assert.assertEquals(0L, m2.getException());
    System.out.println("SocialActionFlow.PopularFeedUpdaterFlowlet processed: " + m2.getProcessed());

    RuntimeMetrics m3 = RuntimeStats.getFlowletMetrics(LishApp.APP_NAME,
                                                       SocialActionFlow.FLOW_NAME,
                                                       "activity_feed_updater");

    System.out.println("Waiting for social action activity updater flowlet");
    m3.waitForProcessed(numActions, 5, TimeUnit.SECONDS);
    Assert.assertEquals(0L, m3.getException());
    System.out.println("SocialActionFlow.ActivityFeedUpdaterFlowlet processed: " + m2.getProcessed());

    // Verify flow processing results using feed reader queries
    ClusterTable clusterTable = (ClusterTable) applicationManager.getDataSet(LishApp.CLUSTER_TABLE);
    SortedCounterTable topScoreTable = applicationManager.getDataSet(LishApp.TOP_SCORE_TABLE);
    ActivityFeedTable activityFeedTable = applicationManager.getDataSet(LishApp.ACTIVITY_FEED_TABLE);

    ClusterFeedReader feedReader = new ClusterFeedReader(clusterTable, topScoreTable, activityFeedTable);

    // FIRST HOUR
    Long firstHour = 1349125200000L;
    Long secondHour = 1349128800000L;
    Long thirdHour = 1349132400000L;

    // Read first hour.  Should have pop entries all the same score
    // and activity feed in descending productId order for clusters 1 and 3.
    System.out.println("Validating Activity feed entries...");

    try {
      // Cluster 1 and 3 pop
      PopularFeed popFeed = feedReader.getPopularFeed(US, 1, firstHour, 1, 15, 0);
      List<PopularFeedEntry> popEntries = popFeed.getFeed(15);
      assertEquals(10, popEntries.size());
      assertDescendingScore(popEntries);

      Long expectedScore = SocialAction.SocialActionType.YAY.getScore();

      for (PopularFeedEntry entry : popEntries) {
        assertEquals(expectedScore, entry.score);
      }

      popFeed = feedReader.getPopularFeed(US, 3, firstHour, 1, 5, 0);
      popEntries = popFeed.getFeed(5);
      assertDescendingScore(popEntries);
      assertEquals(5, popEntries.size());
      for (PopularFeedEntry entry : popEntries) {
        assertEquals(expectedScore, entry.score);
      }

      // Cluster 1 and 3 activity
      ActivityFeed activityFeed = feedReader.getActivityFeed(US, 1, 15, secondHour, firstHour);
      List<ActivityFeedEntry> activityEntries = activityFeed.getEntireFeed();
      assertEquals(10, activityEntries.size());
      assertDescendingTime(activityEntries);
      activityFeed = feedReader.getActivityFeed(US, 3, 5, secondHour, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(5, activityEntries.size());
      assertDescendingTime(activityEntries);
      System.out.println(ActivityFeed.toJson(activityFeed));

      // Cluster 2 should be empty for pop and activity
      popFeed = feedReader.getPopularFeed(US, 2, firstHour, 1, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(0, popEntries.size());
      activityFeed = feedReader.getActivityFeed(US, 2, 15, secondHour, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(0, activityEntries.size());

      // Read first and second hour.
      // Every product has been liked N times where N is id.

      Long likeScore = SocialAction.SocialActionType.LIKE.getScore();
      popFeed = feedReader.getPopularFeed(US, 1, secondHour, 2, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(10, popEntries.size());
      Long productId = 10L;
      for (PopularFeedEntry entry : popEntries) {
        assertEquals(productId, entry.productId);
        Long score = expectedScore + (productId * likeScore);

        assertEquals("For productId " + productId + ", expected score " +
                       score + " but found score " + entry.score, score, entry.score);
        productId--;
      }
      popFeed = feedReader.getPopularFeed(US, 3, secondHour, 2, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(10, popEntries.size());
      productId = 10L;
      for (PopularFeedEntry entry : popEntries) {
        assertEquals(productId, entry.productId);
        Long score = expectedScore + (productId * likeScore);
        assertEquals(score, entry.score);
        productId--;
      }

      // Cluster 1 and 3 activity
      activityFeed = feedReader.getActivityFeed(US, 1, 15, thirdHour, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(10, activityEntries.size());
      assertDescendingTime(activityEntries);
      activityFeed = feedReader.getActivityFeed(US, 3, 5, thirdHour, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(5, activityEntries.size());
      assertDescendingTime(activityEntries);

      // Cluster 2 should still be empty
      popFeed = feedReader.getPopularFeed(US, 2, secondHour, 2, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(0, popEntries.size());
      activityFeed = feedReader.getActivityFeed(US, 2, 15, thirdHour, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(0, activityEntries.size());

      // Read all three hours now.
      // Cluster 1 should not change at all, cluster 2 and 3 will have full checks

      // Verify cluster 1 is the same
      popFeed = feedReader.getPopularFeed(US, 1, thirdHour, 3, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(10, popEntries.size());
      productId = 10L;
      for (PopularFeedEntry entry : popEntries) {
        assertEquals(productId, entry.productId);
        Long score = expectedScore + (productId * likeScore);
        assertEquals(score, entry.score);
        productId--;
      }

      // Cluster 2 is made up of only the third hour of activity categories
      // Explicitly verify cluster 2 activity feed
      activityFeed = feedReader.getActivityFeed(US, 2, 10, Long.MAX_VALUE, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(2, activityEntries.size());
      assertDescendingTime(activityEntries);

      assertTrue(activityEntries.get(0).equals(
        new ActivityFeedEntry(1349132435000L, 230L, 30L, likeScore).addEntry(31L, 12 * likeScore)));

      assertTrue(activityEntries.get(1).equals(
        new ActivityFeedEntry(1349132432000L, 220L, 20L, likeScore).addEntry(21L, 12 * likeScore)));

      // Cluster 2 pop has four products, check explicitly
      popFeed = feedReader.getPopularFeed(US, 2, thirdHour, 3, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(4, popEntries.size());
      assertTrue(popEntries.get(0).equals(new PopularFeedEntry(31L, likeScore * 12)));
      assertTrue(popEntries.get(1).equals(new PopularFeedEntry(21L, likeScore * 12)));
      assertTrue(popEntries.get(2).equals(new PopularFeedEntry(30L, likeScore)));
      assertTrue(popEntries.get(3).equals(new PopularFeedEntry(20L, likeScore)));

      // Verify count and properties on cluster 3

      // Pop cluster 3
      popFeed = feedReader.getPopularFeed(US, 3, thirdHour, 3, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(14, popEntries.size());
      assertDescendingScore(popEntries);
      // Cluster 3 activity
      activityFeed = feedReader.getActivityFeed(US, 3, 15, Long.MAX_VALUE, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(12, activityEntries.size());
      assertDescendingTime(activityEntries);

      // Now try with different countries

      // UK and clusters 1 and 3 should have 1 product, cluster 2 none
      String uk = "UK";
      // pop
      popFeed = feedReader.getPopularFeed(uk, 1, thirdHour, 3, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(1, popEntries.size());
      assertDescendingScore(popEntries);
      popFeed = feedReader.getPopularFeed(uk, 3, thirdHour, 3, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(1, popEntries.size());
      assertDescendingScore(popEntries);
      popFeed = feedReader.getPopularFeed(uk, 2, thirdHour, 3, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(0, popEntries.size());
      // activity
      activityFeed = feedReader.getActivityFeed(uk, 1, 15, Long.MAX_VALUE, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(1, activityEntries.size());
      assertDescendingTime(activityEntries);
      activityFeed = feedReader.getActivityFeed(uk, 3, 15, Long.MAX_VALUE, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(1, activityEntries.size());
      assertDescendingTime(activityEntries);
      activityFeed = feedReader.getActivityFeed(uk, 2, 15, Long.MAX_VALUE, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(0, activityEntries.size());

      // JP should have 1 product for cluster 1, 4 for 2, 5 for 3
      String jp = "JP";
      // pop
      popFeed = feedReader.getPopularFeed(jp, 1, thirdHour, 3, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(1, popEntries.size());
      assertDescendingScore(popEntries);
      popFeed = feedReader.getPopularFeed(jp, 2, thirdHour, 3, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(4, popEntries.size());
      assertDescendingScore(popEntries);
      popFeed = feedReader.getPopularFeed(jp, 3, thirdHour, 3, 15, 0);
      popEntries = popFeed.getFeed(15);
      assertEquals(5, popEntries.size());
      assertDescendingScore(popEntries);
      // activity cluster 2 will only have 2 entries (2 stores), 3 on cluster 3
      activityFeed = feedReader.getActivityFeed(jp, 1, 15, Long.MAX_VALUE, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(1, activityEntries.size());
      assertDescendingTime(activityEntries);
      activityFeed = feedReader.getActivityFeed(jp, 2, 15, Long.MAX_VALUE, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(2, activityEntries.size());
      assertDescendingTime(activityEntries);
      activityFeed = feedReader.getActivityFeed(jp, 3, 15, Long.MAX_VALUE, firstHour);
      activityEntries = activityFeed.getEntireFeed();
      assertEquals(3, activityEntries.size());
      assertDescendingTime(activityEntries);

      // Stop all flows
      applicationManager.stopAll();

    } catch (OperationException e) {
      System.out.println(e.getLocalizedMessage());
    }
  }

  private void assertDescendingScore(List<PopularFeedEntry> popEntries) {
    Long score = Long.MAX_VALUE;
    for (PopularFeedEntry entry : popEntries) {
      assertTrue(score >= entry.score);
      score = entry.score;
    }
  }

  private void assertDescendingTime(List<ActivityFeedEntry> activityEntries) {
    Long timestamp = Long.MAX_VALUE;
    for (ActivityFeedEntry entry : activityEntries) {
      assertTrue(timestamp >= entry.timestamp);
      timestamp = entry.timestamp;
    }
  }

  private int writeFileToStream(String inputFile,
                                StreamWriter stream,
                                String streamName,
                                int limit) throws IOException, OperationException {

    System.out.println("Opening file '" + inputFile + "' to write to stream '" +
                         streamName + "'");
    BufferedReader reader =
      new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream(inputFile)));

    String line = null;
    int i = 0;
    while ((line = reader.readLine()) != null && i < limit) {
      System.out.println("\t " + line);

      if (line.startsWith("cluster") || line.startsWith("#") || line.equals("")) {
        continue;
      }

      stream.send(line);
      i++;
    }
    System.out.println("Wrote " + i + " events to stream " + streamName);
    reader.close();
    return i;
  }
}
