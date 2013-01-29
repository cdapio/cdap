package com.payvment.continuuity;

import com.continuuity.test.FlowTestHelper;
import com.continuuity.test.TestQueryHandle;
import com.continuuity.test.query.info.QueryTestInfo;
import com.google.common.collect.ImmutableMultimap;
import com.payvment.continuuity.data.ActivityFeed;
import com.payvment.continuuity.data.ActivityFeedTable;
import com.payvment.continuuity.data.ClusterTable;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestClusterFeedQueryProvider extends PayvmentBaseFlowTest {
  @Test
  public void testQueryProvider() throws Exception {
    TestQueryHandle queryHandle = startQuery(ClusterFeedQueryProvider.class);
    Assert.assertTrue(queryHandle.isSuccess());
    Assert.assertTrue(queryHandle.isRunning());

    QueryTestInfo queryTestInfo = queryHandle.getQueryTestInfo("feedreader");

    populateActivityFeed();

    QueryResult queryResult = runQuery(queryHandle, "readactivity", ImmutableMultimap.<String,
      String>of("clusterid", "1", "limit", "10", "country", "us"));
    Assert.assertEquals(1, queryTestInfo.getNumQueries());
    Assert.assertEquals(200, queryResult.getReturnCode());
    Assert.assertEquals("{\"activity\":[{\"timestamp\":12347,\"store_id\":101,\"products\":[{\"product_id\":1011," +
                          "" + "\"score\":6},{\"product_id\":1011,\"score\":5},{\"product_id\":1011," +
                          "\"score\":1}]}]}", queryResult.getContent());
    queryHandle.stop();
    Assert.assertFalse(queryHandle.isRunning());
  }

  private void populateActivityFeed() {
    ClusterTable clusterTable = new ClusterTable();
    // TODO: Use DataSetRegistry from QueryProviderContext
    getDataSetRegistry().registerDataSet(clusterTable);
    clusterTable.writeCluster(1, "sports", 1.2);
    clusterTable.writeCluster(2, "music", 2.0);
    clusterTable.writeCluster(3, "apparel", 1.5);

    ActivityFeedTable activityFeedTable = new ActivityFeedTable();
    // TODO: Use DataSetRegistry from QueryProviderContext
    getDataSetRegistry().registerDataSet(activityFeedTable);
    activityFeedTable.writeEntry("us", "sports", new ActivityFeed.ActivityFeedEntry(12345L, 101L, 1011L, 1L));
    activityFeedTable.writeEntry("us", "sports", new ActivityFeed.ActivityFeedEntry(12346L, 101L, 1011L, 5L));
    activityFeedTable.writeEntry("us", "sports", new ActivityFeed.ActivityFeedEntry(12347L, 101L, 1011L, 6L));
    activityFeedTable.writeEntry("us", "music", new ActivityFeed.ActivityFeedEntry(12348L, 102L, 1021L, 2L));
    activityFeedTable.writeEntry("us", "music", new ActivityFeed.ActivityFeedEntry(12349L, 102L, 1021L, 7L));
    activityFeedTable.writeEntry("us", "apparel", new ActivityFeed.ActivityFeedEntry(12350L, 103L, 1031L, 10L));
    activityFeedTable.writeEntry("us", "apparel", new ActivityFeed.ActivityFeedEntry(12351L, 103L, 1031L, 6L));
    activityFeedTable.writeEntry("us", "apparel", new ActivityFeed.ActivityFeedEntry(12352L, 103L, 1031L, 2L));
    activityFeedTable.writeEntry("us", "apparel", new ActivityFeed.ActivityFeedEntry(12353L, 103L, 1031L, 2L));
  }
}
