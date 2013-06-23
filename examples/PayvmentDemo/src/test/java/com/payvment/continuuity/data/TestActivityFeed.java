package com.payvment.continuuity.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.payvment.continuuity.data.ActivityFeed;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;

/**
 * Isolated tests of the {@link ActivityFeed} class.
 */
public class TestActivityFeed {

  @Test
  public void testJsonSerialization() {

    // Generate an activity feed with 3 entries
    ActivityFeed feed = generateRandomActivityFeed(3);
    System.out.println(ActivityFeed.toJson(feed));
    
    // Generate an activity feed with aggregated entries
    feed = new ActivityFeed();
    feed.addEntry(1L, 1L, 1L, 1L);
    feed.addEntry(1L, 1L, 2L, 1L);
    String feedJson = ActivityFeed.toJson(feed);
    System.out.println(feedJson);
    
    // Verify JSON is exactly what we expect it to be
    // (NOTE: this needs to change if JSON format of activity feed changes)
    String expectedJson = "{\"activity\":[{\"timestamp\":1,\"store_id\":1," +
        "\"products\":[{\"product_id\":1,\"score\":1},{\"product_id\":2," +
        "\"score\":1}]}]}";
    assertEquals(expectedJson, feedJson);
  }

  @Test
  public void testProcessingAndAggregation() {
    
    ActivityFeed feed = new ActivityFeed();
    
    // Start with three entries from three stores
    Long [] stores = new Long [] { 1L, 2L, 3L };
    Long product_id = 1L;
    Long ts = 1000L;
    
    feed.addEntry(ts--, stores[0], product_id++, 1L);
    feed.addEntry(ts--, stores[1], product_id++, 1L);
    feed.addEntry(ts--, stores[2], product_id++, 1L);
    
    // Three entries, same order as insertion
    List<ActivityFeedEntry> entries = feed.getEntireFeed();
    assertEquals(3, entries.size());
    assertTrue(entries.get(0).toString(),
        new ActivityFeedEntry(1000L, 1L, 1L, 1L).equals(entries.get(0)));
    assertTrue(new ActivityFeedEntry(999L, 2L, 2L, 1L).equals(entries.get(1)));
    assertTrue(new ActivityFeedEntry(998L, 3L, 3L, 1L).equals(entries.get(2)));
    
    // Verify limits of 1 and 2 work
    entries = feed.getFeed(2);
    assertEquals(2, entries.size());
    assertTrue(new ActivityFeedEntry(1000L, 1L, 1L, 1L).equals(entries.get(0)));
    assertTrue(new ActivityFeedEntry(999L, 2L, 2L, 1L).equals(entries.get(1)));
    entries = feed.getFeed(1);
    assertEquals(1, entries.size());
    assertTrue(new ActivityFeedEntry(1000L, 1L, 1L, 1L).equals(entries.get(0)));
    
    // Add aggregated products to the first and third stores
    feed.addEntry(ts--, stores[0], 4L, 1L);
    feed.addEntry(ts--, stores[2], 5L, 1L);
    
    // Still just three entries
    entries = feed.getEntireFeed();
    assertEquals(3, entries.size());
    assertTrue(new ActivityFeedEntry(1000L, 1L, 1L, 1L).addEntry(4L, 1L).equals(entries.get(0)));
    assertTrue(new ActivityFeedEntry(999L, 2L, 2L, 1L).equals(entries.get(1)));
    assertTrue(new ActivityFeedEntry(998L, 3L, 3L, 1L).addEntry(5L, 1L).equals(entries.get(2)));
  }
  
  private static final int BIG_QUERY_ITERATIONS = 1000;
  
  @Test
  public void testAggregation_BigQuery() {
    ActivityFeed feed = new ActivityFeed();
    
    // time will go down
    Long time = 100000000L;
    // products will go up
    Long product_id = 1L;
    // only 5 stores
    Long [] stores = new Long [] { 1L, 2L, 3L, 4L, 5L };
    
    // insert lots of entries but only from 5 stores
    for (int i=0; i<BIG_QUERY_ITERATIONS; i++) {
      feed.addEntry(time--, stores[rand.nextInt(stores.length)],
          product_id++, 1L);
    }
    
    // feed should only have 5 entries
    List<ActivityFeedEntry> entries = feed.getEntireFeed();
    assertEquals(5, entries.size());
  }
  
  @Test
  public void testSortOrder_BigQuery() {
    ActivityFeed feed = new ActivityFeed();
    
    // time will go down
    Long time = 100000000L;
    // other entries don't matter for this test
    Long ids = 1L;
    
    for (int i=0; i<BIG_QUERY_ITERATIONS; i++) {
      feed.addEntry(time--, ids++, ids++, ids++);
    }
    
    List<ActivityFeedEntry> entries = feed.getEntireFeed();
    Long lastTime = Long.MAX_VALUE;
    for (ActivityFeedEntry entry : entries) {
      assertTrue(lastTime > entry.timestamp);
      lastTime = entry.timestamp;
    }
  }
  
  // Helper methods
  
  private static final Random rand = new Random();

  public static ActivityFeed generateRandomActivityFeed(int numEntries) {
    ActivityFeed feed = new ActivityFeed();
    for (int i=0; i<numEntries; i++) {
      feed.addEntry(generateRandomActivityFeedEntry());
    }
    return feed;
  }
  
  private static long now = System.currentTimeMillis();
  
  public static ActivityFeedEntry generateRandomActivityFeedEntry() {
    return new ActivityFeedEntry(now--, new Long(Math.abs(rand.nextInt())),
        new Long(Math.abs(rand.nextInt())), new Long(Math.abs(rand.nextInt())));
  }
}
