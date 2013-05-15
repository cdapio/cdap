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

package com.payvment.continuuity.data;

import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
      "\"products\":[{\"productId\":1,\"score\":1},{\"productId\":2," +
      "\"score\":1}]}]}";
    assertEquals(expectedJson, feedJson);
  }

  @Test
  public void testProcessingAndAggregation() {

    ActivityFeed feed = new ActivityFeed();

    // Start with three entries from three stores
    Long[] stores = new Long[]{1L, 2L, 3L};
    Long productId = 1L;
    Long ts = 1000L;

    feed.addEntry(ts--, stores[0], productId++, 1L);
    feed.addEntry(ts--, stores[1], productId++, 1L);
    feed.addEntry(ts--, stores[2], productId++, 1L);

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
    Long productId = 1L;
    // only 5 stores
    Long[] stores = new Long[]{1L, 2L, 3L, 4L, 5L};

    // insert lots of entries but only from 5 stores
    for (int i = 0; i < BIG_QUERY_ITERATIONS; i++) {
      feed.addEntry(time--, stores[rand.nextInt(stores.length)],
                    productId++, 1L);
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

    for (int i = 0; i < BIG_QUERY_ITERATIONS; i++) {
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
    for (int i = 0; i < numEntries; i++) {
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
