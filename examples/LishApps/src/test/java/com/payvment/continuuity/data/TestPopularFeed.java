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

import com.payvment.continuuity.data.PopularFeed.PopularFeedEntry;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Isolated tests of the {@link ActivityFeed} class.
 */
public class TestPopularFeed {

  @Test
  public void testJsonSerialization() {

    // Generate an activity feed with 3 entries
    PopularFeed feed = generateRandomPopularFeed(3);
    System.out.println(PopularFeed.toJson(feed.getFeed(5)));

    // Generate an activity feed with aggregated entries
    feed = new PopularFeed();
    feed.addEntry(1L, 1L);
    feed.addEntry(1L, 1L);
    feed.addEntry(2L, 1L);
    feed.addEntry(2L, 3L);
    feed.addEntry(3L, 5L);
    feed.addEntry(1L, 1L);
    String popJson = PopularFeed.toJson(feed.getFeed(5));
    System.out.println(popJson);

    // Verify JSON is exactly what we expect it to be
    // (NOTE: this needs to change if JSON format of popular feed changes)
    String expectedJson = "{\"popular\":[{\"product_id\":3,\"score\":5}," +
      "{\"product_id\":2,\"score\":4},{\"product_id\":1,\"score\":3}]}";

    assertEquals(expectedJson, popJson);

    // Test 
  }

  @Test
  public void testProcessingAndAggregation() {

    PopularFeed feed = new PopularFeed();

    // Start with three products with scores of equal to product id (1, 2, 3)
    Long[] products = new Long[]{1L, 2L, 3L};
    feed.addEntry(products[0], 1L);
    feed.addEntry(products[1], 2L);
    feed.addEntry(products[2], 3L);

    // Three entries, same order as insertion
    List<PopularFeedEntry> entries = feed.getFeed(5);
    assertEquals(3, entries.size());
    assertTrue(entries.get(0).toString(),
               new PopularFeedEntry(3L, 3L).equals(entries.get(0)));
    assertTrue(new PopularFeedEntry(2L, 2L).equals(entries.get(1)));
    assertTrue(new PopularFeedEntry(1L, 1L).equals(entries.get(2)));

    // Verify limits of 1 and 2 work
    entries = feed.getFeed(2);
    assertEquals(2, entries.size());
    assertTrue(entries.get(0).toString(),
               new PopularFeedEntry(3L, 3L).equals(entries.get(0)));
    assertTrue(new PopularFeedEntry(2L, 2L).equals(entries.get(1)));
    entries = feed.getFeed(1);
    assertEquals(1, entries.size());
    assertTrue(entries.get(0).toString(),
               new PopularFeedEntry(3L, 3L).equals(entries.get(0)));

    // Add aggregated scores to the first and third products
    feed.addEntry(1L, 10L);
    feed.addEntry(3L, 5L);

    // Still just three entries
    entries = feed.getFeed(5);
    assertEquals(3, entries.size());
    assertEquals(3, entries.size());
    assertTrue(entries.get(0).toString(),
               new PopularFeedEntry(1L, 11L).equals(entries.get(0)));
    assertTrue(new PopularFeedEntry(3L, 8L).equals(entries.get(1)));
    assertTrue(new PopularFeedEntry(2L, 2L).equals(entries.get(2)));
  }

  private static final int BIG_QUERY_ITERATIONS = 10000;

  @Test
  public void testAggregationAndSortOrder_RandomBigQuery() {
    PopularFeed feed = new PopularFeed();

    // only 5 products
    Long[] products = new Long[]{1L, 2L, 3L, 4L, 5L};

    // insert lots of entries but only from 5 products
    for (int i = 0; i < BIG_QUERY_ITERATIONS; i++) {
      feed.addEntry(products[rand.nextInt(products.length)],
                    1L);
    }

    // feed should only have 5 entries
    List<PopularFeedEntry> entries = feed.getFeed(10);
    assertEquals(5, entries.size());

    // should be in decreasing score order
    // and total score should = BIG_QUERY_ITERATIONS
    int totalScore = 0;
    long lastScore = Long.MAX_VALUE;
    for (PopularFeedEntry entry : entries) {
      totalScore += entry.score;
      assertTrue(entry.score <= lastScore);
      lastScore = entry.score;
    }
    assertEquals(BIG_QUERY_ITERATIONS, totalScore);
  }

  // Helper methods

  private static final Random rand = new Random();

  public static PopularFeed generateRandomPopularFeed(int numEntries) {
    PopularFeed feed = new PopularFeed();
    for (int i = 0; i < numEntries; i++) {
      feed.addEntry(new Long(Math.abs(rand.nextInt())), 1L);
    }
    return feed;
  }
}
