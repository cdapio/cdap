package com.payvment.continuuity.data;

import java.util.Random;

import org.junit.Test;

import com.payvment.continuuity.data.ActivityFeed;
import com.payvment.continuuity.data.PopularFeed;

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
    System.out.println(PopularFeed.toJson(feed.getFeed(5)));
  }
  
  // Helper methods
  
  private static final Random rand = new Random();

  public static PopularFeed generateRandomPopularFeed(int numEntries) {
    PopularFeed feed = new PopularFeed();
    for (int i=0; i<numEntries; i++) {
      feed.addEntry(new Long(Math.abs(rand.nextInt())), 1L);
    }
    return feed;
  }
}
