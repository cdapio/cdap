package com.payvment.continuuity.data;

import static org.junit.Assert.assertEquals;

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
