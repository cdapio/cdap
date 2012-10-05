package com.payvment.continuuity.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.gson.Gson;

/**
 * A Payvment/Lish Popular Product Feed.
 * <p>
 * A popular feed is a descending score-ordered list of products.
 * <p>
 * This class is used to build and store a popular feed.  It is feed a stream
 * of scored products and performs any necessary sorting and aggregations.
 */
public class PopularFeed {

  /**
   * Map from product id to score.
   */
  public Map<Long,Long> products = new HashMap<Long,Long>();

  /**
   * Adds or updates the score of the specified product to this popular feed.
   * <p>
   * If entry for product exists, adds specified score to existing score,
   * otherwise, initializes score to the specified score.
   * @param product_id id of product
   * @param score score of product
   */
  public void addEntry(Long product_id, Long score) {
    Long existingScore = this.products.get(product_id);
    if (existingScore == null) existingScore = 0L;
    this.products.put(product_id, existingScore + score);
  }

  /**
   * Processes and returns the most popular feed entries in this
   * feed in score descending order, up to the specified limit.
   * <p>
   * This is an expensive operation that performs sorting of all processed
   * entries.
   * @param limit maximum product entries
   * @return list of product entries in descending score order
   */
  public List<PopularFeedEntry> getFeed(int limit) {
    NavigableMap<PopularFeedEntry,PopularFeedEntry> sortedEntries =
        new TreeMap<PopularFeedEntry,PopularFeedEntry>();
    for (Map.Entry<Long,Long> product : this.products.entrySet()) {
      PopularFeedEntry entry = new PopularFeedEntry(product.getKey(),
          product.getValue());
      sortedEntries.put(entry, entry);
    }
    int idx = 0;
    List<PopularFeedEntry> entries = new ArrayList<PopularFeedEntry>(limit);
    for (PopularFeedEntry entry : sortedEntries.keySet()) {
      entries.add(entry);
      if (++idx == limit) break;
    }
    return entries;
  }

  private static final Gson gson = new Gson();

  /**
   * Converts the specified list of popular feed entries to the proper JSON
   * representation.
   * @param feedEntries popular feed entries
   * @return json string representation of popular feed
   */
  public static String toJson(List<PopularFeedEntry> feedEntries) {
    return gson.toJson(feedEntries);
  }

  /**
   * Product entry in popular feed.  Contains a product id and score.
   * <p>
   * Comparator orders entries in descending score order, then descending
   * product id order.
   */
  public static class PopularFeedEntry implements Comparable<PopularFeedEntry> {

    public Long product_id;

    public Long score;

    public PopularFeedEntry(Long product_id, Long score) {
      this.product_id = product_id;
      this.score = score;
    }

    @Override
    public int compareTo(PopularFeedEntry o) {
      if (this.score > o.score) return -1;
      if (this.score < o.score) return 1;
      if (this.product_id > o.product_id) return -1;
      if (this.product_id < o.product_id) return 1;
      return 0;
    }

  }
}
