package com.payvment.continuuity.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.util.Helpers;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * A Payvment/Lish Popular Product Feed.
 * <p>
 * A popular feed is a descending score-ordered list of products.
 * <p>
 * This class is used to build and store a popular feed.  It is feed a stream
 * of scored products and performs any necessary sorting and aggregations.
 * <p>
 * <b>JSON Format<b>
 * <pre>
 *    {"popular" : [ { "product_id" : # , "score" : # }, ... ]}
 * </pre>
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
    JsonObject obj = new JsonObject();
    obj.add("popular", gson.toJsonTree(feedEntries));
    return obj.toString();
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

    @Override
    public boolean equals(Object o) {
      return this.compareTo((PopularFeedEntry)o) == 0;
    }
  }

  /**
   * Returns the row key for the specified hour, country, and category popular
   * feed bucket.
   * @param hour the epoch hour (should be divisible by 3600000)
   * @param country the country code
   * @param category the category string
   * @return row key
   */
  public static byte [] makeRow(Long hour, String country, String category) {
    if (!hour.equals(Helpers.hour(hour))) {
      System.err.println("Hour in popular feed invalid (" + hour + " not " +
          Helpers.hour(hour) + ")");
      assert false;
    }
    return Bytes.add(Bytes.toBytes(hour), Bytes.toBytes(country),
        Bytes.toBytes(category)); 
  }
}
