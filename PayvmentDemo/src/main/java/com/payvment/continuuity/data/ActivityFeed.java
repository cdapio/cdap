package com.payvment.continuuity.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.payvment.continuuity.entity.Product;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.util.Helpers;
import com.google.gson.Gson;

/**
 * An in-memory representation of a Payvment/Lish Activity Feed.
 * <p>
 * An activity feed is a descending time-ordered list of
 * {@link ActivityFeedEntry}s.
 * <p>
 * This class is used to build and store an activity feed.  It is feed a stream
 * of descending time-ordered activity feed entries and performs any required
 * aggregations.
 * <p>
 * Currently this will aggregate all products of the same seller into a single
 * activity feed entry.  The position and timestamp of that entry will be the
 * latest for that seller.
 * <p>
 * <b>JSON Format<b>
 * <pre>
 *    {"activity" : [ { "timestamp" : # ,
 *                     "store_id" : #,
 *                     "products" : [ { "product_id" : #, "score" : # }, ... ]
 *                   }, ... ]}
 * </pre>
 */
public class ActivityFeed {

  /**
   * Descending time-ordered list of activity feed entries.
   */
  public List<ActivityFeedEntry> activity = new ArrayList<ActivityFeedEntry>();

  /**
   * Map from store_id to it's activity feed entry.
   */
  public transient Map<Long,ActivityFeedEntry> stores =
      new TreeMap<Long,ActivityFeedEntry>();

  /**
   * Adds a feed entry to this activity feed, performing any aggregation
   * necessary.
   * @param timestamp
   * @param store_id
   * @param product_id
   * @param score
   */
  public void addEntry(Long timestamp, Long store_id, Long product_id,
      Long score) {
    ActivityFeedEntry entry = this.stores.get(store_id);
    if (entry == null) {
      entry = new ActivityFeedEntry(timestamp, store_id, product_id, score);
      this.stores.put(store_id, entry);
      this.activity.add(entry);
    } else {
      entry.addEntry(product_id, score);
    }
  }

  /**
   * Adds the specified feed entry to this activity feed, utilizing only the
   * first product in the list of the specified feed entry.
   * @param feedEntry a single activity feed entry (will be aggregated,
   *                  if possible)
   */
  public void addEntry(ActivityFeedEntry feedEntry) {
    ActivityFeedEntry existingEntry = this.stores.get(feedEntry.store_id);
    if (existingEntry == null) {
      existingEntry = new ActivityFeedEntry(feedEntry.timestamp,
          feedEntry.store_id, feedEntry.products.get(0).product_id,
          feedEntry.products.get(0).score);
      this.stores.put(feedEntry.store_id, existingEntry);
      this.activity.add(existingEntry);
    } else {
      existingEntry.addEntry(feedEntry.products.get(0).product_id,
          feedEntry.products.get(0).score);
    }
  }

  /**
   * Returns the entries of this activity feed up to the specified limit.
   * <p>
   * Entries are in descending time order.
   * @param limit maximum entries to return
   * @return descending time order list of activity feed entries
   */
  public List<ActivityFeedEntry> getFeed(int limit) {
    return this.activity.subList(0, Math.min(limit, this.activity.size()));
  }

  /**
   * Returns all of the entries of this activity feed.
   * <p>
   * Entries are in descending time order.
   * @return descending time order list of activity feed entries
   */
  public List<ActivityFeedEntry> getEntireFeed() {
    return this.activity;
  }

  /**
   * Returns the number of activity feed entries (after any aggregation, not
   * the total number of entries processed).
   * @return current number of entries in this activity feed
   */
  public int size() {
    return this.activity.size();
  }

  private static final Gson gson = new Gson();

  /**
   * Converts the specified activity feed to it's JSON representation.
   * @param af activity feed
   * @return json string representation of activity feed
   */
  public static String toJson(ActivityFeed af) {
    return gson.toJson(af);
  }

  /**
   * An entry in a Payvment/List Activity Feed.
   * <p>
   * An entry in an activity feed occurs at a specific time and is for one or
   * more products of a single store.
   */
  public static class ActivityFeedEntry {

    public Long timestamp;

    public Long store_id;

    public List<Product> products = new ArrayList<Product>();

    public ActivityFeedEntry(Long timestamp, Long store_id, Long product_id,
        Long score) {
      this.timestamp = timestamp;
      this.store_id = store_id;
      addEntry(product_id, score);
    }

    public ActivityFeedEntry(byte [] column, byte [] value) {
      this(Helpers.reverse(Bytes.toLong(column)), Bytes.toLong(value),
          Bytes.toLong(column, 8), Bytes.toLong(value, 8));
    }

    public ActivityFeedEntry addEntry(Long product_id, Long score) {
      for (Product product : products) {
        if (product_id == product.product_id) {
          product.score += score;
          return this;
        }
      }
      this.products.add(new Product(product_id, score));
      return this;
    }

    public byte [] getColumn() {
      return Bytes.add(Bytes.toBytes(Helpers.reverse(this.timestamp)),
          Bytes.toBytes(this.products.get(0).product_id));
    }

    public byte [] getValue() {
      return Bytes.add(Bytes.toBytes(this.store_id),
          Bytes.toBytes(this.products.get(0).score));
    }
    
    @Override
    public String toString() {
      return gson.toJson(this);
    }
    
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ActivityFeedEntry)) return false;
      ActivityFeedEntry afe = (ActivityFeedEntry)o;
      if (!timestamp.equals(afe.timestamp)) return false;
      if (!store_id.equals(afe.store_id)) return false;
      if (products.size() != afe.products.size()) return false;
      for (int i=0; i<products.size(); i++) {
        if (!products.get(i).product_id.equals(afe.products.get(i).product_id))
          return false;
        if (!products.get(i).score.equals(afe.products.get(i).score))
          return false;
      }
      return true;
    }
  }
}
