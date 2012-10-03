package com.continuuity.payvment.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.continuuity.payvment.entity.Product;
import com.continuuity.payvment.util.Bytes;
import com.continuuity.payvment.util.Helpers;

/**
 * A Payvment/Lish Activity Feed.
 * <p>
 * An activity feed is a descending time-ordered list of
 * {@link ActivityFeedEntry}s.
 * <p>
 * This class is used to build and store an activity feed.  It is feed a stream
 * of activity feed entries and performs any necessary aggregations.
 * <p>
 * Currently this will aggregate all products of the same seller into a single
 * activity feed entry.
 */
public class ActivityFeed {

  /**
   * Descending time-ordered list of activity feed entries.
   */
  public List<ActivityFeedEntry> entries = new ArrayList<ActivityFeedEntry>();

  /**
   * Map from store_id to it's activity feed entry.
   */
  public Map<Long,ActivityFeedEntry> stores =
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
      this.entries.add(entry);
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
      this.entries.add(existingEntry);
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
    return this.entries.subList(0, Math.min(limit, this.entries.size()));
  }

  /**
   * Returns all of the entries of this activity feed.
   * <p>
   * Entries are in descending time order.
   * @return descending time order list of activity feed entries
   */
  public List<ActivityFeedEntry> getEntireFeed() {
    return this.entries;
  }

  /**
   * Returns the number of activity feed entries (after any aggregation, not
   * the total number of entries processed).
   * @return current number of entries in this activity feed
   */
  public int size() {
    return this.entries.size();
  }

  public static byte [] makeActivityFeedRow(String category) {
    return Bytes.add(Bytes.toBytes("activityFeed"), Bytes.toBytes(category));
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
  }
}
