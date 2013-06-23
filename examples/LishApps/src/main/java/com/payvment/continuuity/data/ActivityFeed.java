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

import com.continuuity.api.common.Bytes;
import com.google.gson.Gson;
import com.payvment.continuuity.Helpers;
import com.payvment.continuuity.entity.Product;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

//import com.continuuity.api.data.util.Helpers;

/**
 * An in-memory representation of a Payvment/Lish Activity Feed.
 * <p/>
 * An activity feed is a descending time-ordered list of
 * {@link ActivityFeedEntry}s.
 * <p/>
 * This class is used to build and store an activity feed.  It is feed a stream
 * of descending time-ordered activity feed entries and performs any required
 * aggregations.
 * <p/>
 * Currently this will aggregate all products of the same seller into a single
 * activity feed entry.  The position and timestamp of that entry will be the
 * latest for that seller.
 * <p/>
 * <b>JSON Format</b>
 * <pre>
 *    {"activity" : [ { "timestamp" : # ,
 *                     "storeId" : #,
 *                     "products" : [ { "productId" : #, "score" : # }, ... ]
 *                   }, ... ]}
 * </pre>
 */
public class ActivityFeed {

  /**
   * Descending time-ordered list of activity feed entries.
   */
  public List<ActivityFeedEntry> activity = new ArrayList<ActivityFeedEntry>();

  /**
   * Map from storeId to it's activity feed entry.
   */
  public transient Map<Long, ActivityFeedEntry> stores = new TreeMap<Long, ActivityFeedEntry>();

  /**
   * Adds a feed entry to this activity feed, performing any aggregation
   * necessary.
   *
   * @param timestamp
   * @param storeId
   * @param productId
   * @param score
   */
  public void addEntry(Long timestamp, Long storeId, Long productId, Long score) {
    ActivityFeedEntry entry = this.stores.get(storeId);
    if (entry == null) {
      entry = new ActivityFeedEntry(timestamp, storeId, productId, score);
      this.stores.put(storeId, entry);
      this.activity.add(entry);
    } else {
      entry.addEntry(productId, score);
    }
  }

  /**
   * Adds the specified feed entry to this activity feed, utilizing only the
   * first product in the list of the specified feed entry.
   *
   * @param feedEntry a single activity feed entry (will be aggregated,
   *                  if possible)
   */
  public void addEntry(ActivityFeedEntry feedEntry) {
    ActivityFeedEntry existingEntry = this.stores.get(feedEntry.storeId);

    if (existingEntry == null) {
      existingEntry = new ActivityFeedEntry(feedEntry.timestamp,
                                            feedEntry.storeId,
                                            feedEntry.products.get(0).productId,
                                            feedEntry.products.get(0).score);

      this.stores.put(feedEntry.storeId, existingEntry);
      this.activity.add(existingEntry);
    } else {
      existingEntry.addEntry(feedEntry.products.get(0).productId, feedEntry.products.get(0).score);
    }
  }

  /**
   * Returns the entries of this activity feed up to the specified limit.
   * <p/>
   * Entries are in descending time order.
   *
   * @param limit maximum entries to return
   * @return descending time order list of activity feed entries
   */
  public List<ActivityFeedEntry> getFeed(int limit) {
    return this.activity.subList(0, Math.min(limit, this.activity.size()));
  }

  /**
   * Returns all of the entries of this activity feed.
   * <p/>
   * Entries are in descending time order.
   *
   * @return descending time order list of activity feed entries
   */
  public List<ActivityFeedEntry> getEntireFeed() {
    return this.activity;
  }

  /**
   * Returns the number of activity feed entries (after any aggregation, not
   * the total number of entries processed).
   *
   * @return current number of entries in this activity feed
   */
  public int size() {
    return this.activity.size();
  }

  private static final Gson gson = new Gson();

  /**
   * Converts the specified activity feed to it's JSON representation.
   *
   * @param af activity feed
   * @return json string representation of activity feed
   */
  public static String toJson(ActivityFeed af) {
    return gson.toJson(af);
  }

  /**
   * An entry in a Payvment/List Activity Feed.
   * <p/>
   * An entry in an activity feed occurs at a specific time and is for one or
   * more products of a single store.
   */
  public static class ActivityFeedEntry {

    public Long timestamp;

    public Long storeId;

    public List<Product> products = new ArrayList<Product>();

    public ActivityFeedEntry(Long timestamp, Long storeId, Long productId, Long score) {
      this.timestamp = timestamp;
      this.storeId = storeId;
      addEntry(productId, score);
    }


    public ActivityFeedEntry(byte[] column, byte[] value) {
      this(Helpers.reverse(Bytes.toLong(column)), Bytes.toLong(value), Bytes.toLong(column, 8), Bytes.toLong(value, 8));
    }

    public ActivityFeedEntry addEntry(Long productId, Long score) {
      for (Product product : products) {
        if (productId == product.productId) {
          product.score += score;
          return this;
        }
      }
      this.products.add(new Product(productId, score));
      return this;
    }

    public byte[] getColumn() {
      return Bytes.add(Bytes.toBytes(Helpers.reverse(this.timestamp)), Bytes.toBytes(this.products.get(0).productId));
    }

    public byte[] getValue() {
      return Bytes.add(Bytes.toBytes(this.storeId), Bytes.toBytes(this.products.get(0).score));
    }

    @Override
    public String toString() {
      return gson.toJson(this);
    }

    @Override
    public boolean equals(Object o) {

      if (!(o instanceof ActivityFeedEntry)) {
        return false;
      }

      ActivityFeedEntry afe = (ActivityFeedEntry) o;
      if (!timestamp.equals(afe.timestamp)) {
        return false;
      }

      if (!storeId.equals(afe.storeId)) {
        return false;
      }

      if (products.size() != afe.products.size()) {
        return false;
      }

      for (int i = 0; i < products.size(); i++) {
        if (!products.get(i).productId.equals(afe.products.get(i).productId)) {
          return false;
        }

        if (!products.get(i).score.equals(afe.products.get(i).score)) {
          return false;
        }
      }

      return true;
    }
  }
}
