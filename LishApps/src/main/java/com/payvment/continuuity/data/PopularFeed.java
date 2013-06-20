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
import com.google.gson.JsonObject;
import com.payvment.continuuity.Helpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A Payvment/Lish Popular Product Feed.
 * <p/>
 * A popular feed is a descending score-ordered list of products.
 * <p/>
 * This class is used to build and store a popular feed.  It is feed a stream
 * of scored products and performs any necessary sorting and aggregations.
 * <p/>
 * <b>JSON Format</b>
 * <pre>
 *    {"popular" : [ { "productId" : # , "score" : # }, ... ]}
 * </pre>
 */
public class PopularFeed {

  /**
   * Map from product id to score.
   */
  public Map<Long, Long> products = new HashMap<Long, Long>();

  /**
   * Adds or updates the score of the specified product to this popular feed.
   * <p/>
   * If entry for product exists, adds specified score to existing score,
   * otherwise, initializes score to the specified score.
   *
   * @param productId id of product
   * @param score      score of product
   */
  public void addEntry(Long productId, Long score) {
    Long existingScore = this.products.get(productId);

    if (existingScore == null) {
      existingScore = 0L;
    }

    this.products.put(productId, existingScore + score);
  }

  /**
   * Processes and returns the most popular feed entries in this
   * feed in score descending order, up to the specified limit.
   * <p/>
   * This is an expensive operation that performs sorting of all processed
   * entries.
   *
   * @param limit maximum product entries
   * @return list of product entries in descending score order
   */
  public List<PopularFeedEntry> getFeed(int limit) {
    NavigableMap<PopularFeedEntry, PopularFeedEntry> sortedEntries =
      new TreeMap<PopularFeedEntry, PopularFeedEntry>();
    for (Map.Entry<Long, Long> product : this.products.entrySet()) {
      PopularFeedEntry entry = new PopularFeedEntry(product.getKey(),
                                                    product.getValue());
      sortedEntries.put(entry, entry);
    }

    int idx = 0;
    List<PopularFeedEntry> entries = new ArrayList<PopularFeedEntry>(limit);
    for (PopularFeedEntry entry : sortedEntries.keySet()) {
      entries.add(entry);
      if (++idx == limit) {
        break;
      }
    }
    return entries;
  }

  private static final Gson gson = new Gson();

  /**
   * Converts the specified list of popular feed entries to the proper JSON
   * representation.
   *
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
   * <p/>
   * Comparator orders entries in descending score order, then descending
   * product id order.
   */
  public static class PopularFeedEntry implements Comparable<PopularFeedEntry> {

    public Long productId;

    public Long score;

    public PopularFeedEntry(Long productId, Long score) {
      this.productId = productId;
      this.score = score;
    }

    @Override
    public int compareTo(PopularFeedEntry o) {
      if (this.score > o.score) {
        return -1;
      }

      if (this.score < o.score) {
        return 1;
      }

      if (this.productId > o.productId) {
        return -1;
      }

      if (this.productId < o.productId) {
        return 1;
      }

      return 0;
    }

    @Override
    public boolean equals(Object o) {
      return this.compareTo((PopularFeedEntry) o) == 0;
    }
  }

  /**
   * Returns the row key for the specified hour, country, and category popular
   * feed bucket.
   *
   * @param hour     the epoch hour (should be divisible by 3600000)
   * @param country  the country code
   * @param category the category string
   * @return row key
   */
  public static byte[] makeRow(Long hour, String country, String category) {
    if (!hour.equals(Helpers.hour(hour))) {
      System.err.println("Hour in popular feed invalid (" + hour + " not " +
                           Helpers.hour(hour) + ")");
      assert false;
    }
    return Bytes.add(Bytes.toBytes(hour), Bytes.toBytes(country),
                     Bytes.toBytes(category));
  }
}
