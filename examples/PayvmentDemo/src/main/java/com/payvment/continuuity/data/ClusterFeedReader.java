package com.payvment.continuuity.data;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.data.lib.SortedCounterTable;
import com.continuuity.api.data.lib.SortedCounterTable.Counter;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.util.Helpers;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;

/**
 * Performs activity feed and popular feed read queries.
 * <p>
 * For more information on activity feeds, see
 * {@link #getActivityFeed(int, int, long, long)} and {@link ActivityFeed}.
 * <p>
 * For more information on popular feeds, see
 * {@link #getPopularFeed(int, int, int, int)} and {@link PopularFeed}.
 */
public class ClusterFeedReader {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClusterFeedReader.class);

  private final ActivityFeedTable activityFeedTable;

  private final ClusterTable clusterTable;

  private final SortedCounterTable topScoreTable;

  public ClusterFeedReader(ClusterTable clusterTable,
      SortedCounterTable topScoreTable, ActivityFeedTable activityFeedTable) {
    this.clusterTable = clusterTable;
    this.topScoreTable = topScoreTable;
    this.activityFeedTable = activityFeedTable;
  }

  public ActivityFeed getActivityFeed(String country, int clusterId, int limit)
      throws OperationException {
    return getActivityFeed(country, clusterId, limit, Long.MAX_VALUE, 0L);
  }

  /**
   * Reads the activity feed for the specified cluster containing entries with
   * timestamps less than the max stamp and greater than the min stamp, up to
   * the specified limit.
   * <p>
   * See {@link ActivityFeed} javadoc for JSON format.
   * @param country the country code
   * @param clusterId cluster id
   * @param limit maximum entries
   * @param maxStamp maximum stamp, exclusive
   * @param minStamp minimum stamp, exclusive
   * @return activity feed of product entries in descending time order
   * @throws OperationException
   */
  public ActivityFeed getActivityFeed(String country, int clusterId, int limit,
      long maxStamp, long minStamp) throws OperationException {
    // Read cluster info
    Map<String, Double> clusterInfo = this.clusterTable.readCluster(clusterId);
    if (clusterInfo == null || clusterInfo.isEmpty()) {
      String str = "Cluster not found (id=" + clusterId + ")";
      LOG.warn(str);
      throw new OperationException(StatusCode.KEY_NOT_FOUND, str);
    }
    // For each category, open caching sorted lists on activity feeds
    // starting from max_stamp to min_stamp (but each reversed)
    List<CachingActivityFeedScanner> scanners =
        new ArrayList<CachingActivityFeedScanner>(clusterInfo.size());
    for (Map.Entry<String, Double> entry : clusterInfo.entrySet()) {
      String category = entry.getKey();
      Double weight = entry.getValue(); // TODO: Do something with weight
      CachingActivityFeedScanner scanner = new CachingActivityFeedScanner(
          this.activityFeedTable, country, category, weight, maxStamp, minStamp,
          limit);
      scanners.add(scanner);
    }
    // Heap merge (PriQueue) across all scanners
    PriorityQueue<ActivityFeedMergeEntry> headMerge =
        new PriorityQueue<ActivityFeedMergeEntry>(scanners.size());
    // Initialize priority queue with top entries in each scanner
    for (CachingActivityFeedScanner scanner : scanners) {
      ActivityFeedEntry activityFeedEntry = scanner.next();
      if (activityFeedEntry == null) continue;
      headMerge.add(new ActivityFeedMergeEntry(activityFeedEntry, scanner));
    }
    // Initialize activity feed
    ActivityFeed activityFeed = new ActivityFeed();
    // Loop until activity feed has fulfilled limit
    while (activityFeed.size() < limit) {
      // Grab top entry in priority queue
      ActivityFeedMergeEntry topEntry = headMerge.poll();
      // Stop if empty
      if (topEntry == null) break;
      // Add entry to activity feed
      ActivityFeedEntry feedEntry = topEntry.getEntry();
      activityFeed.addEntry(feedEntry);
      // Grab next entry from current entry scanner and add back to pri queue
      ActivityFeedEntry nextFeedEntry = topEntry.scanner.next();
      if (nextFeedEntry != null) {
        headMerge.add(new ActivityFeedMergeEntry(nextFeedEntry,
            topEntry.scanner));
      }
    }
    // Close scanners (probably a no-op for now)
    for (CachingActivityFeedScanner scanner : scanners) scanner.close();
    return activityFeed;
  }

  /**
   * Reads the popular feed for the specified cluster, determining the most
   * popular products from the number of hours specified.  Resulting list of
   * popular products can be paged using limit and offset.
   * <p>
   * See {@link PopularFeed} javadoc for JSON format.
   * @param country the country code
   * @param clusterId cluster id
   * @param numHours number of hours (must be >= 1)
   * @param limit maximum number of products to return
   * @param offset number of products offset from most popular to return
   * @return feed of products in descending popularity order
   * @throws OperationException
   */
  public PopularFeed getPopularFeed(String country, int clusterId, int numHours,
      int limit, int offset) throws OperationException {
    return getPopularFeed(country, clusterId,
        Helpers.hour(System.currentTimeMillis()), numHours, limit, offset);
  }

  /**
   * Reads the popular feed for the specified cluster, determining the most
   * popular products from the number of hours specified.  Resulting list of
   * popular products can be paged using limit and offset.  Uses the specified
   * currentHour rather than actual currentHour.
   * <p>
   * See {@link PopularFeed} javadoc for JSON format.
   * @param country the country code
   * @param clusterId cluster id
   * @param currentHour the current hour, in epoch millis
   * @param numHours number of hours (must be >= 1)
   * @param limit maximum number of products to return
   * @param offset number of products offset from most popular to return
   * @return feed of products in descending popularity order
   * @throws OperationException
   */
  public PopularFeed getPopularFeed(String country, int clusterId,
      long currentHour, int numHours, int limit, int offset)
          throws OperationException {
    int n = limit + offset;
    if (numHours < 1 || limit < 1 || offset < 0) {
      throw new OperationException(StatusCode.KEY_NOT_FOUND,
          "Invalid input argument");
    }
    // Construct PopularFeed which handles all aggregation of product scores
    PopularFeed popFeed = new PopularFeed();
    // Read cluster info
    Map<String,Double> clusterInfo = this.clusterTable.readCluster(clusterId);
    if (clusterInfo == null || clusterInfo.isEmpty()) {
      String str = "Cluster not found (id=" + clusterId + ")";
      LOG.warn(str);
      return popFeed;
    }
    // Iterate categories and for each category iterate hours
    // (total iterations = # categories * # hours)
    for (Map.Entry<String,Double> entry : clusterInfo.entrySet()) {
      String category = entry.getKey();
      @SuppressWarnings("unused")
      Double weight = entry.getValue(); // TODO: Do something with weight
      List<Long> hours = new ArrayList<Long>(numHours);
      for (int i=0; i<numHours; i++) {
        hours.add(currentHour - (i * 3600000));
      }
      // Iterate hours
      for (Long hour : hours) {
        // Grab top counters for this category and this hour
        List<Counter> topCounters = this.topScoreTable.readTopCounters(
            PopularFeed.makeRow(hour, country, category), n);
        for (Counter counter : topCounters) {
          // Add each counter to pop feed
          popFeed.addEntry(Bytes.toLong(counter.getName()), counter.getCount());
        }
      }
    }
    return popFeed;
  }

  /**
   * Used in priority queue of activity feed entries.
   * <p>
   * Contains an activity feed entry and the scanner it came from.  Orders by
   * descending time, descending weight, ascending category.
   */
  private class ActivityFeedMergeEntry
  implements Comparable<ActivityFeedMergeEntry> {

    ActivityFeedEntry entry;

    CachingActivityFeedScanner scanner;

    public ActivityFeedMergeEntry(ActivityFeedEntry feedEntry,
        CachingActivityFeedScanner scanner) {
      this.entry = feedEntry;
      this.scanner = scanner;
    }

    public ActivityFeedEntry getEntry() {
      return this.entry;
    }

    /**
     * Orders in descending time, then descending weight,
     * then ascending category.
     */
    @Override
    public int compareTo(ActivityFeedMergeEntry o) {
      if (this.entry.timestamp > o.entry.timestamp) return -1;
      if (this.entry.timestamp < o.entry.timestamp) return 1;
      if (this.scanner.weight > o.scanner.weight) return -1;
      if (this.scanner.weight < o.scanner.weight) return 1;
      return this.scanner.category.compareTo(o.scanner.category);
    }

  }

  /**
   * Scanner of the activity feed of a single category.
   * <p>
   * Caches batches of entries at a time according to a specified page size.
   */
  private class CachingActivityFeedScanner {

    final ActivityFeedTable table;

    final String country;

    final String category;

    final Double weight;

    long maxStamp;

    final long minStamp;

    @SuppressWarnings("unused")
    final int limit;

    final int pageSize;

    /**
     * Whether this scanner has reached the end (including emptying cache).
     */
    private boolean done = false;

    /**
     * Whether the cache filler has hit the end (not necessarily cache empty).
     */
    private boolean cacheFillEnd = false;

    /**
     * Cache of activity feed entries.
     */
    private final LinkedList<ActivityFeedEntry> cache =
        new LinkedList<ActivityFeedEntry>();

    CachingActivityFeedScanner(ActivityFeedTable table, String country,
        String category, Double weight, long maxStamp, long minStamp, int limit)
            throws OperationException {
      this.table = table;
      this.country = country;
      this.category = category;
      this.weight = weight;
      this.maxStamp = maxStamp;
      this.minStamp = minStamp;
      this.limit = limit;
      this.pageSize = limit;
      fillCache();
    }

    /**
     * Returns next entry from scanner and moves pointer to next entry.
     * @return next entry in scanner
     * @throws OperationException
     */
    public ActivityFeedEntry next() throws OperationException {
      if (this.done) return null;
      if (this.cache.isEmpty()) {
        // Cache empty, attempt to fill
        boolean filled = fillCache();
        if (!filled) {
          // Nothing left, scanner is done
          this.done = true;
          return null;
        }
      }
      // Entries are in cache, return head of cache
      return this.cache.poll();
    }

    public void close() {}

    /**
     * Fills the cache according to the page size.  Will fill cache even if
     * there are existing entries in cache.
     * @return true if anything was put into cache, false if read was empty
     * @throws OperationException
     */
    private boolean fillCache() throws OperationException {
      if (cacheFillEnd) return false;
      List<ActivityFeedEntry> entries = this.table.readEntries(
          country, category, pageSize, maxStamp, minStamp);
      if (entries.isEmpty()) {
        cacheFillEnd = true;
        return false;
      }
      // Iterate result and add feed entries
      for (ActivityFeedEntry feedEntry : entries) {
        this.cache.add(feedEntry);
      }
      // If result is less than a page, we reached the end of the feed
      if (entries.size() < pageSize) {
        cacheFillEnd = true;
      }
      // Update the maxStamp to point to the timestamp of the last entry
      this.maxStamp = entries.get(entries.size() - 1).timestamp;
      return true;
    }
  }
}
