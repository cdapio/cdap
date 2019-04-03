/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.messaging.cache;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.messaging.store.MessageFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;

/**
 * An in-memory cache for messages. This cache is expected to be shared between publishers and fetchers of the same
 * topic. This cache is specifically designed for the TMS operations, hence leveraging certain properties from TMS:
 *
 * - Single writer, concurrent fetchers
 * - Ordered, unique entry (row key)
 *
 * This cache uses three memory limits to balance between publish and consume efficiency as well as
 * bounding the memory usage. It uses a provided {@link Weigher} to compute the weight of each entry
 * being stored inside the cache.
 *
 * - Hard limit. This is the upper bound weight for the cache and it won't grow beyond this.
 * - Min retain. This is the minimum weight that the cache will try to maintain.
 * - Reduce trigger. This is the cache weight that triggers the logic for reducing the cache size back to the
 *   min retain weight. When the cache weight is larger than this limit, weight reduction logic will be executed
 *   by the consumer. On adding entries to the cache, the cache can keep growing without blocking as long as
 *   the hard limit is not hit so that the publisher doesn't need to be blocked.
 *   The room between the reduce trigger and hard limits is basically the buffer for non-blocking addition.
 *   - On addition, once the hard limit is reached, a blocking operation is needed to reduce the weight of the
 *     cache back to min retain.
 *   - On fetching entries from the cache, the fetcher will check whether it needs to reduce the cache weight and reduce
 *     it if needed. This essentially is to amortize the cost of the blocking weight reduction operations among all
 *     fetchers (which typically has multiple of them), without blocking the single publish as much as possible.
 *
 * @param <T> type of entry stored in the cache
 */
public class MessageCache<T> {

  private static final String METRICS_WEIGHT = "cache.weight";
  private static final String METRICS_ENTRIES_ADDED = "cache.entries.added";
  private static final String METRICS_ENTRIES_REMOVED = "cache.entries.removed";
  private static final String METRICS_ADD_REQUESTS = "cache.add.requests";
  private static final String METRICS_ADD_REDUCE_WEIGHT = "cache.add.reduce.weight";
  private static final String METRICS_SCAN_REQUESTS = "cache.scan.requests";
  private static final String METRICS_SCAN_REDUCE_WEIGHT = "cache.scan.reduce.weight";

  private final NavigableSet<CacheEntry<T>> cache;
  private final Comparator<T> comparator;
  private final AtomicReference<Limits> limits;
  private final MetricsContext metricsContext;
  private final AtomicLong currentWeight;
  private final AtomicBoolean needReduceWeight;
  private final AtomicBoolean adding;
  private final Weigher<T> weigher;
  private final ReadWriteLock cacheLock;

  /**
   * Creates a new instance of the cache.
   *
   * @param comparator a {@link Comparator} for ordering cache entries
   * @param weigher a {@link Weigher} for computing the weight of each cache entry
   * @param limits the limits for maintaining cache weight; see class description for more detail
   * @param metricsContext a {@link MetricsContext} for emitting metrics about this cache.
   */
  public MessageCache(Comparator<T> comparator, Weigher<T> weigher, Limits limits, MetricsContext metricsContext) {
    this.cache = new ConcurrentSkipListSet<>(new CacheEntryComparator<>(comparator));
    this.comparator = comparator;
    this.limits = new AtomicReference<>(limits);
    this.metricsContext = metricsContext;
    this.currentWeight = new AtomicLong();
    this.needReduceWeight = new AtomicBoolean();
    this.adding = new AtomicBoolean();
    this.weigher = weigher;
    this.cacheLock = new ReentrantReadWriteLock();
  }

  /**
   * Returns the {@link Comparator} used by this cache.
   */
  public Comparator<T> getComparator() {
    return comparator;
  }

  /**
   * Adds a list of entries to the cache. The entries provided must be in strictly increasing order and should be
   * larger than existing entries in the cache. Also, this method doesn't allow concurrent invocation.
   *
   * @param entries a {@link Iterator} to provide entries to be added to the cache
   * @throws ConcurrentModificationException if called by multiple threads concurrently
   * @throws IllegalArgumentException if the entries provided are not in strictly increasing order
   *                                  or not larger existing cached entries
   */
  public void addAll(Iterator<T> entries) {
    if (!adding.compareAndSet(false, true)) {
      // This is to guard against bug, otherwise this shouldn't happen
      throw new ConcurrentModificationException(
        "The MessageCache.addAll method shouldn't be called concurrently by multiple threads.");
    }

    try {
      long newWeight = 0L;
      CacheEntry<T> largestCacheEntry = null;

      int entriesAdded = 0;
      while (entries.hasNext()) {
        T entry = entries.next();
        CacheEntry<T> cacheEntry = new CacheEntry<>(entry, weigher.weight(entry));
        newWeight = currentWeight.addAndGet(cacheEntry.getWeight());
        if (newWeight > limits.get().getHardLimit()) {
          reduceWeight();
          metricsContext.increment(METRICS_ADD_REDUCE_WEIGHT, 1L);
          newWeight = currentWeight.get();
        }

        // Make sure new entries are also in increasing order.
        // For the first entry from the provided iterator, it must be larger than everything in the cache, hence
        // the ceiling call must be returning null.
        // For sub-sequence entries in the iterator, they must be in strictly increasing order
        largestCacheEntry = largestCacheEntry == null ? cache.ceiling(cacheEntry) : largestCacheEntry;
        if (largestCacheEntry != null && comparator.compare(largestCacheEntry.getEntry(), cacheEntry.getEntry()) >= 0) {
          // Entries must be in strictly increasing order
          // Clear the cache to reset state. This is just for precaution, as this shouldn't happen,
          // unless there is bug in the TMS system (from the caller side).
          currentWeight.addAndGet(-1 * cacheEntry.getWeight());
          clear();
          throw new IllegalArgumentException("Cache entry must be in strictly increasing order. " +
                                               "Entry " + entry + " is smaller than or equal to " +
                                               largestCacheEntry.getEntry());
        }

        // It's ok to "leak" this to reader even if the new weight is larger than the hard limit
        // The entry will get removed eventually and the read/write operations as a whole still give valid
        // results
        cache.add(cacheEntry);
        entriesAdded++;
        largestCacheEntry = cacheEntry;
      }

      metricsContext.increment(METRICS_ADD_REQUESTS, 1L);
      metricsContext.increment(METRICS_ENTRIES_ADDED, entriesAdded);
      metricsContext.gauge(METRICS_WEIGHT, newWeight);

      if (newWeight > limits.get().getHardLimit()) {
        reduceWeight();
        metricsContext.increment(METRICS_ADD_REDUCE_WEIGHT, 1L);
      } else if (newWeight > limits.get().getReduceTrigger()) {
        needReduceWeight.compareAndSet(false, true);
      }
    } finally {
      adding.set(false);
    }
  }

  /**
   * Creates a {@link Scanner} for fetching cached entries in ascending order.
   *
   * @param startEntry the entry to start fetching from
   * @param includeStart {@code true} to include the startEntry in the resulting {@link Scanner}
   *                                 if it exists in the cache
   * @param limit maximum number of entries to fetch
   * @return a {@link Scanner} for accessing to the fetched entries
   */
  public Scanner<T> scan(T startEntry, boolean includeStart, int limit, MessageFilter<T> filter) {
    List<T> entries = new LinkedList<>();

    // Acquire the read lock and copy the entries. This is to guard against weight reduction while the caller
    // is iterating using the returned Scanner.
    cacheLock.readLock().lock();
    T firstInCache;
    try {
      firstInCache = cache.isEmpty() ? null : cache.first().getEntry();
      for (CacheEntry<T> cacheEntry : cache.tailSet(new CacheEntry<>(startEntry, 0), includeStart)) {
        if (entries.size() >= limit) {
          break;
        }

        MessageFilter.Result result = filter.apply(cacheEntry.getEntry());
        if (result == MessageFilter.Result.ACCEPT) {
          entries.add(cacheEntry.getEntry());
        } else if (result == MessageFilter.Result.HOLD) {
          // Hold means not to scan more, so just break
          break;
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    metricsContext.increment(METRICS_SCAN_REQUESTS, 1L);

    return new AbstractScanner<T>(entries.iterator(), firstInCache) {
      @Override
      void doClose() {
        // Use compareAndSet to check if need to reduce weight. There will only be
        // one winner to proceed with the reduce weight call.
        if (needReduceWeight.compareAndSet(true, false)) {
          reduceWeight();
          metricsContext.increment(METRICS_SCAN_REDUCE_WEIGHT, 1L);
        }
      }
    };
  }

  /**
   * Updates entries in the cache. Update to each entry shouldn't change the ordering of the entry based on the
   * {@link Comparator} provided to this cache.
   *
   * @param startEntry the starting entry for the update to start (inclusive)
   * @param endEntry the ending entry for the update to end (inclusive)
   * @param updater a {@link EntryUpdater} to update the content of a entry
   */
  public void updateEntries(T startEntry, T endEntry, EntryUpdater<T> updater) {
    CacheEntry<T> startCacheEntry = new CacheEntry<>(startEntry, 0);

    cacheLock.writeLock().lock();
    try {
      CacheEntry<T> lower = cache.lower(startCacheEntry);
      Iterator<CacheEntry<T>> iterator = cache.subSet(startCacheEntry, true,
                                                      new CacheEntry<>(endEntry, 0), true).iterator();
      CacheEntry<T> cacheEntry = iterator.hasNext() ? iterator.next() : null;
      while (cacheEntry != null) {
        CacheEntry<T> nextCacheEntry = iterator.hasNext() ? iterator.next() : null;
        CacheEntry<T> higher = nextCacheEntry == null ? cache.higher(cacheEntry) : nextCacheEntry;

        try {
          updater.updateEntry(cacheEntry.getEntry());
        } catch (RuntimeException e) {
          clear();
          throw e;
        }

        // A quick check that the ordering hasn't been altered.
        // It doesn't cover all possible case though. This is just a quick catch for bug in the caller.
        if ((lower != null && comparator.compare(lower.getEntry(), cacheEntry.getEntry()) >= 0)
            || (higher != null && comparator.compare(higher.getEntry(), cacheEntry.getEntry()) <= 0)) {
          // This shouldn't happen, unless there is bug in the caller.
          clear();
          throw new IllegalStateException("Entry order should not be altered after update.");
        }

        lower = cacheEntry;
        cacheEntry = nextCacheEntry;
      }

    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  /**
   * Clears the cache. The caller is responsible to make sure there is no concurrent call to the
   * {@link #addAll(Iterator)} method.
   */
  public void clear() {
    // To clear the cache, first set the limit to 0, the reset it back to proper limit
    Limits oldLimits = limits.get();
    resize(new Limits(0, 0, 0));
    resize(oldLimits);
  }

  /**
   * Resize the cache limits.
   *
   * @param limits the new limits for this cache.
   */
  public void resize(Limits limits) {
    cacheLock.writeLock().lock();
    try {
      this.limits.set(limits);
      reduceWeight();
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  /**
   * Returns the current soft and hard limits of this cache.
   */
  public Limits getLimits() {
    return limits.get();
  }

  /**
   * Returns the current cache weight.
   */
  @VisibleForTesting
  long getCurrentWeight() {
    return currentWeight.get();
  }

  /**
   * Reduces the cache weight. Cached entries will be removed until the cache weight is smaller than the soft limit.
   */
  private void reduceWeight() {
    int entriesRemoved = 0;
    cacheLock.writeLock().lock();
    try {
      long newWeight = currentWeight.get();
      Iterator<CacheEntry<T>> iterator = cache.iterator();
      while (iterator.hasNext()) {
        CacheEntry<T> cacheEntry = iterator.next();
        // If removing the next entry is smaller than the min weight, we are done with the reduce logic
        if (newWeight - cacheEntry.getWeight() < limits.get().getMinRetain()) {
          break;
        }
        iterator.remove();
        entriesRemoved++;
        newWeight = currentWeight.addAndGet(-1 * cacheEntry.getWeight());
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
    metricsContext.increment(METRICS_ENTRIES_REMOVED, entriesRemoved);
  }

  /**
   * Carries the limits for the {@link MessageCache}.
   */
  public static final class Limits {
    private final long minRetain;
    private final long reduceTrigger;
    private final long hardLimit;

    public Limits(long minRetain, long reduceTrigger, long hardLimit) {
      Preconditions.checkArgument(reduceTrigger <= hardLimit,
                                  "The reduce trigger weight should not be larger than hard limit");
      Preconditions.checkArgument(minRetain <= reduceTrigger,
                                  "The minimum retain weight should not be larger than the reduce trigger weight");

      this.minRetain = minRetain;
      this.reduceTrigger = reduceTrigger;
      this.hardLimit = hardLimit;
    }

    public long getMinRetain() {
      return minRetain;
    }

    public long getReduceTrigger() {
      return reduceTrigger;
    }

    public long getHardLimit() {
      return hardLimit;
    }
  }

  /**
   * This interface is for calculating the weight of a cache entry.
   *
   * @param <T> type of the entry
   */
  public interface Weigher<T> {
    int weight(T entry);
  }

  /**
   * This interface is for accessing cached entries.
   *
   * @param <T> type of the entry
   */
  public interface Scanner<T> extends CloseableIterator<T> {

    /**
     * Returns the first (smallest) entry in the cache when this scanner was created.
     *
     * @return the first entry in the cache or {@code null} if the cache was empty
     */
    @Nullable
    T getFirstInCache();
  }

  /**
   * A updater for updating an entry.
   *
   * @param <T> type of the entry
   */
  public abstract static class EntryUpdater<T> {

    /**
     * Updates the entry.
     *
     * @param entry the entry to update
     */
    public abstract void updateEntry(T entry);
  }


  /**
   * Abstract implementation of {@link Scanner}.
   *
   * @param <T> type of the entry
   */
  private abstract static class AbstractScanner<T> extends AbstractIterator<T> implements Scanner<T> {

    private final Iterator<T> iterator;
    private final T firstInCache;
    private boolean closed;

    private AbstractScanner(Iterator<T> iterator, @Nullable T firstInCache) {
      this.iterator = iterator;
      this.firstInCache = firstInCache;
    }

    @Override
    protected final T computeNext() {
      if (!closed && iterator.hasNext()) {
        return iterator.next();
      }
      close();
      return endOfData();
    }

    @Nullable
    @Override
    public final T getFirstInCache() {
      return firstInCache;
    }

    @Override
    public final void close() {
      if (!closed) {
        closed = true;
        doClose();
      }
    }

    /**
     * Performs cleanup task.
     */
    abstract void doClose();
  }

  /**
   * A private class that wraps a user provided entry of type {@code T} with an associated weight.
   *
   * @param <T> type of the entry
   */
  private static class CacheEntry<T> {
    private final T entry;
    private final int weight;

    private CacheEntry(T entry, int weight) {
      this.entry = entry;
      this.weight = weight;
    }

    T getEntry() {
      return entry;
    }

    int getWeight() {
      return weight;
    }

    @Override
    public String toString() {
      return "CacheEntry{" +
        "entry=" + entry +
        ", weight=" + weight +
        '}';
    }
  }

  /**
   * A {@link Comparator} for {@link CacheEntry} that only compares with the user entry of type {@code T},
   * using the provided {@link Comparator}.
   *
   * @param <T> type of the user entry
   */
  private static final class CacheEntryComparator<T> implements Comparator<CacheEntry<T>> {

    private final Comparator<T> comparator;

    private CacheEntryComparator(Comparator<T> comparator) {
      this.comparator = comparator;
    }

    @Override
    public int compare(CacheEntry<T> entry1, CacheEntry<T> entry2) {
      return comparator.compare(entry1.getEntry(), entry2.getEntry());
    }
  }
}
