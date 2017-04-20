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

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.messaging.store.MessageFilter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link MessageCache}.
 */
public class MessageCacheTest {

  private static final MetricsContext NOOP_METRICS = new NoopMetricsContext();

  @Test
  public void testNoCache() {
    // Create a cache with zero limits
    MessageCache<String> cache = new MessageCache<>(String.CASE_INSENSITIVE_ORDER, new MessageCache.Weigher<String>() {
      @Override
      public int weight(String entry) {
        return entry.length();
      }
    }, new MessageCache.Limits(0, 0, 0), NOOP_METRICS);
    cache.addAll(Arrays.asList("111", "222", "333").iterator());
    Assert.assertEquals(0L, cache.getCurrentWeight());

    try (MessageCache.Scanner<String> scanner = cache.scan("0", true, 10, MessageFilter.<String>alwaysAccept())) {
      Assert.assertNull(scanner.getFirstInCache());
      Assert.assertFalse(scanner.hasNext());
    }
  }

  @Test
  public void testBasic() {
    // Test basic operations for the cache from single thread
    MessageCache<Integer> cache = new MessageCache<>(new IntComparator(), new UnitWeigher<Integer>(),
                                                     new MessageCache.Limits(10, 14, 20), NOOP_METRICS);

    cache.addAll(Arrays.asList(1, 2, 3, 4, 5, 11, 12, 13, 14, 15).iterator());
    Assert.assertEquals(10, cache.getCurrentWeight());

    MessageFilter<Integer> filter = MessageFilter.alwaysAccept();

    // Scan with a start key that is not in cache
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, filter)) {
      Assert.assertEquals(Integer.valueOf(1), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 11, 12, 13, 14, 15), Lists.newArrayList(scanner));
    }

    // Scan with a limit
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 5, filter)) {
      Assert.assertEquals(Integer.valueOf(1), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), Lists.newArrayList(scanner));
    }

    // Scan with a start key that is in the cache, inclusive
    try (MessageCache.Scanner<Integer> scanner = cache.scan(3, true, 5, filter)) {
      Assert.assertEquals(Integer.valueOf(1), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(3, 4, 5, 11, 12), Lists.newArrayList(scanner));
    }

    // Scan with a start key that is in the cache, exclusive
    try (MessageCache.Scanner<Integer> scanner = cache.scan(3, false, 5, filter)) {
      Assert.assertEquals(Integer.valueOf(1), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(4, 5, 11, 12, 13), Lists.newArrayList(scanner));
    }

    // Scan with a start key that is between keys in the cache
    try (MessageCache.Scanner<Integer> scanner = cache.scan(9, true, 5, filter)) {
      Assert.assertEquals(Integer.valueOf(1), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(11, 12, 13, 14, 15), Lists.newArrayList(scanner));
    }

    // Scan with filter that only accept evens
    MessageFilter<Integer> acceptEvens = new MessageFilter<Integer>() {
      @Override
      public Result apply(Integer input) {
        return input % 2 == 0 ? Result.ACCEPT : Result.SKIP;
      }
    };
    // Scan that takes all evens
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, acceptEvens)) {
      Assert.assertEquals(Integer.valueOf(1), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(2, 4, 12, 14), Lists.newArrayList(scanner));
    }
    // Scan that takes evens with limit
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 3, acceptEvens)) {
      Assert.assertEquals(Integer.valueOf(1), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(2, 4, 12), Lists.newArrayList(scanner));
    }

    // Scan with filter that hold when number 11
    MessageFilter<Integer> holdAtEleven = new MessageFilter<Integer>() {
      @Override
      public Result apply(Integer input) {
        return input == 11 ? Result.HOLD : Result.ACCEPT;
      }
    };
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, holdAtEleven)) {
      Assert.assertEquals(Integer.valueOf(1), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), Lists.newArrayList(scanner));
    }
  }

  @Test
  public void testUpdate() {
    MessageCache<Entry> cache = new MessageCache<>(new EntryComparator(), new UnitWeigher<Entry>(),
                                                   new MessageCache.Limits(10, 14, 20), NOOP_METRICS);

    // Try update that alter order at different element. Exception should be raised in all cases.
    for (int i = 0; i < 3; i++) {
      // Add some entries to the cache
      cache.addAll(Arrays.asList(new Entry(0, "Name"), new Entry(1, "Name"),
                                 new Entry(2, "Name"), new Entry(3, "Name")).iterator());

      // Update entries that alter ordering. Exception should be raised
      try {
        final int idx = i;
        cache.updateEntries(new Entry(0, "Name"), new Entry(2, "Name"), new MessageCache.EntryUpdater<Entry>() {
          @Override
          public void updateEntry(Entry entry) {
            if (entry.getId() == idx) {
              entry.setId(entry.getId() + 1);
            }
          }
        });
        Assert.fail("Expected exception of out of order update in iteration " + i);
      } catch (IllegalStateException e) {
        Assert.assertEquals(0, cache.getCurrentWeight());
      }
    }

    // Repopulate the cache
    cache.addAll(Arrays.asList(new Entry(0, "Name"), new Entry(1, "Name"),
                               new Entry(2, "Name"), new Entry(3, "Name")).iterator());

    // Update entries normally
    cache.updateEntries(new Entry(0, null), new Entry(3, null), new MessageCache.EntryUpdater<Entry>() {
      @Override
      public void updateEntry(Entry entry) {
        entry.setName("Name " + entry.getId());
      }
    });

    try (MessageCache.Scanner<Entry> scanner = cache.scan(new Entry(0, null), true, 10,
                                                          MessageFilter.<Entry>alwaysAccept())) {
      int idx = 0;
      while (scanner.hasNext()) {
        Entry entry = scanner.next();
        Assert.assertEquals(idx, entry.getId());
        Assert.assertEquals("Name " + idx, entry.getName());
        idx++;
      }
    }
  }

  @Test
  public void testCacheReduction() {
    // Test the cache reduction logic in single thread case.
    MessageCache<Integer> cache = new MessageCache<>(new IntComparator(), new UnitWeigher<Integer>(),
                                                     new MessageCache.Limits(5, 7, 10), NOOP_METRICS);

    MessageFilter<Integer> filter = MessageFilter.alwaysAccept();

    // Add some entries to the cache, without going over the reduce trigger
    cache.addAll(Arrays.asList(1, 2, 3, 4, 5).iterator());

    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, filter)) {
      Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), Lists.newArrayList(scanner));
    }

    // Add some more entries so that it goes over the reduce trigger limit, but not hitting hard limit
    cache.addAll(Arrays.asList(6, 7, 8, 9).iterator());

    // First scan of the cache should gives all cached entries, since reduction only performed at scanner close
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, filter)) {
      Assert.assertEquals(Integer.valueOf(1), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), Lists.newArrayList(scanner));
    }

    // The second scan should only give the last 5 entries (min retain)
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, filter)) {
      Assert.assertEquals(Integer.valueOf(5), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(5, 6, 7, 8, 9), Lists.newArrayList(scanner));
    }

    // Add more entries so that it will hit the hard limit while adding
    cache.addAll(Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18).iterator());

    // Since weight reduction happened during adding of element 15 (5 - 15 has weight 11),
    // it retains the latest 5 (11 to 15). Then adding 16 to 18 doesn't hit the hard limit, hence the cache
    // will be holding 11 to 18.
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, filter)) {
      Assert.assertEquals(Integer.valueOf(11), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(11, 12, 13, 14, 15, 16, 17, 18), Lists.newArrayList(scanner));
    }

    // Since on previous scan closure will reduce the cache back to the min retain, scanning again would
    // only give the last 5 elements
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, filter)) {
      Assert.assertEquals(Integer.valueOf(14), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(14, 15, 16, 17, 18), Lists.newArrayList(scanner));
    }
  }

  @Test
  public void testCacheResize() {
    // Test resize the cache
    MessageCache<Integer> cache = new MessageCache<>(new IntComparator(), new UnitWeigher<Integer>(),
                                                     new MessageCache.Limits(2, 3, 4), NOOP_METRICS);

    MessageFilter<Integer> filter = MessageFilter.alwaysAccept();

    // Add entries to the cache up to the hard limit
    cache.addAll(Arrays.asList(1, 2, 3, 4).iterator());

    // Resize the the cache to double the size
    cache.resize(new MessageCache.Limits(4, 6, 8));

    // Scanning should get all entries
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, filter)) {
      Assert.assertEquals(Integer.valueOf(1), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(1, 2, 3, 4), Lists.newArrayList(scanner));
    }

    // Add more elements that passes the new hard limit
    cache.addAll(Arrays.asList(5, 6, 7, 8, 9).iterator());

    // Scanning should get the last four elements (new min retain)
    try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, filter)) {
      Assert.assertEquals(Integer.valueOf(6), scanner.getFirstInCache());
      Assert.assertEquals(Arrays.asList(6, 7, 8, 9), Lists.newArrayList(scanner));
    }
  }

  @Test
  public void testAddError() throws Exception {
    // Test to verify various error situations are being safeguarded
    final MessageCache<Integer> cache = new MessageCache<>(new IntComparator(), new UnitWeigher<Integer>(),
                                                           new MessageCache.Limits(5, 7, 10), NOOP_METRICS);

    // 1. Adding out of order should result in error
    try {
      cache.addAll(Arrays.asList(5, 2, 3, 4).iterator());
      Assert.fail("Expected failure for adding out of order");
    } catch (IllegalArgumentException e) {
      // Expected. The cache should be cleared
      Assert.assertEquals(0, cache.getCurrentWeight());
    }

    // 2. Adding entries that are smaller than the largest one in the cache
    cache.addAll(Arrays.asList(5, 6, 7, 8).iterator());
    try {
      cache.addAll(Arrays.asList(1, 2, 3, 4).iterator());
      Assert.fail("Expected failure for adding out of order");
    } catch (IllegalArgumentException e) {
      // Expected. The cache should be cleared
      Assert.assertEquals(0, cache.getCurrentWeight());
    }

    // 3. Adding duplicate entry
    try {
      cache.addAll(Arrays.asList(1, 1).iterator());
      Assert.fail("Expected failure for adding out of order");
    } catch (IllegalArgumentException e) {
      // Expected. The cache should be cleared
      Assert.assertEquals(0, cache.getCurrentWeight());
    }

    // 4. Adding entry that is already exist in the cache
    cache.addAll(Arrays.asList(1, 2).iterator());
    try {
      cache.addAll(Arrays.asList(2, 3).iterator());
    } catch (IllegalArgumentException e) {
      // Expected. The cache should be cleared
      Assert.assertEquals(0, cache.getCurrentWeight());
    }

    // 5. Multiple threads calling addAll at the same time
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final CountDownLatch produceLatch = new CountDownLatch(1);

    // Starts the the first thread and block inside the addAll call
    new Thread() {
      @Override
      public void run() {
        cache.addAll(new AbstractIterator<Integer>() {
          private boolean produced;

          @Override
          protected Integer computeNext() {
            try {
              barrier.await();
            } catch (Exception e) {
              // This shouldn't happen
            }
            Uninterruptibles.awaitUninterruptibly(produceLatch);
            if (!produced) {
              produced = true;
              return 1;
            }
            return endOfData();
          }
        });
      }
    }.start();

    // Starts the second thread that tries to call addAll after the first thread is blocked inside the addAll call
    final BlockingQueue<Exception> exception = new ArrayBlockingQueue<>(1);
    new Thread() {
      @Override
      public void run() {
        try {
          barrier.await();
        } catch (Exception e) {
          // This shouldn't happen
        }
        try {
          cache.addAll(Arrays.asList(1, 2, 3).iterator());
        } catch (Exception e) {
          exception.add(e);
        }
      }
    }.start();

    Exception e = exception.poll(10, TimeUnit.SECONDS);
    Assert.assertNotNull(e);
    Assert.assertTrue(e instanceof ConcurrentModificationException);

    // Unblock the first thread
    produceLatch.countDown();

    final MessageFilter<Integer> filter = MessageFilter.alwaysAccept();
    Tasks.waitFor(Collections.singletonList(1), new Callable<List<Integer>>() {
      @Override
      public List<Integer> call() throws Exception {
        try (MessageCache.Scanner<Integer> scanner = cache.scan(0, true, 10, filter)) {
          return Lists.newArrayList(scanner);
        }
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }


  /**
   * A {@link Comparator} for {@link Integer}.
   */
  private static final class IntComparator implements Comparator<Integer> {

    @Override
    public int compare(Integer o1, Integer o2) {
      return o1.compareTo(o2);
    }
  }

  /**
   * A {@link MessageCache.Weigher} that also return 1 for the weight
   *
   * @param <T> type of entry
   */
  private static final class UnitWeigher<T> implements MessageCache.Weigher<T> {

    @Override
    public int weight(T entry) {
      return 1;
    }
  }

  /**
   * A cache entry for testing.
   */
  private static final class Entry {
    private int id;
    private String name;

    public Entry(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public void setId(int id) {
      this.id = id;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  /**
   * A {@link Comparator} for {@link Entry} that only compare with the id.
   */
  private static final class EntryComparator implements Comparator<Entry> {

    @Override
    public int compare(Entry entry1, Entry entry2) {
      return Integer.compare(entry1.getId(), entry2.getId());
    }
  }
}
