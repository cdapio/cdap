package com.continuuity.api.data.lib;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.Increment;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.ReadColumnRange;
import com.continuuity.api.data.util.Bytes;

/**
 * Table of sets of counters that support reading in descending value order.
 * <p>
 * That is, counters in a counter set can be sorted and queried in descending
 * order according to their counter values.
 * <p>
 * Configuration of various parameters of this sorted counter algorithm is done
 * through {@link SortedCounterConfig}.
 * <p>
 * In order to efficiently perform this top-n style query, increment value
 * pass-thrus are used as follows:
 * <p>
 * <ul>
 *  <li>
 *   Generate initial increment using
 *   {@link #generatePrimaryCounterIncrement(byte[], byte[], long)}
 *  </li>
 *  <li>
 *   Output Increment within a field of a tuple going to the next flowlet:
 *   <p>
 *   <pre>TupleBuilder.set("top-counter", generatedPrimaryIncrement)</pre>
 *  </li>
 *  <li>
 *   In the next flowlet, perform any necessary secondary counter increments
 *   using {@link #performSecondaryCounterIncrements(byte[], byte[], long, long)}.
 *  </li>
 * </ul>
 */
public class SortedCounterTable extends DataLib {

  private final SortedCounterConfig config;

  public SortedCounterTable(String tableName, DataFabric fabric,
      BatchCollectionRegistry registry, SortedCounterConfig config) {
    super(tableName, fabric, registry);
    this.config = config;
  }
  
  /**
   * Generates the primary increment of the specified amount for the specified
   * counter in the specified counter set.  This row will contain the raw counts
   * for all counters in this counter set.
   * <p>
   * This increment operation should be output as a field in a tuple.  The
   * receiving flowlet should then perform
   * {@link #performSecondaryCounterIncrements(byte[], byte[], long, long)}
   * with the incremented value.
   * @param counterSet counter set name
   * @param counter counter name
   * @param amount counter increment amount
   * @return generated increment operation
   */
  public Increment generatePrimaryCounterIncrement(byte [] counterSet,
      byte [] counter, long amount) {
    byte [] row = makeRow(counterSet, 0);
    return new Increment(this.tableName, row, counter, amount);
  }

  /**
   * Performs an asynchronous update of secondary counter increments for the
   * specified counter in the specified counter set, which was incremented
   * by the specified amount and is now equal to the specified value. 
   * @param counterSet counter set name
   * @param counter counter name
   * @param amount original increment amount
   * @param value value of counter after increment
   */
  public void performSecondaryCounterIncrements(
      byte [] counterSet, byte [] counter, long amount, long value) {
    for (long bucket : this.config.buckets) {
      if (value >= bucket) {
        long localAmount = amount;
        if (value - bucket < amount) localAmount = value - bucket;
        byte [] row = makeRow(counterSet, bucket);
        this.collector.add(new Increment(this.tableName, row, counter, localAmount));
      } else break;
    }
  }

  /**
   * Performs a synchronous read operation for the top counters in the specified
   * counter set, up to the limit.
   * <p>
   * Top counter list returned is in descending count order.
   * @param counterSet counter set name
   * @param limit maximum number of counters to return
   * @return list of top counters, in descending count order
   * @throws OperationException
   */
  public List<Counter> readTopCounters(byte [] counterSet, int limit)
      throws OperationException {
    CounterBucket counterBucket = new CounterBucket();
    // Build a list of buckets in descending order to be processed
    List<Long> bucketList = new ArrayList<Long>(this.config.buckets.length);
    for (int i = this.config.buckets.length - 1 ; i >= 0 ; i--) {
      bucketList.add(this.config.buckets[i]);
    }
    bucketList.add(0L);
    // Iterate through buckets (descending / reverse order)
    for (Long bucket : bucketList) {
      byte [] row = makeRow(counterSet, bucket);
      // Read all counters in bucket
      // TODO: This uses Read of all columns not key-value!
      ReadColumnRange bucketRead =
          new ReadColumnRange(this.tableName, row, null, null);
      OperationResult<Map<byte[],byte[]>> result = this.fabric.read(bucketRead);
      // If nothing in this bucket, go to next bucket
      if (result.isEmpty()) continue;
      // Iterate through all entries in this bucket (these are ordered by name
      // not by count, so need to look at all of them, cannot early-out)
      for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
        byte [] counterName = entry.getKey();
        long counterValue = Bytes.toLong(entry.getValue()) + bucket;
        Counter counter = new Counter(counterName, counterValue);
        counterBucket.add(counter);
      }
      // If we have enough counters, we are done
      if (counterBucket.size() >= limit) break;
      // Otherwise, keep going to next bucket
    }
    // Found enough counters or ran out, get the top results, up to limit
    return counterBucket.getTop(limit);
  }

  private class CounterBucket {

    /**
     * Set of counters, unique on counter name.
     */
    private final Set<Counter> counterSet = new TreeSet<Counter>(
        new Comparator<Counter>() {
          @Override
          public int compare(Counter left, Counter right) {
            return Bytes.compareTo(left.name, right.name);
          }
        });
    
    int size() {
      return this.counterSet.size();
    }

    void add(Counter counter) {
      counterSet.add(counter);
    }

    /**
     * Returns the counters with the highest counts, up to the specified limit.
     * <p>
     * This is an expensive operation and does not cache the result!
     * @param limit maximum counters to return
     * @return list of counters with highest counts, in descending count order
     */
    List<Counter> getTop(int limit) {
      /*
       * Ordered map of top counters.  Comparator will sort map in descending
       * count order, and then ascending counter order for tied count.
       */
      NavigableMap<Counter,Counter> topMap = new TreeMap<Counter,Counter>(
          new Comparator<Counter>() {
            @Override
            public int compare(Counter left, Counter right) {
              if (left.count > right.count) return -1;
              if (left.count < right.count) return 1;
              return Bytes.compareTo(left.name, right.name);
            }
          });
      // Add everything to the map
      for (Counter counter : counterSet) topMap.put(counter, counter);
      // Grab up to n (min of limit/total found)
      int n = Math.min(limit, topMap.size());
      List<Counter> top = new ArrayList<Counter>(n);
      Iterator<Counter> topIterator = topMap.keySet().iterator();
      for (int i=0; i<n; i++) {
        top.add(topIterator.next());
      }
      return top;
    }

  }

  private byte[] makeRow(byte[] counterSet, long bucketMin) {
    return Bytes.add(counterSet, Bytes.toBytes(bucketMin));
  }

  /**
   * A named counter (a binary name and a long count).
   */
  public static class Counter {
    private final byte [] name;
    private final Long count;

    public Counter(byte [] name, Long count) {
      this.name = name;
      this.count = count;
    }

    public byte [] getName() {
      return this.name;
    }

    public Long getCount() {
      return this.count;
    }
  }

  public static class SortedCounterConfig {

    public long [] buckets = new long [] { 10, 100, 1000 };

  }
}
