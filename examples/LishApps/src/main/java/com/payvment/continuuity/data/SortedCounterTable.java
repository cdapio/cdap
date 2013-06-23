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
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Table of sets of counters that support reading in descending value order.
 * <p/>
 * That is, counters in a counter set can be sorted and queried in descending
 * order according to their counter values.
 * <p/>
 * Configuration of various parameters of this sorted counter algorithm is done
 * through {@link SortedCounterConfig}.
 */
public class SortedCounterTable extends DataSet {

  private final SortedCounterConfig config;

  private final Table counters;

  public SortedCounterTable(String name) {
    super(name);
    this.counters = new Table("sct_" + name);
    this.config = new SortedCounterConfig();
  }

  public SortedCounterTable(DataSetSpecification spec) {
    super(spec);
    this.counters = new Table("sct_" + getName());
    this.config = new SortedCounterConfig(spec.getProperty("config"));
  }


  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .property("config", this.config.toString())
      .dataset(this.counters.configure())
      .create();
  }

  /**
   * Performs the necessary increment operations to enable top-n queries on the
   * specified counter set.
   * <p/>
   *
   * @param counterSet counter set name
   * @param counter    counter name
   * @param amount     counter increment amount
   */
  public void increment(byte[] counterSet, byte[] counter, long amount)
    throws OperationException {
    // Determine total count
    byte[] row = makeRow(counterSet, 0);
    Map<byte[], Long> result = this.counters.incrementAndGet(
      new Increment(row, counter, amount));
    long value = result.get(counter);

    // Perform any necessary bucket updates
    for (long bucket : this.config.buckets) {

      if (value >= bucket) {
        long localAmount = amount;

        if (value - bucket < amount) {
          localAmount = value - bucket;
        }

        byte[] bucketRow = makeRow(counterSet, bucket);
        this.counters.write(new Increment(bucketRow, counter, localAmount));
      } else {
        break;
      }
    }
  }

  /**
   * Performs a synchronous read operation for the top counters in the specified
   * counter set, up to the limit.
   * <p/>
   * Top counter list returned is in descending count order.
   *
   * @param counterSet counter set name
   * @param limit      maximum number of counters to return
   * @return list of top counters, in descending count order
   * @throws com.continuuity.api.data.OperationException
   *
   */
  public List<Counter> readTopCounters(byte[] counterSet, int limit)
    throws OperationException {
    CounterBucket counterBucket = new CounterBucket();
    // Build a list of buckets in descending order to be processed
    List<Long> bucketList = new ArrayList<Long>(this.config.buckets.length);
    for (int i = this.config.buckets.length - 1; i >= 0; i--) {
      bucketList.add(this.config.buckets[i]);
    }
    bucketList.add(0L);
    // Iterate through buckets (descending / reverse order)
    for (Long bucket : bucketList) {
      byte[] row = makeRow(counterSet, bucket);

      // Read all counters in bucket
      Read bucketRead = new Read(row);
      OperationResult<Map<byte[], byte[]>> result = this.counters.read(bucketRead);

      // If nothing in this bucket, go to next bucket
      if (result.isEmpty()) {
        continue;
      }

      // Iterate through all entries in this bucket (these are ordered by name
      // not by count, so need to look at all of them, cannot early-out)
      for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
        byte[] counterName = entry.getKey();
        long counterValue = Bytes.toLong(entry.getValue()) + bucket;
        Counter counter = new Counter(counterName, counterValue);
        counterBucket.add(counter);
      }

      // If we have enough counters, we are done
      if (counterBucket.size() >= limit) {
        break;
      }

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
      this.counterSet.add(counter);
    }

    /**
     * Returns the counters with the highest counts, up to the specified limit.
     * <p/>
     * This is an expensive operation and does not cache the result!
     *
     * @param limit maximum counters to return
     * @return list of counters with highest counts, in descending count order
     */
    List<Counter> getTop(int limit) {
      /*
       * Ordered map of top counters.  Comparator will sort map in descending
       * count order, and then ascending counter order for tied count.
       */
      NavigableMap<Counter, Counter> topMap = new TreeMap<Counter, Counter>(new Comparator<Counter>() {
          @Override
          public int compare(Counter left, Counter right) {
            if (left.count > right.count) {
              return -1;
            }

            if (left.count < right.count) {
              return 1;
            }

            return Bytes.compareTo(left.name, right.name);
          }
        });

      // Add everything to the map
      for (Counter counter : this.counterSet) {
        topMap.put(counter, counter);
      }

      // Grab up to n (min of limit/total found)
      int n = Math.min(limit, topMap.size());
      List<Counter> top = new ArrayList<Counter>(n);
      Iterator<Counter> topIterator = topMap.keySet().iterator();
      for (int i = 0; i < n; i++) {
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
    private final byte[] name;
    private final Long count;

    public Counter(byte[] name, Long count) {
      this.name = name;
      this.count = count;
    }

    public byte[] getName() {
      return this.name;
    }

    public Long getCount() {
      return this.count;
    }
  }

  /**
   *
   */
  public static class SortedCounterConfig {

    private final long[] buckets;

    public SortedCounterConfig() {
      this.buckets = new long[]{10, 100, 1000};
    }

    public SortedCounterConfig(String serialized) {
      String[] strings = serialized.split(" ");
      this.buckets = new long[strings.length];
      for (int i = 0; i < strings.length; ++i) {
        this.buckets[i] = Long.parseLong(strings[i]);
      }
    }

    public long[] getBuckets() {
      return this.buckets;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      for (long i : this.buckets) {
        builder.append(i);
        builder.append(" ");
      }
      return builder.toString().trim();
    }
  }
}
