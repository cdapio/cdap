/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.util.Iterator;

/**
 * A Dataset for incrementing counts over time periods. This Dataset provides an extension to {@link TimeseriesTable}
 * for long values and provides increment methods for counting.
 *
 * <p>For more information on choosing values for <code>rowPartitionIntervalSize</code> and tag usage, please see the
 * {@link TimeseriesTable} class description.</p>
 *
 * @see TimeseriesTable
 */
public class CounterTimeseriesTable extends TimeseriesDataset {

  /**
   * Creates an instance of the DataSet.
   */
  public CounterTimeseriesTable(DatasetSpecification spec, Table table) {
    super(spec, table);
  }

  /**
   * Increments the value for a counter for a row and timestamp.
   *
   * @param counter the name of the counter to increment
   * @param amount the amount to increment by
   * @param timestamp timestamp of the entry
   * @param tags optional list of tags associated with the counter. See {@link TimeseriesTable} class description
   *             for more details.
   * @return value of the entry after increment
   */
  public long increment(byte[] counter, long amount, long timestamp, byte[]... tags) {
    return internalIncrement(counter, amount, timestamp, tags);
  }

  /**
   * Set the value for a counter.
   *
   * @param counter the name of the counter to set
   * @param value the value to set
   * @param timestamp timestamp of the entry
   * @param tags optional list of tags associated with the counter. See {@link TimeseriesTable} class description
   *             for more details.
   */
  public void set(byte[] counter, long value, long timestamp, byte[]... tags) {
    write(counter, Bytes.toBytes(value), timestamp, tags);
  }

  /**
   * Reads entries for a given time range and returns an <code>Iterator<Counter></code>.
   * NOTE: A limit is placed on the max number of time intervals to be scanned during a read, as defined by
   * {@link #MAX_ROWS_TO_SCAN_PER_READ}.
   *
   * @param counter name of the counter to read
   * @param startTime defines start of the time range to read, inclusive
   * @param endTime defines end of the time range to read, inclusive
   * @param tags a set of tags which entries returned must contain. Tags for entries are defined at write-time and an
   *             entry is only returned if it contains all of these tags.
   * @return an iterator over entries that satisfy provided conditions
   */
  public Iterator<Counter> read(byte[] counter, long startTime, long endTime, byte[]... tags) {
    return Iterators.transform(readInternal(counter, startTime, endTime, tags),
                               new Function<Entry, Counter>() {
                                 @Override
                                 public Counter apply(Entry input) {
                                   return new Counter(input.getKey(), Bytes.toLong(input.getValue()),
                                                      input.getTimestamp(), input.getTags());
                                 }
                               });
  }

  /**
   * Reads entries for a given time range and returns an <code>Iterator<Counter></code>.
   * Provides the same functionality as {@link #read(byte[], long, long, byte[][]) read(byte[], long, long, byte[]...)}
   * but accepts additional parameters for pagination purposes.
   *
   * @param counter name of the counter to read
   * @param startTime defines start of the time range to read, inclusive
   * @param endTime defines end of the time range to read, inclusive
   * @param offset the number of initial entries to ignore and not add to the results
   * @param limit upper limit on number of results returned. If limit is exceeded, the first <code>limit</code> results
   *              are returned.
   * @param tags a set of tags which entries returned must contain. Tags for entries are defined at write-time and an
   *             entry is only returned if it contains all of these tags.
   * @return an iterator over entries that satisfy provided conditions
   */
  public Iterator<Counter> read(byte[] counter, long startTime, long endTime, int offset, int limit, byte[]... tags) {
    Iterator<Counter> iterator = read(counter, startTime, endTime, tags);
    iterator = Iterators.limit(iterator, limit + offset);
    Iterators.advance(iterator, offset);
    return iterator;
  }

  /**
   * Defines an object for counters in {@link CounterTimeseriesTable}.
   */
  public static final class Counter {
    private byte[] counter;
    private long value;
    private long timestamp;
    private byte[][] tags;

    /**
     * Creates an instance of a time series counter.
     * @param counter name of the counter
     * @param value value of the counter
     * @param timestamp timestamp of the counter
     * @param tags optional list of tags associated with the counter. See {@link TimeseriesTable} class description
     *             for more details.
     */
    private Counter(byte[] counter, long value, long timestamp, byte[]... tags) {
      this.counter = counter;
      this.value = value;
      this.timestamp = timestamp;
      this.tags = tags;
    }

    /**
     * Returns the name of the counter.
     * @return the name of the counter
     */
    public byte[] getCounter() {
      return counter;
    }

    /**
     * Returns the count value of the counter.
     * @return the count value of the counter
     */
    public long getValue() {
      return value;
    }

    /**
     * Returns the timestamp of the counter.
     * @return the timestamp of the counter
     */
    public long getTimestamp() {
      return timestamp;
    }

    /**
     * Returns the tags associated with the counter.
     * @return the tags associated with the counter
     */
    public byte[][] getTags() {
      return tags;
    }
  }
}
