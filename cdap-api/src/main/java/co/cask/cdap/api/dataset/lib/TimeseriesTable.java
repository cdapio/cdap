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


import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.IteratorBasedSplitReader;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Defines a Dataset implementation for managing time series data. This class offers simple ways to process read
 * operations for time ranges.
 *
 * <p>
 * This Dataset works by partitioning time into bins representing time intervals. Entries added to the Dataset
 * are added to a bin based on their timestamp and row key. Hence, every row in the underlying table contains entries
 * that share the same time interval and row key. Data for each entry is stored in separate columns.
 * </p>
 *
 * <p>
 * A user can set the time interval length for partitioning data into rows (as defined by 
 * <code>timeIntervalToStorePerRow</code> in the {@link co.cask.cdap.api.dataset.DatasetSpecification} properties).
 * This interval should be chosen according to the use case at hand. In general, larger time interval sizes mean
 * faster reading of small-to-medium time ranges (range size up to several time intervals) of entries data,
 * while having slower reading of very small time ranges of entries data (range size a small portion of the time
 * interval). Using a larger time interval also helps with faster batched writing of entries.
 * </p>
 *
 * <p>Vice versa, setting smaller time intervals provides faster reading of very small time ranges of entries data,
 * but has slower batched writing of entries.
 * </p>
 *
 * <p>
 * As expected, a larger time interval means that more data will be stored per row. A user should
 * generally avoid storing more than 50 megabytes of data per row, since it affects performance.
 * </p>
 * <p>
 * The default value for time interval length is one hour and is generally suggested for users to use a value of
 * between one minute and several hours. In cases where the amount of written entries is small, the rule of thumb is:
 * <br/><br/>
 * <code>row partition interval size = 5 * (average size of the time range to be read)</code>
 * </p>
 *
 * <p>
 * TimeseriesTable supports tagging, where each entry is (optionally) labeled with a set of tags used for filtering of
 * items during data retrievals. For an entry to be retrievable using a given tag, the tag must be provided when
 * the entry was written. If multiple tags are provided during reading, an entry must contain every one of these tags
 * in order to qualify for return.
 * </p>
 *
 * <p>
 * Due to the data format used for storing, filtering by tags during reading is done on client-side (not on a cluster).
 * At the same time, filtering by entry keys happens on the server side, which is much more efficient performance-wise.
 * Depending on the use-case you may want to push some of the tags you would use into the entry key for faster reading.
 * </p>
 *
 * <p>
 * Notes on implementation:
 * <ol>
 *   <li>
 *       This implementation does NOT address the RegionServer hot-spotting issue that appears when writing rows with
 *       monotonically increasing/decreasing keys into HBase. This point is relevant for HBase-backed data stores.
 *       To avoid this problem, a user should not write all data under the same metric key. In general, writes will be
 *       as distributed as the number of different metric keys the data is written for. Having a single metric key would
 *       mean hitting a single RegionServer at any given point of time with all writes; this is generally not desirable.
 *   </li>
 *   <li>
 *       The current implementation (including the format of the stored data) is heavily affected by the
 *       {@link co.cask.cdap.api.dataset.table.Table} API which is used "under-the-hood". In particular the
 *       implementation is constrained by the absence of a <code>readHigherOrEq()</code> method in the
 *       {@link co.cask.cdap.api.dataset.table.Table} API, which would return the next row with key greater
 *       or equals to the given.
 *   </li>
 *   <li>
 *       The client code should not rely on the implementation details as they may be changed without notice.
 *   </li>
 * </ol>
 * </p>
 *
 * @see CounterTimeseriesTable
 */
public class TimeseriesTable extends TimeseriesDataset
  implements BatchReadable<byte[], TimeseriesTable.Entry>, BatchWritable<byte[], TimeseriesTable.Entry> {

  /**
   * Creates an instance of the table.
   */
  public TimeseriesTable(DatasetSpecification spec, Table table) {
    super(spec, table);
  }

  /**
   * Writes an entry to the Dataset.
   *
   * @param entry entry to write
   */
  public final void write(Entry entry) {
    write(entry.getKey(), entry.getValue(), entry.getTimestamp(), entry.getTags());
  }

  /**
   * Reads entries for a given time range and returns an <code>Iterator<Entry></code>.
   * Provides the same functionality as {@link #read(byte[], long, long, byte[][]) read(byte[], long, long, byte[]...)} 
   * but accepts additional parameters for pagination purposes.
   * NOTE: A limit is placed on the max number of time intervals to be scanned during a read, as defined by
   * {@link #MAX_ROWS_TO_SCAN_PER_READ}.
   *
   * @param key key of the entries to read
   * @param startTime defines start of the time range to read, inclusive
   * @param endTime defines end of the time range to read, inclusive
   * @param offset the number of initial entries to ignore and not add to the results
   * @param limit upper limit on number of results returned. If limit is exceeded, the first <code>limit</code> results
   *              are returned
   * @param tags a set of tags which entries returned must contain. Tags for entries are defined at write-time and an
   *             entry is only returned if it contains all of these tags.
   *
   * @return an iterator over entries that satisfy provided conditions
   * @throws IllegalArgumentException when provided condition is incorrect
   */
  public final Iterator<Entry> read(byte[] key, long startTime, long endTime, int offset, int limit, byte[]... tags) {
    Iterator<Entry> iterator = read(key, startTime, endTime, tags);
    iterator = Iterators.limit(iterator, limit + offset);
    Iterators.advance(iterator, offset);
    return iterator;
  }

  /**
   * Reads entries for a given time range and returns an <code>Iterator<Entry></code>.
   * NOTE: A limit is placed on the max number of time intervals to be scanned during a read, as defined by
   * {@link #MAX_ROWS_TO_SCAN_PER_READ}.
   *
   * @param key key of the entries to read
   * @param startTime defines start of the time range to read, inclusive
   * @param endTime defines end of the time range to read, inclusive
   * @param tags a set of tags which entries returned must contain. Tags for entries are defined at write-time and an
   *             entry is only returned if it contains all of these tags.
   *
   * @return an iterator over entries that satisfy provided conditions
   */
  public Iterator<Entry> read(byte[] key, long startTime, long endTime, byte[]... tags) {
    return Iterators.transform(readInternal(key, startTime, endTime, tags),
                               new Function<TimeseriesDataset.Entry, Entry>() {
                                 @Nullable
                                 @Override
                                 public TimeseriesTable.Entry apply(@Nullable TimeseriesDataset.Entry input) {
                                   return new Entry(input.getKey(), input.getValue(),
                                                    input.getTimestamp(), input.getTags());
                                 }
                               });
  }



  /**
   * A method for using a Dataset as input for a MapReduce job.
   */
  private static final class InputSplit extends Split {
    private byte[] key;
    private long startTime;
    private long endTime;
    private byte[][] tags;

    private InputSplit(byte[] key, long startTime, long endTime, byte[][] tags) {
      this.key = key;
      this.startTime = startTime;
      this.endTime = endTime;
      this.tags = tags;
    }
  }

  /**
   * Defines input selection for batch jobs.
   *
   * @param splitsCount number of parts to split the data selection into
   * @param key key of the entries to read
   * @param startTime defines start of the time range to read, inclusive
   * @param endTime defines end of the time range to read, inclusive
   * @param tags a set of tags which entries returned must contain. Tags for entries are defined at write-time and an
   *             entry is only returned if it contains all of these tags.
   * @return the list of splits
   */
  public List<Split> getInputSplits(int splitsCount, byte[] key, long startTime, long endTime, byte[]... tags) {
    long timeIntervalPerSplit = (endTime - startTime) / splitsCount;
    // we don't want splits to be empty
    timeIntervalPerSplit = timeIntervalPerSplit > 0 ? timeIntervalPerSplit : 1;

    List<Split> splits = new ArrayList<Split>();
    long start;
    for (start = startTime; start + timeIntervalPerSplit <= endTime; start += timeIntervalPerSplit) {
      splits.add(new InputSplit(key, start, start + timeIntervalPerSplit, tags));
    }

    // last interval should cover all up to the endTime
    if (start + timeIntervalPerSplit < endTime) {
      splits.add(new InputSplit(key, start + timeIntervalPerSplit, endTime, tags));
    }

    return splits;
  }

  @Override
  public List<Split> getSplits() {
    throw new UnsupportedOperationException("Cannot use TimeSeriesTable as input for Batch directly. " +
                                              "Use getInput(...) and call " +
                                              "MapReduceContext.setInput(tsTable, splits) in the " +
                                              "beforeSubmit(MapReduceContext context) method of the MapReduce app.");
  }

  @Override
  public SplitReader<byte[], Entry> createSplitReader(final Split split) {
    return new TimeseriesTableRecordsReader();
  }

  /**
   * Writes an entry to the Dataset. This method overrides {@code write(key, value)} in {@link BatchWritable}.
   * The key is ignored in this method and instead it uses the key provided in the <code>Entry</code> object.
   *
   * @param key row key to write to. Value is ignored
   * @param value entry to write. The key used to write to the table is extracted from this object
   */
  @Override
  public void write(final byte[] key, final Entry value) {
    write(value);
  }

  /**
   * A record reader for time series.
   */
  public final class TimeseriesTableRecordsReader extends IteratorBasedSplitReader<byte[], Entry> {
    @Override
    public Iterator<Entry> createIterator(final Split split) {
      InputSplit s = (InputSplit) split;
      return read(s.key, s.startTime, s.endTime, s.tags);
    }

    @Override
    protected byte[] getKey(Entry entry) {
      return entry.getKey();
    }
  }

  /**
   * Time series table entry.
   */
  public static final class Entry extends TimeseriesDataset.Entry {

    /**
     * Creates instance of the time series entry.
     *
     * @param key key of the entry
     * @param value value to store
     * @param timestamp timestamp of the entry
     * @param tags optional list of tags associated with the entry. See class description for more details.
     */
    public Entry(byte[] key, byte[] value, long timestamp, byte[]... tags) {
      super(key, value, timestamp, tags);
    }
  }
}
