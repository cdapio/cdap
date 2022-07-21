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

package io.cdap.cdap.api.dataset.lib;


import io.cdap.cdap.api.annotation.ReadOnly;
import io.cdap.cdap.api.annotation.WriteOnly;
import io.cdap.cdap.api.data.batch.BatchReadable;
import io.cdap.cdap.api.data.batch.BatchWritable;
import io.cdap.cdap.api.data.batch.IteratorBasedSplitReader;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.data.batch.SplitReader;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.table.Table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

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
 * <code>timeIntervalToStorePerRow</code> in the {@link io.cdap.cdap.api.dataset.DatasetSpecification} properties).
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
 *       {@link io.cdap.cdap.api.dataset.table.Table} API which is used "under-the-hood". In particular the
 *       implementation is constrained by the absence of a <code>readHigherOrEq()</code> method in the
 *       {@link io.cdap.cdap.api.dataset.table.Table} API, which would return the next row with key greater
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
  @WriteOnly
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
  @ReadOnly
  public final Iterator<Entry> read(byte[] key, long startTime, long endTime,
                                    int offset, final int limit, byte[]... tags) {
    final Iterator<Entry> iterator = read(key, startTime, endTime, tags);
    int advance = offset;
    while (advance > 0 && iterator.hasNext()) {
      iterator.next();
      advance--;
    }

    return new Iterator<Entry>() {
      int count;

      @Override
      public boolean hasNext() {
        return count < limit && iterator.hasNext();
      }

      @Override
      public Entry next() {
        if (hasNext()) {
          count++;
          return iterator.next();
        }
        throw new NoSuchElementException();
      }

      @Override
      public void remove() {
        iterator.remove();
      }
    };
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
  @ReadOnly
  public Iterator<Entry> read(byte[] key, long startTime, long endTime, byte[]... tags) {
    final Iterator<TimeseriesDataset.Entry> internalIterator = readInternal(key, startTime, endTime, tags);
    return new Iterator<Entry>() {
      @Override
      public boolean hasNext() {
        return internalIterator.hasNext();
      }

      @Override
      public Entry next() {
        TimeseriesDataset.Entry entry = internalIterator.next();
        return new Entry(entry.getKey(), entry.getValue(), entry.getTimestamp(), entry.getTags());
      }

      @Override
      public void remove() {
        internalIterator.remove();
      }
    };
  }



  /**
   * A method for using a Dataset as input for a MapReduce job.
   */
  public static final class InputSplit extends Split {
    private byte[] key;
    private long startTime;
    private long endTime;
    private byte[][] tags;

    /**
     * Constructor for serialization only. Don't call directly.
     */
    public InputSplit() {
      // no-op
    }

    private InputSplit(byte[] key, long startTime, long endTime, byte[][] tags) {
      this.key = key;
      this.startTime = startTime;
      this.endTime = endTime;
      this.tags = tags;
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {
      if (key == null) {
        out.writeInt(-1);
      } else {
        out.writeInt(key.length);
        out.write(key);
      }

      out.writeLong(startTime);
      out.writeLong(endTime);

      out.writeInt(tags.length);
      for (byte[] tag : tags) {
        if (tag == null) {
          out.writeInt(-1);
        } else {
          out.writeInt(tag.length);
          out.write(tag);
        }
      }
    }

    @Override
    public void readExternal(DataInput in) throws IOException {
      int len = in.readInt();
      if (len < 0) {
        key = null;
      } else {
        key = new byte[len];
        in.readFully(key);
      }

      startTime = in.readLong();
      endTime = in.readLong();

      tags = new byte[in.readInt()][];
      for (int i = 0; i < tags.length; i++) {
        len = in.readInt();
        if (len < 0) {
          tags[i] = null;
        } else {
          tags[i] = new byte[len];
          in.readFully(tags[i]);
        }
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      InputSplit that = (InputSplit) o;
      return startTime == that.startTime &&
        endTime == that.endTime &&
        Arrays.equals(key, that.key) &&
        Arrays.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, startTime, endTime, tags);
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

    List<Split> splits = new ArrayList<>();
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
                                              "initialize(MapReduceContext context) method of the MapReduce app.");
  }

  @ReadOnly
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
  @WriteOnly
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
