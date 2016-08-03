/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An abstract class for time series Datasets.
 */
abstract class TimeseriesDataset extends AbstractDataset {

  public static final String ATTR_TIME_INTERVAL_TO_STORE_PER_ROW = "timeIntervalToStorePerRow";

  /**
   * See {@link TimeseriesTable} javadoc for description.
   */
  public static final long DEFAULT_TIME_INTERVAL_PER_ROW = TimeUnit.HOURS.toMillis(1);

  // This is a hard limit on the number of rows to read per read. This is safety-check, not intended to rely on in user
  // code. We need this check in current implementation and this may change when we have readHigherOrEq() mentioned
  // above.
  // That means that max time range to be scanned is
  // rowPartitionIntervalSize * MAX_ROWS_TO_SCAN_PER_READ
  // For 1 min intervals this is ~ 70 days, for 1 hour intervals this is ~11.5 years
  /**
   * Limit on the number of rows to scan per read.
   */
  public static final int MAX_ROWS_TO_SCAN_PER_READ = 100000;

  protected final Table table;

  @Property
  long rowPartitionIntervalSize;

  /**
   * Base constructor that only sets the name of the data set.
   */
  TimeseriesDataset(DatasetSpecification spec, Table table) {
    super(spec.getName(), table);
    this.rowPartitionIntervalSize = getIntervalSize(spec.getProperties());
    this.table = table;
  }

  /**
   * Extract the size of the time interval to store per row from the properties.
   */
  // package-visible to be accessed by dataset definition's reconfigure()
  static long getIntervalSize(Map<String, String> properties) {
    String value = properties.get(ATTR_TIME_INTERVAL_TO_STORE_PER_ROW);
    return value == null ? DEFAULT_TIME_INTERVAL_PER_ROW : Long.parseLong(value);
  }

  /**
   * Writes constructed value. This implementation overrides the existing value.
   * This method can be overridden to apply update logic relevant to the subclass (e.g. increment counter).
   *
   * @param row row key to write to
   * @param columnName column name to write to
   * @param value value passed with {@link Entry} into
   */
  void write(byte[] row, byte[] columnName, byte[] value) {
    Put put = new Put(row, columnName, value);
    table.put(put);
  }

  void write(byte[] key, byte[] value, long timestamp, byte[]... tags) {

    // Note: no need to validate entry as long as its fullness enforced by its constructor
    // Please see the class javadoc for details on the stored data format.

    byte[] row = createRow(key, timestamp, rowPartitionIntervalSize);

    // Note: we could move sorting code to Entry, but we didn't as we use same ctor when reading and we
    // don't need to sort during reading (they are already sorted asc according to storage format).
    byte[][] sortedTags = tags.clone();
    sortTags(sortedTags);

    byte[] columnName = createColumnName(timestamp, sortedTags);

    write(row, columnName, value);
  }

  long internalIncrement(byte[] counter, long amount, long timestamp, byte[]... tags) {
    byte[][] sortedTags = tags.clone();
    sortTags(sortedTags);
    byte[] columnName = createColumnName(timestamp, sortedTags);
    byte[] rowName = createRow(counter, timestamp, rowPartitionIntervalSize);
    return table.incrementAndGet(rowName, columnName, amount);
  }

  private int applyLimitOnRowsToRead(long timeIntervalsCount) {
    return (timeIntervalsCount > MAX_ROWS_TO_SCAN_PER_READ) ? MAX_ROWS_TO_SCAN_PER_READ : (int) timeIntervalsCount;
  }

  /**
   * Returns the value that will be used as the actual row key.
   * It has the following format:
   * {@code <key>[<timestamp>/<rowPartitionIntervalSize>]}.
   *
   * @param key a user-provided entry key value
   * @param timestamp is 8-byte encoded long which defines interval timestamp stamp
   * @param rowPartitionIntervalSize the size of time interval for partitioning data into rows. Used for performance
   *                    optimization. Please refer to {@link TimeseriesTable} for more details including how to choose
   *                    this value.
   * @return a composite value used as the row key
   */
  static byte[] createRow(byte[] key, long timestamp, long rowPartitionIntervalSize) {
    return Bytes.add(key, Bytes.toBytes(getRowKeyTimestampPart(timestamp, rowPartitionIntervalSize)));
  }

  private static long getRowKeyTimestampPart(final long timestamp, final long rowPartitionIntervalSize) {
    return timestamp / rowPartitionIntervalSize;
  }

  private static void sortTags(byte[][] tags) {
    Arrays.sort(tags, Bytes.BYTES_COMPARATOR);
  }

  /**
   * Returns the value that will be used as the actual column name.
   * Column name has the following format: {@code <timestamp><tags>}. Sorting of tags is needed for
   * efficient filtering based on provided tags during reading
   *
   * @param timestamp is 8-byte encoded long: user-provided entry timestamp.
   * @param tags is an encoded user-provided entry tags list. It is formatted as:
   * {@code [<tag_length><tag_value>]*}, where tag length is the 4-byte encoded int length of the tag and tags
   *             are sorted in ascending order
   */
  static byte[] createColumnName(long timestamp, byte[][] tags) {
    // hint: possible perf improvement: we can calculate the columnLength ahead of time and avoid creating many array
    //       objects

    // hint: possible perf improvement: we can actually store just the diff from the timestamp encoded in the row key
    //       and by doing that reduce the footprint of every stored entry
    // hint: consider different column name format: we may want to know "sooner" how many there are tags to make other
    //       parts of the code run faster and avoid creating too many array objects. This may be easily doable as column
    //       name is immutable.
    byte[] columnName = createColumnNameFirstPart(timestamp);
    for (byte[] tag : tags) {
      // hint: possible perf improvement: use compressed int (see Bytes.intToByte()) or at least Bytes.toBytes(short)
      //       which should be well enough
      columnName = Bytes.add(columnName, Bytes.toBytes(tag.length), tag);
    }

    return columnName;
  }

  private static byte[] createColumnNameFirstPart(final long timestamp) {
    return Bytes.toBytes(timestamp);
  }

  static long getTimeIntervalsCount(final long startTime, final long endTime,
                                    final long rowPartitionIntervalSize) {
    return (getRowKeyTimestampPart(endTime, rowPartitionIntervalSize) -
      getRowKeyTimestampPart(startTime, rowPartitionIntervalSize) + 1);
  }

  static byte[] getRowOfKthInterval(final byte[] key,
                                    final long timeRangeStart,
                                    // zero-based
                                    final int intervalIndex,
                                    final long rowPartitionIntervalSize) {
    return createRow(key, timeRangeStart + intervalIndex * rowPartitionIntervalSize, rowPartitionIntervalSize);
  }

  static boolean hasTags(final byte[] columnName) {
    // if columnName only has timestamp, then there's no tags encoded into column name
    return (columnName.length > Bytes.SIZEOF_LONG);
  }

  static long parseTimeStamp(final byte[] columnName) {
    return Bytes.toLong(columnName, 0);
  }

  /**
   * Reads entries for a given time range and returns an {@code Iterator<Entry>}. This method is intended to be
   * used by subclasses to define their own public <code>read</code> method.
   * NOTE: A limit is placed on the max number of time intervals to be scanned during a read, as defined by
   * {@link #MAX_ROWS_TO_SCAN_PER_READ}.
   *
   * @param key name of the entry to read
   * @param startTime defines start of the time range to read, inclusive
   * @param endTime defines end of the time range to read, inclusive
   * @param tags defines a set of tags that MUST present in every returned entry.
   *        NOTE: using tags returns entries containing all tags that were providing during writing
   * @return an iterator over entries that satisfy provided conditions
   */
  @ReadOnly
  final Iterator<Entry> readInternal(byte[] key, long startTime, long endTime, byte[]... tags) {
    // validating params
    if (startTime > endTime) {
      throw new IllegalArgumentException("Provided time range condition is incorrect: startTime > endTime");
    }

    return new EntryScanner(key, startTime, endTime, tags);
  }

  /**
   * Create Entry. Checking if filter tags are contained in columnName and parsing tags in one pass.
   *
   * @param key key of the entries to read
   * @param value value of the entries
   * @param columnName columnName of the entries integrated timestamp and tags
   * @param tags the tags to filter entries
   * @return an Entry by parsing tags from columnName, if the columnName contains sortedTags. Otherwise, return
   * <code>null</code>
   */
  private Entry createEntry(final byte[] key, final byte[] value, final byte[] columnName, final byte[][] tags) {
    // columnName doesn't contain tags.
    if (!hasTags(columnName)) {
      if (tags == null || tags.length == 0) {
        return new Entry(key, value, parseTimeStamp(columnName));
      }
      return null;
    }

    // columnName contains tags.
    byte[][] sortedTags = null;
    if (tags != null) {
      sortedTags = tags.clone();
      sortTags(sortedTags);
    }

    // Since we know that tags are sorted we can test match in one pass (like in merge sort)
    int curPos = Bytes.SIZEOF_LONG;
    int curTagToCheck = 0;
    List<byte[]> parsedTags = new ArrayList<>();

    while (curPos < columnName.length - 1) {
      int tagLength = Bytes.toInt(columnName, curPos);
      curPos += Bytes.SIZEOF_INT;
      int tagStartPos = curPos;
      curPos += tagLength;

      // parse tag from columnName
      if (tagLength > columnName.length) {
        return null;
      } else {
        byte[] tag = new byte[tagLength];
        System.arraycopy(columnName, tagStartPos, tag, 0, tagLength);
        parsedTags.add(tag);
      }
      // we need to parse all tags in columnName if no sortedTags is passed. And we need parse the remaining tags
      // in the columnName, after sortedTags are matched.
      if (sortedTags == null || sortedTags.length == 0 || curTagToCheck == sortedTags.length) {
        continue;
      }
      // check tags encoded in columnName against sortedTags.
      // tag is encoded in columnName array from curPos and in length of tagLength.
      int tagsMatch;
      tagsMatch = Bytes.compareTo(columnName, tagStartPos, tagLength,
                                  sortedTags[curTagToCheck], 0, sortedTags[curTagToCheck].length);
      if (tagsMatch == 0) {
        // Tags match, advancing to the next tag to be checked.
        curTagToCheck++;
      } else if (tagsMatch > 0) {
        // Tags do NOT match and fetched tag is bigger than the one we are matching against. Since tags encoded in
        // sorted order this means we will not find this tag we are matching against.
        return null;
      }
      // tagsMatch < 0 means we can advance and check against next tag encoded into the column
    }
    if (sortedTags != null && curTagToCheck < sortedTags.length) {
      // this means we didn't find all required tags in the entry data
      return null;
    }
    return new Entry(key, value, parseTimeStamp(columnName), parsedTags.toArray(new byte[parsedTags.size()][]));
  }

  /**
   * An iterator over entries.
   */
  public final class EntryScanner extends AbstractCloseableIterator<Entry> {
    private final byte[] key;
    private final long startTime;
    private final byte[][] tags;

    // the number of rows to fetch
    private final long timeIntervalsCount;

    private final byte[] startColumnName;
    private final byte[] endColumnName;

    // track the number of rows scanned through
    private int rowScanned;

    // use an internal iterator to avoid leaking AbstractIterator methods to outside.
    private Iterator<Map.Entry<byte[], byte[]>>  internalIterator;

    /**
     * Construct an EntryScanner. Should only be called by TimeseriesTable.
     *
     * @param key key of the entries to read
     * @param startTime defines start of the time range to read, inclusive
     * @param endTime defines end of the time range to read, inclusive
     * @param tags defines a set of tags that MUST present in every returned entry.
     *        NOTE: using tags returns entries containing all tags that were providing during writing
     */
    EntryScanner(byte[] key, long startTime, long endTime, byte[][] tags) {
      this.key = key;
      this.startTime = startTime;
      this.tags = tags;

      // calculating time intervals (i.e. rows, as one row = one time interval) to fetch.
      long timeIntervals = getTimeIntervalsCount(startTime, endTime, rowPartitionIntervalSize);
      timeIntervalsCount = applyLimitOnRowsToRead(timeIntervals);
      // Note: do NOT use tags when calculating start/stop column keys due to the column name format.
      startColumnName = createColumnNameFirstPart(startTime);
      endColumnName = createColumnNameFirstPart(endTime + 1);
      internalIterator = null;
    }

    @Override
    protected Entry computeNext() {
      while ((internalIterator == null || !internalIterator.hasNext()) && rowScanned < timeIntervalsCount) {
        byte[] row = getRowOfKthInterval(key, startTime, rowScanned, rowPartitionIntervalSize);
        internalIterator = createIterator(row);
        rowScanned++;
      }
      if (rowScanned <= timeIntervalsCount && internalIterator != null && internalIterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = internalIterator.next();
        Entry returnValue = createEntry(key, entry.getValue(), entry.getKey(), tags);
        if (returnValue == null) {
          return computeNext();
        }
        return returnValue;
      }
      return endOfData();
    }

    private Iterator<Map.Entry<byte[], byte[]>> createIterator(byte[] row) {
      Row currentRow = table.get(row,
                                 // we only need to set left bound on the first row: others cannot have records
                                 // with the timestamp less than startTime
                                 (rowScanned == 0) ? startColumnName : null,
                                 // we only need to set right bound on the last row: others cannot have records
                                 // with the timestamp greater than startTime
                                 (rowScanned == timeIntervalsCount - 1) ? endColumnName : null,
                                 // read all
                                 -1);

      if (!currentRow.isEmpty()) {
        return currentRow.getColumns().entrySet().iterator();
      }

      return null;
    }

    @Override
    public void close() {
      // no op for now since the internal scanner is created from Row, which is a local byte[]
    }
  }

  /**
   * Time series DataSet entry.
   */
  static class Entry {
    private byte[] key;
    private byte[] value;
    private long timestamp;
    private byte[][] tags;

    /**
     * Creates instance of the time series entry.
     *
     * @param key key of the entry. E.g. "metric1"
     * @param value value to store
     * @param timestamp timestamp of the entry
     * @param tags optional list of tags associated with the entry
     */
    Entry(final byte[] key, final byte[] value, final long timestamp, final byte[]... tags) {
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
      this.tags = tags;
    }

    /**
     * Returns the key of the entry.
     * @return the key of the entry
     */
    public byte[] getKey() {
      return key;
    }

    /**
     * Returns the count value of the entry.
     * @return the count value of the entry
     */
    public byte[] getValue() {
      return value;
    }

    /**
     * Returns the timestamp of the entry.
     * @return the timestamp of the entry
     */
    public long getTimestamp() {
      return timestamp;
    }

    /**
     * Returns the tags associated with the entry.
     * @return the tags associated with the entry
     */
    public byte[][] getTags() {
      return tags;
    }
  }
}
