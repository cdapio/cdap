/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.dataset;

import com.continuuity.api.annotation.Property;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.IteratorBasedSplitReader;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.table.Put;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Provides simple implementation of time series table.
 * <p>
 * The easiest way to give an insight of the usage details is to describe the format in which data is
 * stored:
 * </p>
 * <ul>
 *   <li>
 *     All entries are logically partitioned into time intervals of the same size based on the entry timestamp.
 *   </li>
 *   <li>
 *     Every row in underlying table holds entries of the same time interval with the same key.
 *   </li>
 *   <li>
 *     Each entry's data is stored in one column.
 *   </li>
 * </ul>
 * Time interval length for partitioning can be defined by the user and should be chosen 
 * depending on the use-case. <br/>
 * Bigger time interval to store per row means:
 * <ul>
 *   <li>
 *     More data is stored per row. Too many data stored per row should be avoided: it is usually not a good idea to
 *     store more than tens of megabytes per row.
 *   </li>
 *   <li>
 *     Faster reading of small-to-medium time ranges (range size is up to several time intervals) of entries data.
 *   </li>
 *   <li>
 *     Slower reading of very small time ranges (range size is a small portion of time interval) of entries data.
 *   </li>
 *   <li>
 *     Faster batched writing of entries.
 *   </li>
 * </ul>
 * Smaller time interval to store per row on the other hand means:
 * <ul>
 *   <li>
 *     Faster reading of very small time ranges (range size is a small portion of time interval) of entries data.
 *   </li>
 *   <li>
 *     Slower batched writing of entries.
 *   </li>
 * </ul>
 *
 * YMMV, but usually you want the value to be between 1 minute and several hours. Default value is 1 hour. In case
 * amount of written entries is not big the rule of thumb could be
 * "time interval to store per row = [1..10] * (average size of the time range to be read)".
 *
 * <p>
 * Due to the data format used for storing, filtering by tags during reading is done on client-side (not on a cluster).
 * At the same time filtering by entries keys happens on the server side which is much more performance efficient.
 * Depending on the use-case you may want to push some of the tags you would use into the entry key for faster reading.
 * </p>
 *
 * <p>
 * NOTES:
 * <ol>
 *   <li>
 *    This implementation does NOT address RegionServer hot-spotting issue that appears when writing rows with
 *       monotonically increasing/decreasing keys into HBase. Which is relevant for HBase-based back-end.
 *       To avoid this problem user should NOT write all data under same metric key. In general, writes will be as
 *       distributed as the amount of different metric keys the data is written for. Having one metric key would mean
 *       hitting single RegionServer at any given point of time with all writes. Which is usually NOT desired.
 *   </li>
 *   <li>
 *    The current implementation (incl. the format of the stored data) is heavily affected by
 *    {@link com.continuuity.api.data.dataset.table.Table} API which
 *       is used under the hood. In particular the implementation is constrained by the absence of
 *       <code>readHigherOrEq()</code> method in {@link com.continuuity.api.data.dataset.table.Table} API,
 *       which would  return next row with key greater or equals to the given.<br/>
 *   </li>
 *   <li>
 *    The client code should not rely on the implementation details: they can be changed without a notice.
 *   </li>
 * </ol>
 * </p>
 *
 * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.dataset.lib.TimeseriesTable}
 */
@Deprecated
public class SimpleTimeseriesTable extends DataSet
  implements TimeseriesTable,
             BatchReadable<byte[], TimeseriesTable.Entry>,
             BatchWritable<byte[], TimeseriesTable.Entry> {
  /**
   * 1 hour. See class javadoc for description. Can be overridden by client code.
    */
  public static final int DEFAULT_TIME_INTERVAL_PER_ROW = 60 * 60 * 1000;

  private static final String ATTR_TIME_INTERVAL_TO_STORE_PER_ROW = "timeIntervalToStorePerRow";

  // This is a hard limit on the number of rows to scan per read. This is safety-check, not intended to rely on in user
  // code. We need this check in current implementation and this may change when we have readHigherOrEq() mentioned
  // above.
  // That means that max time range to be scanned is
  // timeIntervalToStorePerRow * MAX_ROWS_TO_SCAN_PER_READ
  // For 1 min intervals this is ~ 70 days, for 1 hour intervals this is ~11.5 years
  private static final int MAX_ROWS_TO_SCAN_PER_READ = 100000;

  private Table table;

  @Property
  private long timeIntervalToStorePerRow;

  /**
   * Creates instance of the table.
   * @param name name of the dataset.
   */
  public SimpleTimeseriesTable(final String name) {
    this(name, DEFAULT_TIME_INTERVAL_PER_ROW, -1);
  }

  /**
   * Creates instance of the table.
   * @param name name of the dataset.
   * @param timeIntervalToStorePerRow time interval to store per row. Please refer to the class javadoc for more details
   *                                  including how to choose this value.
   */
  public SimpleTimeseriesTable(final String name, final int timeIntervalToStorePerRow) {
    this(name, timeIntervalToStorePerRow, -1);
  }

  /**
   * Creates instance of the table.
   * @param name name of the dataset.
   * @param timeIntervalToStorePerRow time interval to store per row. Please refer to the class javadoc for more details
   *                                  including how to choose this value.
   * @param ttl time to live for the data in ms, negative means unlimited.
   */
  public SimpleTimeseriesTable(final String name, final int timeIntervalToStorePerRow, final int ttl) {
    super(name);
    this.timeIntervalToStorePerRow = timeIntervalToStorePerRow;
    this.table = new Table("ts", ttl);
  }

  /**
   * Writes an entry. See {@link com.continuuity.api.data.dataset.TimeseriesTable#write(Entry)}
   * for more details on usage.
   * @param entry to write
   */
  @Override
  public void write(Entry entry) {
    Put put = createPut(entry);
    table.put(put);
  }

  /**
   * Reads entries of a time range.
   * See {@link TimeseriesTable#read} for more details
   * on usage.<br/>
   * NOTE: There's a hard limit on the max number of time intervals to be scanned during read. Defined in
   * MAX_ROWS_TO_SCAN_PER_READ parameter.
   *
   * @param key key of the entries to read
   * @param startTime defines start of the time range to read, inclusive.
   * @param endTime defines end of the time range to read, inclusive.
   * @param tags defines a set of tags that MUST present in every returned entry.
   *        NOTE: return entries contain all tags that were providing during writing, NOT passed with this param.
   *
   * @return list of entries that satisfy provided conditions.
   * @throws IllegalArgumentException when provided condition is incorrect.
   */
  @Override
  public List<Entry> read(byte key[], long startTime, long endTime, byte[]... tags) {
    // validating params
    Preconditions.checkArgument(startTime <= endTime,
                                "Provided time range condition is incorrect: startTime > endTime");

    // Note: do NOT use tags when calculating start/stop column keys due to the column name format
    byte[] startColumnName = createColumnNameFirstPart(startTime);
    // +1 here is because we want inclusive behaviour on both ends, while Table API excludes end column
    byte[] endColumnName = createColumnNameFirstPart(endTime + 1);

    // logic which filters entries by tags (used in loop below) relies on provided tags to be in sorted asc order
    tags = tags.clone();
    sortTags(tags);

    // calculating time intervals (i.e. rows, as one row = one time interval) to fetch
    long timeIntervals = getTimeIntervalsCount(startTime, endTime, timeIntervalToStorePerRow);
    int timeIntervalsCount = applyLimitOnRowsToRead(timeIntervals);

    // Reading records one-by-one, fetching entries from their columns and filtering based on provided tags.
    List<Entry> resultList = new ArrayList<Entry>();
    for (int i = 0; i < timeIntervalsCount; i++) {
      byte[] row = getRowOfKthInterval(key, startTime, i, timeIntervalToStorePerRow);

      Row result = table.get(row,
                                       // we only need to set left bound on the first row: others cannot have records
                                       // with the timestamp less than startTime
                                       (i == 0) ? startColumnName : null,
                                       // we only need to set right bound on the last row: others cannot have records
                                       // with the timestamp greater than startTime
                                       (i == timeIntervalsCount - 1) ? endColumnName : null,
                                       // read all
                                       -1);

      if (!result.isEmpty()) {
        for (Map.Entry<byte[], byte[]> cv : result.getColumns().entrySet()) {
          // note: we don't need to check time interval as we enforce it thru start/stop columns on Read, but we need
          //       to filter by tags
          // hint: possible perf improvement: we can do tags match and Entry parsing at the same time, i.e. in on pass
          if (containsTags(cv.getKey(), tags)) {
            Entry entry = parse(key, cv.getKey(), cv.getValue());
            resultList.add(entry);
          }
        }
      }
    }

    return resultList;
  }

  private Put createPut(final Entry entry) {
    // Note: no need to validate entry as long as its fullness enforced by its constructor
    // Please see the class javadoc for details on the stored data format.

    byte[] row = createRow(entry.getKey(), entry.getTimestamp(), timeIntervalToStorePerRow);

    // Note: we could move sorting code to Entry, but we didn't as we use same ctor when reading and we don't need to
    // sort during reading (they are already sorted asc according to storage format).
    byte[][] tags = entry.getTags().clone();
    sortTags(tags);

    byte[] columnName = createColumnName(entry.getTimestamp(), tags);
    return new Put(row, columnName, entry.getValue());
  }

  private int applyLimitOnRowsToRead(final long timeIntervalsCount) {
    return (timeIntervalsCount > MAX_ROWS_TO_SCAN_PER_READ) ? MAX_ROWS_TO_SCAN_PER_READ : (int) timeIntervalsCount;
  }

  /**
   * Row keys in underlying table have the following format: <code>&lt;entry_key>&lt;interval_timestamp></code>.
   * <code>entry_key</code> is a user-provided entry key value.
   * <code>interval_timestamp</code> is 8-byte encoded long which defines interval timestamp start.<br/>
   * I.e. rows are stored in ascending time order.
   */
  static byte[] createRow(final byte[] key, final long timestamp, long timeIntervalToStorePerRow) {
    return Bytes.add(key, Bytes.toBytes(getRowKeyTimestampPart(timestamp, timeIntervalToStorePerRow)));
  }

  private static long getRowKeyTimestampPart(final long timestamp, final long timeIntervalToStorePerRow) {
    return timestamp / timeIntervalToStorePerRow;
  }

  private void sortTags(final byte[][] tags) {
    Arrays.sort(tags, Bytes.BYTES_COMPARATOR);
  }

  /** Column name has the following format: <entry_timestamp><encoded_entry_tags>.
   * <entry_timestamp> is 8-byte encoded long: user-provided entry timestamp.
   * <encoded_entry_tags> is an encoded user-provided entry tags list. It has the following format:
   * [<tag_length><tag_value>]*, where tag length is 4-byte encoded int length of the tag and tags are sorted in asc
   * order. Sorting is needed for efficient filtering based on provided tags during reading.
   */
  static byte[] createColumnName(long timestamp, byte[][] tags) {
    // hint: possible perf improvement: we can calculate the columnLength ahead of time and avoid creating many array
    //       objects

    // hint: possible perf provement: we can actually store just the diff from the timestamp encoded in the row key and
    //       by doing that reduce the footprint of every stored entry
    // hint: consider different column name format: we may want to know "sooner" how many there are tags to make other
    //       parts of the code run faster and avoid creating too many array objects. This may be easily doable as column
    //       name is immutable.
    byte[] columnName = createColumnNameFirstPart(timestamp);
    for (byte[] tag : tags) {
      // hint: possible perf improvement: use compressed int (see Bytes.vintToByte()) or at least Bytes.toBytes(short)
      //       which should be well enough
      columnName = Bytes.add(columnName, Bytes.toBytes(tag.length), tag);
    }

    return columnName;
  }

  private static byte[] createColumnNameFirstPart(final long timestamp) {
    return Bytes.toBytes(timestamp);
  }

  static long getTimeIntervalsCount(final long startTime, final long endTime,
                                           final long timeIntervalToStorePerRow) {
    return (getRowKeyTimestampPart(endTime, timeIntervalToStorePerRow) -
                  getRowKeyTimestampPart(startTime, timeIntervalToStorePerRow) + 1);
  }

  static byte[] getRowOfKthInterval(final byte[] key,
                                            final long timeRangeStart,
                                            // zero-based
                                            final int intervalIndex,
                                            final long timeIntervalToStorePerRow) {
    return createRow(key, timeRangeStart + intervalIndex * timeIntervalToStorePerRow, timeIntervalToStorePerRow);
  }


  // Note: it is assumed that passed tags are sorted (asc)
  static boolean containsTags(final byte[] columnName, final byte[][] sortedTags) {
    if (sortedTags.length == 0) {
      return true;
    }

    if (!hasTags(columnName)) {
      return false;
    }

    // Since we know that tags are sorted we can test match in one pass (like in merge sort)
    int curPos = Bytes.SIZEOF_LONG;
    int curTagToCheck = 0;

    while ((curTagToCheck < sortedTags.length) && (curPos < columnName.length - 1)) {
      int tagLength = Bytes.toInt(columnName, curPos);
      curPos += Bytes.SIZEOF_INT;
      int tagStartPos = curPos;
      curPos += tagLength;

      // tag is encoded in columnName array from  curPos is tagLength bytes in length
      int tagsMatch = Bytes.compareTo(columnName, tagStartPos, tagLength,
                                      sortedTags[curTagToCheck], 0, sortedTags[curTagToCheck].length);

      if (tagsMatch == 0) {
        // Tags match, advancing to the next tag to be checked.
        curTagToCheck++;
      } else if (tagsMatch > 0) {
        // Tags do NOT match and fetched tag is bigger than the one we are matching against. Since tags encoded in
        // sorted order this means we will not find this tag we are matching against.
        return false;
      }
      // tagsMatch < 0 means we can advance and check against next tag encoded into the column
    }

    if (curTagToCheck < sortedTags.length) {
      // this means we didn't find all required tags in the entry data
      return false;
    }

    return true;
  }

  static boolean hasTags(final byte[] columnName) {
    // if it only has timestamp, then there's no tags encoded into column name
    return (columnName.length > Bytes.SIZEOF_LONG);
  }

  private Entry parse(final byte[] key, final byte[] columnName, final byte[] value) {
    long timestamp = parseTimeStamp(columnName);
    byte[][] tags = parseTags(columnName);
    return new Entry(key, value, timestamp, tags);
  }

  static long parseTimeStamp(final byte[] columnName) {
    return Bytes.toLong(columnName, 0);
  }

  static byte[][] parseTags(final byte[] columnName) {
    if (!(columnName.length > Bytes.SIZEOF_LONG)) {
      return new byte[0][];
    }

    List<byte[]> tags = new ArrayList<byte[]>();
    int curPos = Bytes.SIZEOF_LONG;
    while (curPos < columnName.length - 1) {
      int tagLength = Bytes.toInt(columnName, curPos);
      curPos += Bytes.SIZEOF_INT;
      byte[] tag = Arrays.copyOfRange(columnName, curPos, curPos + tagLength);
      curPos += tagLength;

      tags.add(tag);
    }

    return tags.toArray(new byte[tags.size()][]);
  }

  /////// Methods for using DataSet as input for MapReduce job

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
   * Defines input selection for Batch job.
   * @param splitsCount number of parts to split the data selection into. Each piece
   */
  public List<Split> getInput(int splitsCount, byte[] key, long startTime, long endTime, byte[]... tags) {
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
    throw new UnsupportedOperationException("Cannot use SimpleTimeseriesTable as input for Batch," +
                                              " use getInput(...) to configure data selection.");
  }

  @Override
  public SplitReader<byte[], Entry> createSplitReader(final Split split) {
    return new TimeseriesTableRecordsReader();
  }

  /**
   * A record reader for time series.
   */
  public final class TimeseriesTableRecordsReader
    extends IteratorBasedSplitReader<byte[], Entry> {
    @Override
    public Iterator<Entry> createIterator(final Split split) {

      InputSplit s = (InputSplit) split;

      // TODO: avoid reading all data at once :)
      List<TimeseriesTable.Entry> data = SimpleTimeseriesTable.this.read(s.key, s.startTime, s.endTime, s.tags);
      return data.iterator();
    }

    @Override
    protected byte[] getKey(Entry entry) {
      return entry.getKey();
    }
  }

  /////// Methods for using DataSet as output of MapReduce job

  @Override
  public void write(final byte[] key, final Entry value) {
    write(value);
  }

}
