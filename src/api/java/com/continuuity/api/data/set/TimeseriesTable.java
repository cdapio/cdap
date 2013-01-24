/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.set;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.util.Bytes;
import scala.actors.threadpool.Arrays;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TimeseriesTable extends DataSet {
  // 1 hour
  public static final int DEFAULT_TIME_INTERVAL_PER_ROW = 60 * 60 * 1000;


  private String tableName;

  private Table table;

  private long timeIntervalToStorePerRow;

  @SuppressWarnings("unused")
  public TimeseriesTable(DataSetSpecification spec) throws OperationException {
    super(spec);
    this.init(this.getName(), Long.valueOf(spec.getProperty("timeIntervalToStorePerRow")));
    this.table = new Table(spec.getSpecificationFor(this.tableName));
  }

  public TimeseriesTable(final String name) {
    this(name, DEFAULT_TIME_INTERVAL_PER_ROW);
  }

  public TimeseriesTable(final String name, final int timeIntervalToStorePerRow) {
    super(name);
    this.init(name, timeIntervalToStorePerRow);
    this.table = new Table(tableName);
  }


  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
             .property("timeIntervalToStorePerRow", String.valueOf(timeIntervalToStorePerRow))
             .dataset(this.table.configure())
             .create();
  }

  // TODO: have separate for read and write?
  public static final class Entry {
    private byte[] key;
    private byte[] value;
    private long timestamp;
    private byte[][] tags;

    public Entry(final byte[] key, final byte[] value, final long timestamp) {
      // TODO: allow to have null in tags to avoid creating redundant objects?
      this(key, value, timestamp, new byte[0][]);
    }

    public Entry(final byte[] key, final byte[] value, final long timestamp, final byte[]... tags) {
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
      this.tags = tags;
    }

    public byte[] getKey() {
      return key;
    }

    public byte[] getValue() {
      return value;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public byte[][] getTags() {
      return tags;
    }
  }

  public void write(Entry entry) throws OperationException {
    // TODO: validate params?

    byte[] row = getRow(entry.key, entry.timestamp, timeIntervalToStorePerRow);
    // TODO: create copy of array?
    byte[][] tags = entry.tags;
    sortTags(tags);
    byte[] columnName = getColumnName(entry.timestamp, tags);
    Table.Write write = new Table.Write(row, columnName, entry.value);
    table.stage(write);
  }

  // TODO: define SelectQuery with nice builder?
  public List<Entry> read(byte key[], long startTime, long endTime) throws OperationException {
    return read(key, startTime, endTime, new byte[0][]);
  }

  // TODO: define SelectQuery with nice builder?
  public List<Entry> read(byte key[], long startTime, long endTime, byte[]... tags) throws OperationException {
    // TODO: validate params?

    // TODO: create copy of array?
    sortTags(tags);
    byte[][] rows = getRowsForInterval(key, startTime, endTime, timeIntervalToStorePerRow);
    // Note: do NOT use tags when calculating start/stop column keys due to the column name format
    byte[] startColumnName = getColumnName(startTime);
    byte[] endColumnName = getColumnName(endTime);
    List<Entry> resultList = new ArrayList<Entry>();
    for (byte[] row : rows) {
      // since column names start with timestamp, it is OK to pass start/stop column names even outside of the interval
      // of the specific row. TODO: however, this is not efficient performance wise, as we add restrictions when
      // fetching all rows (which is not necessary: only first and last should have restrictions) which could be very
      // expensive depending on the table.read() implementation.
      Table.Read read = new Table.Read(row, startColumnName, endColumnName);
      OperationResult<Map<byte[], byte[]>> result = table.read(read);
      if (!result.isEmpty()) {
        for (Map.Entry<byte[], byte[]> cv : result.getValue().entrySet()) {
          // note: we don't need to check time interval as we enforce it thru start/stop columns on Read
          // TODO: possible perf improvement: we can do tags match and Entry parsing at same single run
          if (containsTags(cv.getKey(), tags)) {
            Entry entry = parse(key, cv.getKey(), cv.getValue());
            resultList.add(entry);
          }
        }
      }
    }

    // TODO: sort result list? Does table.read returns columns in sorted order in Map? Doubt it since
    //       it doesn't return SortedMap

    return resultList;
  }

  private void init(String name, long timeIntervalToStorePerRow) {
    this.tableName = "ts_" + name;
    this.timeIntervalToStorePerRow = timeIntervalToStorePerRow;
  }

  static byte[] getRow(final byte[] key, final long timestamp, long timeIntervalToStorePerRow) {
    return Bytes.add(key, Bytes.toBytes(timestamp / timeIntervalToStorePerRow));
  }

  private void sortTags(final byte[][] tags) {
    Arrays.sort(tags, Bytes.BYTES_COMPARATOR);
  }

  static byte[] getColumnName(long timestamp, byte[][] tags) {
    // TODO: possible perf improvement: we can calculate the columnLength ahead of time and avoid creating many array objects

    // TODO: possible perf provement: we can actually store just the diff from the timestamp encoded in the row key
    // TODO: consider different column name format: we may want to know "sooner" how many there are tags to make other
    //       parts of the code run faster and avoid creating too many array objects
    byte[] columnName = getColumnName(timestamp);
    for (byte[] tag : tags) {
      // TODO: possible perf improvement: use compressed int (see Bytes.vintToByte())
      columnName = Bytes.add(columnName, Bytes.toBytes(tag.length), tag);
    }

    return columnName;
  }

  private static byte[] getColumnName(final long timestamp) {
    return Bytes.toBytes(timestamp);
  }

  static byte[][] getRowsForInterval(final byte[] key, final long startTime, final long endTime,
                                     final long timeIntervalToStorePerRow) {
    // TODO: this will fail with OOEM if interval is too big. Consider refactoring the logic ;)
    int count = (int) ((endTime - startTime + (timeIntervalToStorePerRow - 1)) / timeIntervalToStorePerRow);
    byte[][] rows = new byte[count][];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = getRow(key, startTime + i * timeIntervalToStorePerRow, timeIntervalToStorePerRow);
    }
    return rows;
  }

  static boolean containsTags(final byte[] columnName, final byte[][] sortedTags) {
    if (sortedTags.length == 0) {
      return true;
    }

    if (!hasTags(columnName)) {
      return false;
    }

    // Note: it is assumed that tags are sorted

    // Since we know that tags are sorted we can test match in one pass TODO: describe algo
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
      // this means we didn't find all tags we were looking for
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
}