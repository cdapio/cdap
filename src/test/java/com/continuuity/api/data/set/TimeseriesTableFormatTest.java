/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.set;

import com.continuuity.api.data.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimeseriesTableFormatTest {
  @Test
  public void testColumnNameFormat() {
    // Note: tags are sorted lexographically
    byte[] tag1 = Bytes.toBytes("111");
    byte[] tag2 = Bytes.toBytes("22");
    byte[] tag3 = Bytes.toBytes("3");

    long ts = System.currentTimeMillis();
    byte[][] tags = { tag1, tag2, tag3 };
    byte[] columnName = TimeseriesTable.getColumnName(ts, tags);

    byte[] tag4 = Bytes.toBytes("34");

    Assert.assertEquals(ts, TimeseriesTable.parseTimeStamp(columnName));
    Assert.assertTrue(TimeseriesTable.hasTags(columnName));
    Assert.assertTrue(TimeseriesTable.containsTags(columnName, new byte[][] { tag1, tag2, tag3 }));
    Assert.assertTrue(TimeseriesTable.containsTags(columnName, new byte[][] { tag2 }));
    Assert.assertTrue(TimeseriesTable.containsTags(columnName, new byte[][] { tag1 }));
    Assert.assertTrue(TimeseriesTable.containsTags(columnName, new byte[][] { tag3 }));
    Assert.assertFalse(TimeseriesTable.containsTags(columnName, new byte[][] { tag4 }));
    Assert.assertFalse(TimeseriesTable.containsTags(columnName, new byte[][] { tag1, tag4 }));
    Assert.assertFalse(TimeseriesTable.containsTags(columnName, new byte[][] { tag2, tag3, tag4 }));
    Assert.assertFalse(TimeseriesTable.containsTags(columnName, new byte[][] { tag1, tag2, tag3, tag4 }));

    assertEquals(tags, TimeseriesTable.parseTags(columnName));
  }

  @Test
  public void testColumnNameFormatWithNoTags() {
    long ts = System.currentTimeMillis();
    byte[] columnName = TimeseriesTable.getColumnName(ts, new byte[0][]);
    Assert.assertEquals(ts, TimeseriesTable.parseTimeStamp(columnName));
    Assert.assertFalse(TimeseriesTable.hasTags(columnName));
    Assert.assertFalse(TimeseriesTable.containsTags(columnName, new byte[][]{ Bytes.toBytes("tag") }));
    assertEquals(new byte[0][], TimeseriesTable.parseTags(columnName));
  }

  @Test
  public void testRowFormat() {
    // 1 min
    long timeIntervalPerRow = 60 * 1000;
    byte[] key = Bytes.toBytes("key");

    long ts1 = timeIntervalPerRow + 1;

    // These are the things we care about:

    // * If the timestamps fall into same time interval then row keys are same
    Assert.assertArrayEquals(TimeseriesTable.getRow(key, ts1, timeIntervalPerRow),
                             TimeseriesTable.getRow(key, ts1 + timeIntervalPerRow / 2, timeIntervalPerRow));
    // * If the timestamps don't fall into same time interval then row keys are different
    Assert.assertFalse(Arrays.equals(TimeseriesTable.getRow(key, ts1, timeIntervalPerRow),
                             TimeseriesTable.getRow(key, ts1 + timeIntervalPerRow * 2, timeIntervalPerRow)));

    // * If timestamp A > timestamp B then row key A > row key B (we will have some optimization logic based on that)
    Assert.assertTrue(Bytes.compareTo(TimeseriesTable.getRow(key, ts1, timeIntervalPerRow),
                                    TimeseriesTable.getRow(key, ts1 + timeIntervalPerRow * 5, timeIntervalPerRow)) < 0);

    // * For different keys the constructed rows are different
    byte[] key2 = Bytes.toBytes("key2");
    Assert.assertFalse(Arrays.equals(TimeseriesTable.getRow(key, ts1, timeIntervalPerRow),
                                     TimeseriesTable.getRow(key2, ts1, timeIntervalPerRow)));

    // * We can get all possible rows for a given time interval with getRowsForInterval
    List<byte[]> rows = new ArrayList<byte[]>();
    long startTime = timeIntervalPerRow * 4;
    long endTime = timeIntervalPerRow * 100;
    for (long i = startTime; i < endTime; i += timeIntervalPerRow / 10) {
      byte[] row = TimeseriesTable.getRow(key, i, timeIntervalPerRow);
      if (rows.size() == 0 || !Arrays.equals(rows.get(rows.size() - 1), row)) {
        rows.add(row);
      }
    }

    byte[][] rowsForInterval = TimeseriesTable.getRowsForInterval(key, startTime, endTime, timeIntervalPerRow);
    assertEquals(rows.toArray(new byte[rows.size()][]), rowsForInterval);
  }

  private void assertEquals(final byte[][] left, final byte[][] right) {
    Assert.assertEquals(left.length, right.length);
    for (int i = 0; i < left.length; i++) {
      Assert.assertArrayEquals(left[i], right[i]);
    }
  }
}
