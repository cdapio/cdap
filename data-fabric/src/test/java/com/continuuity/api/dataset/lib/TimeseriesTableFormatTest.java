/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.dataset.lib;

import com.continuuity.api.common.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Time series table format test.
 */
public class TimeseriesTableFormatTest {
  // TODO: move to TimeseriesTable test?
  @Test
  public void testColumnNameFormat() {
    // Note: tags are sorted lexographically
    byte[] tag1 = Bytes.toBytes("111");
    byte[] tag2 = Bytes.toBytes("22");
    byte[] tag3 = Bytes.toBytes("3");

    long ts = System.currentTimeMillis();
    byte[][] tags = { tag1, tag2, tag3 };
    byte[] columnName = TimeseriesTable.createColumnName(ts, tags);

    byte[] tag4 = Bytes.toBytes("34");

    Assert.assertEquals(ts, TimeseriesTable.parseTimeStamp(columnName));
    Assert.assertTrue(TimeseriesTable.hasTags(columnName));
    Assert.assertTrue(TimeseriesTable.containsTags(columnName, new byte[][]{tag1, tag2, tag3}));
    Assert.assertTrue(TimeseriesTable.containsTags(columnName, new byte[][]{tag2}));
    Assert.assertTrue(TimeseriesTable.containsTags(columnName, new byte[][]{tag1}));
    Assert.assertTrue(TimeseriesTable.containsTags(columnName, new byte[][]{tag3}));
    Assert.assertFalse(TimeseriesTable.containsTags(columnName, new byte[][]{tag4}));
    Assert.assertFalse(TimeseriesTable.containsTags(columnName, new byte[][]{tag1, tag4}));
    Assert.assertFalse(TimeseriesTable.containsTags(columnName, new byte[][]{tag2, tag3, tag4}));
    Assert.assertFalse(TimeseriesTable.containsTags(columnName, new byte[][]{tag1, tag2, tag3, tag4}));

    assertEquals(tags, TimeseriesTable.parseTags(columnName));
  }

  @Test
  public void testColumnNameFormatWithNoTags() {
    long ts = System.currentTimeMillis();
    byte[] columnName = TimeseriesTable.createColumnName(ts, new byte[0][]);
    Assert.assertEquals(ts, TimeseriesTable.parseTimeStamp(columnName));
    Assert.assertFalse(TimeseriesTable.hasTags(columnName));
    Assert.assertFalse(TimeseriesTable.containsTags(columnName, new byte[][]{Bytes.toBytes("tag")}));
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
    Assert.assertArrayEquals(TimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                             TimeseriesTable.createRow(key, ts1 + timeIntervalPerRow / 2, timeIntervalPerRow));
    // * If the timestamps don't fall into same time interval then row keys are different
    Assert.assertFalse(
      Arrays.equals(TimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                    TimeseriesTable.createRow(key, ts1 + timeIntervalPerRow * 2, timeIntervalPerRow)));

    // * If timestamp A > timestamp B then row key A > row key B (we will have some optimization logic based on that)
    Assert.assertTrue(Bytes.compareTo(TimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                                      TimeseriesTable.createRow(key, ts1 + timeIntervalPerRow * 5,
                                                                timeIntervalPerRow)) < 0);

    // * For different keys the constructed rows are different
    byte[] key2 = Bytes.toBytes("KEY2");
    Assert.assertFalse(Arrays.equals(
                                      TimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                                     TimeseriesTable.createRow(key2, ts1, timeIntervalPerRow)));

    // * We can get all possible rows for a given time interval with getRowsForInterval
    List<byte[]> rows = new ArrayList<byte[]>();
    long startTime = timeIntervalPerRow * 4;
    long endTime = timeIntervalPerRow * 100;
    for (long i = startTime; i <= endTime; i += timeIntervalPerRow / 10) {
      byte[] row = TimeseriesTable.createRow(key, i, timeIntervalPerRow);
      if (rows.isEmpty() || !Arrays.equals(rows.get(rows.size() - 1), row)) {
        rows.add(row);
      }
    }

    int intervalsCount = (int) TimeseriesTable.getTimeIntervalsCount(startTime, endTime, timeIntervalPerRow);
    byte[][] rowsForInterval = new byte[intervalsCount][];
    for (int i = 0; i < intervalsCount; i++) {
      rowsForInterval[i] = TimeseriesTable.getRowOfKthInterval(key, startTime, i, timeIntervalPerRow);
    }
    assertEquals(rows.toArray(new byte[rows.size()][]), rowsForInterval);

    // same requirement, one more test (covers some bug I fixed)
    Assert.assertEquals(2,
                        TimeseriesTable.getTimeIntervalsCount(
                          timeIntervalPerRow + 1,
                          timeIntervalPerRow * 2 + 1, timeIntervalPerRow
                        ));
  }

  private void assertEquals(final byte[][] left, final byte[][] right) {
    Assert.assertEquals(left.length, right.length);
    for (int i = 0; i < left.length; i++) {
      Assert.assertArrayEquals(left[i], right[i]);
    }
  }
}
