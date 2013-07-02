/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Time series table format test.
 */
public class SimpleTimeseriesTableFormatTest {
  // TODO: move to SimpleTimeseriesTable test?
  @Test
  public void testColumnNameFormat() {
    // Note: tags are sorted lexographically
    byte[] tag1 = Bytes.toBytes("111");
    byte[] tag2 = Bytes.toBytes("22");
    byte[] tag3 = Bytes.toBytes("3");

    long ts = System.currentTimeMillis();
    byte[][] tags = { tag1, tag2, tag3 };
    byte[] columnName = SimpleTimeseriesTable.createColumnName(ts, tags);

    byte[] tag4 = Bytes.toBytes("34");

    Assert.assertEquals(ts, SimpleTimeseriesTable.parseTimeStamp(columnName));
    Assert.assertTrue(SimpleTimeseriesTable.hasTags(columnName));
    Assert.assertTrue(SimpleTimeseriesTable.containsTags(columnName, new byte[][]{ tag1, tag2, tag3 }));
    Assert.assertTrue(SimpleTimeseriesTable.containsTags(columnName, new byte[][]{ tag2 }));
    Assert.assertTrue(SimpleTimeseriesTable.containsTags(columnName, new byte[][]{ tag1 }));
    Assert.assertTrue(SimpleTimeseriesTable.containsTags(columnName, new byte[][]{ tag3 }));
    Assert.assertFalse(SimpleTimeseriesTable.containsTags(columnName, new byte[][]{ tag4 }));
    Assert.assertFalse(SimpleTimeseriesTable.containsTags(columnName, new byte[][]{ tag1, tag4 }));
    Assert.assertFalse(SimpleTimeseriesTable.containsTags(columnName, new byte[][]{ tag2, tag3, tag4 }));
    Assert.assertFalse(SimpleTimeseriesTable.containsTags(columnName, new byte[][]{ tag1, tag2, tag3, tag4 }));

    assertEquals(tags, SimpleTimeseriesTable.parseTags(columnName));
  }

  @Test
  public void testColumnNameFormatWithNoTags() {
    long ts = System.currentTimeMillis();
    byte[] columnName = SimpleTimeseriesTable.createColumnName(ts, new byte[0][]);
    Assert.assertEquals(ts, SimpleTimeseriesTable.parseTimeStamp(columnName));
    Assert.assertFalse(SimpleTimeseriesTable.hasTags(columnName));
    Assert.assertFalse(SimpleTimeseriesTable.containsTags(columnName, new byte[][]{ Bytes.toBytes("tag") }));
    assertEquals(new byte[0][], SimpleTimeseriesTable.parseTags(columnName));
  }

  @Test
  public void testRowFormat() {
    // 1 min
    long timeIntervalPerRow = 60 * 1000;
    byte[] key = Bytes.toBytes("key");

    long ts1 = timeIntervalPerRow + 1;

    // These are the things we care about:

    // * If the timestamps fall into same time interval then row keys are same
    Assert.assertArrayEquals(
                              SimpleTimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                             SimpleTimeseriesTable.createRow(key, ts1 + timeIntervalPerRow / 2, timeIntervalPerRow));
    // * If the timestamps don't fall into same time interval then row keys are different
    Assert.assertFalse(Arrays.equals(
                                      SimpleTimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                             SimpleTimeseriesTable.createRow(key, ts1 + timeIntervalPerRow * 2, timeIntervalPerRow)));

    // * If timestamp A > timestamp B then row key A > row key B (we will have some optimization logic based on that)
    Assert.assertTrue(Bytes.compareTo(
                                       SimpleTimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                                    SimpleTimeseriesTable.createRow(key, ts1 + timeIntervalPerRow * 5,
                                                                    timeIntervalPerRow)) < 0);

    // * For different keys the constructed rows are different
    byte[] key2 = Bytes.toBytes("KEY2");
    Assert.assertFalse(Arrays.equals(
                                      SimpleTimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                                     SimpleTimeseriesTable.createRow(key2, ts1, timeIntervalPerRow)));

    // * We can get all possible rows for a given time interval with getRowsForInterval
    List<byte[]> rows = new ArrayList<byte[]>();
    long startTime = timeIntervalPerRow * 4;
    long endTime = timeIntervalPerRow * 100;
    for (long i = startTime; i <= endTime; i += timeIntervalPerRow / 10) {
      byte[] row = SimpleTimeseriesTable.createRow(key, i, timeIntervalPerRow);
      if (rows.size() == 0 || !Arrays.equals(rows.get(rows.size() - 1), row)) {
        rows.add(row);
      }
    }

    int intervalsCount = (int) SimpleTimeseriesTable.getTimeIntervalsCount(startTime, endTime, timeIntervalPerRow);
    byte[][] rowsForInterval = new byte[intervalsCount][];
    for (int i = 0; i < intervalsCount; i++) {
      rowsForInterval[i] = SimpleTimeseriesTable.getRowOfKthInterval(key, startTime, i, timeIntervalPerRow);
    }
    assertEquals(rows.toArray(new byte[rows.size()][]), rowsForInterval);

    // same requirement, one more test (covers some bug I fixed)
    Assert.assertEquals(2,
                        SimpleTimeseriesTable.getTimeIntervalsCount(
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
