package com.continuuity.data2.dataset2;

import com.continuuity.api.dataset.table.Row;
import org.junit.Assert;

import java.util.Map;

/**
 * Data set table test.
 */
public class TableTest {

  public static void verifyColumns(Row result, byte[][] columns, byte[][] expected) {
    Assert.assertEquals(columns.length, expected.length);
    Assert.assertFalse(result.isEmpty());
    Map<byte[], byte[]> colsMap = result.getColumns();
    Assert.assertNotNull(colsMap);

    verify(columns, expected, colsMap);
  }

  private static void verify(byte[][] expectedCols, byte[][] expectedVals, Map<byte[], byte[]> toVerify) {
    Assert.assertEquals(expectedCols.length, toVerify.size());
    for (int i = 0; i < expectedCols.length; i++) {
      Assert.assertArrayEquals(expectedVals[i], toVerify.get(expectedCols[i]));
    }
  }

  public static void verifyColumn(Row result, byte[] column, byte[] expected) {
    verifyColumns(result, new byte[][]{column}, new byte[][]{expected});
  }

  private static void verify(Map<byte[], byte[]> expected,
                             Map<byte[], byte[]> actual) {
    Assert.assertEquals(actual.size(), expected.size());
    for (Map.Entry<byte[], byte[]> expectedEntry : expected.entrySet()) {
      Assert.assertArrayEquals(expectedEntry.getValue(), actual.get(expectedEntry.getKey()));
    }
  }
}
