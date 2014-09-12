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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.table.Row;
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
