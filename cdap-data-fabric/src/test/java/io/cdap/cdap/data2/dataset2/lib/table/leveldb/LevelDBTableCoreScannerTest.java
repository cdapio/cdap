/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table.leveldb;

import io.cdap.cdap.api.dataset.table.Scanner;
import java.io.IOException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LevelDBTableCore} scanner exception propagation.
 */
public class LevelDBTableCoreScannerTest {

  @Test
  public void testScannerNextPropagatesUncheckedExceptionDirectlyAndWrapsCheckedException()
      throws Exception {
    DB mockDb = Mockito.mock(DB.class);
    DBIterator mockIterator = Mockito.mock(DBIterator.class);
    Mockito.when(mockDb.iterator()).thenReturn(mockIterator);

    LevelDBTableService mockService = Mockito.mock(LevelDBTableService.class);
    Mockito.when(mockService.getTable("tbl")).thenReturn(mockDb);

    final DBException expectedUnchecked = new DBException("corrupt block");
    final IOException expectedChecked = new IOException("sneaky iterator io error");

    Mockito.when(mockIterator.hasNext())
        .thenThrow(expectedUnchecked)
        .thenAnswer(inv -> {
          throw expectedChecked;
        });

    LevelDBTableCore tableCore = new LevelDBTableCore("tbl", mockService);
    try (Scanner scanner = tableCore.scan(null, null, null, null, null)) {
      try {
        scanner.next();
        Assert.fail("Expected DBException to propagate directly without double-wrapping");
      } catch (RuntimeException actual) {
        Assert.assertSame(expectedUnchecked, actual);
      }

      try {
        scanner.next();
        Assert.fail("Expected checked IOException wrapped in RuntimeException");
      } catch (RuntimeException actual) {
        Assert.assertSame(expectedChecked, actual.getCause());
      }
    }
    Mockito.verify(mockIterator).close();
  }
}
