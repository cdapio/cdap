/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.read;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.logging.appender.LoggingTester;
import io.cdap.cdap.logging.appender.system.LogPathIdentifier;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.meta.FileMetaDataReader;
import java.io.IOException;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link FileLogReader} exception propagation and empty range handling.
 */
public class FileLogReaderTest {

  @Test
  public void testEmptyFileRangeReturnsCleanly() throws Exception {
    FileMetaDataReader metaReader = Mockito.mock(FileMetaDataReader.class);
    Mockito.when(metaReader.listFiles(
        Mockito.any(LogPathIdentifier.class), Mockito.anyLong(), Mockito.anyLong()))
        .thenReturn(Collections.emptyList());

    FileLogReader reader = new FileLogReader(metaReader);
    LoggingContext context = new WorkerLoggingContext("default", "app1", "w1", "run1", "0");

    LoggingTester.LogCallback nextCallback = new LoggingTester.LogCallback();
    reader.getLogNext(context, new ReadRange(0L, 1000L, -10000L), 10,
        Filter.EMPTY_FILTER, nextCallback);
    Assert.assertTrue(nextCallback.getEvents().isEmpty());

    LoggingTester.LogCallback prevCallback = new LoggingTester.LogCallback();
    reader.getLogPrev(context, ReadRange.LATEST, 10, Filter.EMPTY_FILTER, prevCallback);
    Assert.assertTrue(prevCallback.getEvents().isEmpty());

    try (CloseableIterator<LogEvent> iter = reader.getLog(context, 0L, 1000L, Filter.EMPTY_FILTER)) {
      Assert.assertFalse(iter.hasNext());
    }
  }

  @Test
  public void testCheckedIoExceptionWrappedInRuntimeExceptionAcrossAllReadMethods()
      throws Exception {
    final IOException checkedFailure = new IOException("metadata scan io failure");
    FileMetaDataReader metaReader = Mockito.mock(FileMetaDataReader.class);
    Mockito.when(metaReader.listFiles(
        Mockito.any(LogPathIdentifier.class), Mockito.anyLong(), Mockito.anyLong()))
        .thenThrow(checkedFailure);

    FileLogReader reader = new FileLogReader(metaReader);
    LoggingContext context = new WorkerLoggingContext("default", "app1", "w1", "run1", "0");

    try {
      reader.getLogNext(context, new ReadRange(0L, 1000L, -10000L), 10,
          Filter.EMPTY_FILTER, new LoggingTester.LogCallback());
      Assert.fail("Expected RuntimeException from getLogNext");
    } catch (RuntimeException actual) {
      Assert.assertEquals(RuntimeException.class, actual.getClass());
      Assert.assertSame(checkedFailure, actual.getCause());
    }

    try {
      reader.getLogPrev(context, new ReadRange(0L, 1000L, -10000L), 10,
          Filter.EMPTY_FILTER, new LoggingTester.LogCallback());
      Assert.fail("Expected RuntimeException from getLogPrev");
    } catch (RuntimeException actual) {
      Assert.assertEquals(RuntimeException.class, actual.getClass());
      Assert.assertSame(checkedFailure, actual.getCause());
    }

    try {
      reader.getLog(context, 0L, 1000L, Filter.EMPTY_FILTER);
      Assert.fail("Expected RuntimeException from getLog");
    } catch (RuntimeException actual) {
      Assert.assertEquals(RuntimeException.class, actual.getClass());
      Assert.assertSame(checkedFailure, actual.getCause());
    }
  }

  @Test
  public void testUncheckedRuntimeExceptionIsNotDoubleWrappedAcrossAllReadMethods()
      throws Exception {
    final IllegalStateException uncheckedFailure =
        new IllegalStateException("unexpected runtime failure");
    FileMetaDataReader metaReader = Mockito.mock(FileMetaDataReader.class);
    Mockito.when(metaReader.listFiles(
        Mockito.any(LogPathIdentifier.class), Mockito.anyLong(), Mockito.anyLong()))
        .thenThrow(uncheckedFailure);

    FileLogReader reader = new FileLogReader(metaReader);
    LoggingContext context = new WorkerLoggingContext("default", "app1", "w1", "run1", "0");

    try {
      reader.getLogNext(context, new ReadRange(0L, 1000L, -10000L), 10,
          Filter.EMPTY_FILTER, new LoggingTester.LogCallback());
      Assert.fail("Expected IllegalStateException from getLogNext");
    } catch (IllegalStateException actual) {
      Assert.assertSame(uncheckedFailure, actual);
    }

    try {
      reader.getLogPrev(context, new ReadRange(0L, 1000L, -10000L), 10,
          Filter.EMPTY_FILTER, new LoggingTester.LogCallback());
      Assert.fail("Expected IllegalStateException from getLogPrev");
    } catch (IllegalStateException actual) {
      Assert.assertSame(uncheckedFailure, actual);
    }

    try {
      reader.getLog(context, 0L, 1000L, Filter.EMPTY_FILTER);
      Assert.fail("Expected IllegalStateException from getLog");
    } catch (IllegalStateException actual) {
      Assert.assertSame(uncheckedFailure, actual);
    }
  }
}
