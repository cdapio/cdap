/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.common.logging;

import ch.qos.logback.classic.spi.IThrowableProxy;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogThrowable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link TwillLogEntryAdapter} and {@link TwillLogThrowableAdapter} SLF4J 2.x / Logback 1.6 methods.
 */
public class TwillLogEntryAdapterTest {

  @Test
  public void testSlf4j2AndLogback16Methods() {
    LogThrowable throwable = new LogThrowable() {
      @Override
      public String getClassName() {
        return IllegalStateException.class.getName();
      }

      @Override
      public String getMessage() {
        return "boom";
      }

      @Override
      public StackTraceElement[] getStackTraces() {
        return new StackTraceElement[0];
      }

      @Override
      public LogThrowable getCause() {
        return null;
      }
    };

    TwillLogEntryAdapter positive = new TwillLogEntryAdapter(createEntry(1700000000456L, throwable));
    Assert.assertEquals(1700000000456L, positive.getTimeStamp());
    Assert.assertEquals(456_000_000, positive.getNanoseconds());
    Assert.assertEquals(0L, positive.getSequenceNumber());
    Assert.assertNull(positive.getMarkerList());
    Assert.assertNull(positive.getKeyValuePairs());

    IThrowableProxy proxy = positive.getThrowableProxy();
    Assert.assertNotNull(proxy);
    Assert.assertFalse(proxy.isCyclic());

    TwillLogEntryAdapter negative = new TwillLogEntryAdapter(createEntry(-1L, null));
    Assert.assertEquals(999_000_000, negative.getNanoseconds());
    Assert.assertNull(negative.getThrowableProxy());
  }

  private static LogEntry createEntry(long timestamp, LogThrowable throwable) {
    return new LogEntry() {
      @Override
      public String getLoggerName() {
        return "test.twill";
      }

      @Override
      public String getHost() {
        return "localhost";
      }

      @Override
      public String getRunnableName() {
        return "worker";
      }

      @Override
      public long getTimestamp() {
        return timestamp;
      }

      @Override
      public Level getLogLevel() {
        return Level.INFO;
      }

      @Override
      public String getSourceClassName() {
        return "TestClass";
      }

      @Override
      public String getSourceMethodName() {
        return "testMethod";
      }

      @Override
      public String getFileName() {
        return "TestClass.java";
      }

      @Override
      public int getLineNumber() {
        return 1;
      }

      @Override
      public String getMessage() {
        return "hello";
      }

      @Override
      public String getThreadName() {
        return "main";
      }

      @Override
      public LogThrowable getThrowable() {
        return throwable;
      }

      @Override
      public StackTraceElement[] getStackTraces() {
        return new StackTraceElement[0];
      }
    };
  }
}
