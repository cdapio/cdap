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

package co.cask.cdap.logging.serialize;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import co.cask.cdap.api.log.ThrowableProxy;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.logback.TestLoggingContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Test cases for LoggingEvent.
 */
public class LoggingEventTest {

  @BeforeClass
  public static void setUpContext() {
    LoggingContextAccessor.setLoggingContext(new TestLoggingContext("TEST_ACCT_ID1", "TEST_APP_ID1", "RUN1",
                                                                    "INSTANCE1"));
  }

  @Test
  public void testSerialize() throws Exception {
    Logger logger = LoggerFactory.getLogger(LoggingEventTest.class);
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent(
      getClass().getName(), (ch.qos.logback.classic.Logger) logger, Level.ERROR, "Log message1", null,
      new Object[] {"arg1", "arg2", "100"});
    iLoggingEvent.setLoggerName("loggerName1");
    iLoggingEvent.setThreadName("threadName1");
    iLoggingEvent.setTimeStamp(1234567890L);
    iLoggingEvent.setLoggerContextRemoteView(new LoggerContextVO("loggerContextRemoteView",
                                                                 ImmutableMap.of("key1", "value1", "key2", "value2"),
                                                                 100000L));
    Map<String, String> mdcMap = Maps.newHashMap(ImmutableMap.of("mdck1", "mdcv1", "mdck2", "mdck2"));
    iLoggingEvent.setMDCPropertyMap(mdcMap);

    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/logging/schema/LoggingEvent.avsc"));
    GenericRecord datum = LoggingEvent.encode(schema, iLoggingEvent,
                                              LoggingContextAccessor.getLoggingContext());

    LoggingEvent actualEvent = new LoggingEvent(LoggingEvent.decode(datum));
    LoggingEventSerializerTest.assertLoggingEventEquals(iLoggingEvent, actualEvent);
    testLoggingEventFromILoggingEvent(iLoggingEvent);
  }

  private void testLoggingEventFromILoggingEvent(ILoggingEvent iLoggingEvent) {
    co.cask.cdap.api.log.LoggingEvent loggingEvent = LoggingEventImpl.getLoggingEvent(iLoggingEvent);
    Assert.assertEquals(iLoggingEvent.getMessage(), loggingEvent.getMessage());
    Assert.assertEquals(iLoggingEvent.getFormattedMessage(), loggingEvent.getFormattedMessage());
    Assert.assertEquals(iLoggingEvent.getLoggerName(), loggingEvent.getLoggerName());
    Assert.assertEquals(iLoggingEvent.getThreadName(), loggingEvent.getThreadName());
    Assert.assertArrayEquals(iLoggingEvent.getArgumentArray(), loggingEvent.getArgumentArray());
    Assert.assertArrayEquals(iLoggingEvent.getCallerData(), loggingEvent.getCallerData());
    Assert.assertEquals(iLoggingEvent.getLevel().toString(), loggingEvent.getLevel().toString());

    if (iLoggingEvent.getLoggerContextVO() != null) {
      Assert.assertNotNull(loggingEvent.getLoggerContextVO());
      testLoggerContextVO(iLoggingEvent.getLoggerContextVO(), loggingEvent.getLoggerContextVO());
    } else {
      Assert.assertNull(loggingEvent.getLoggerContextVO());
    }

    Assert.assertEquals(iLoggingEvent.getMarker(), loggingEvent.getMarker());
    Assert.assertEquals(iLoggingEvent.getMDCPropertyMap(), loggingEvent.getMDCPropertyMap());
    Assert.assertEquals(iLoggingEvent.getTimeStamp(), loggingEvent.getTimeStamp());
    testThrowableProxyOnNotNull(iLoggingEvent.getThrowableProxy(), loggingEvent.getThrowableProxy());
  }

  private void testThrowableProxyOnNotNull(IThrowableProxy iThrowableProxy, ThrowableProxy throwableProxy) {
    if (iThrowableProxy != null) {
      Assert.assertNotNull(throwableProxy);
      testThrowableProxy(iThrowableProxy, throwableProxy);
    } else {
      Assert.assertNull(throwableProxy);
    }
  }
  private void testThrowableProxy(IThrowableProxy iThrowableProxy, ThrowableProxy throwableProxy) {
    testThrowableProxyOnNotNull(iThrowableProxy.getCause(), throwableProxy.getCause());
    Assert.assertEquals(iThrowableProxy.getMessage(), throwableProxy.getMessage());
    Assert.assertEquals(iThrowableProxy.getClassName(), throwableProxy.getClassName());
    Assert.assertEquals(iThrowableProxy.getCommonFrames(), throwableProxy.getCommonFrames());
    int length = iThrowableProxy.getSuppressed().length;
    Assert.assertEquals(length, throwableProxy.getSuppressed().length);
    for (int i = 0; i < length; i++) {
      testThrowableProxyOnNotNull(iThrowableProxy.getSuppressed()[i], throwableProxy.getSuppressed()[i]);
    }
    length = iThrowableProxy.getStackTraceElementProxyArray().length;
    Assert.assertEquals(length, throwableProxy.getStackTraceElementProxyArray().length);
    for (int i = 0; i < length; i++) {
      testStackTraceElementProxy(iThrowableProxy.getStackTraceElementProxyArray()[i],
                                 throwableProxy.getStackTraceElementProxyArray()[i]);
    }
  }

  private void testStackTraceElementProxy(
    StackTraceElementProxy iStackTraceElementProxy,
    co.cask.cdap.api.log.StackTraceElementProxy stackTraceElementProxy) {
    if (stackTraceElementProxy != null) {
      Assert.assertNotNull(stackTraceElementProxy);
      Assert.assertEquals(iStackTraceElementProxy.getStackTraceElement(),
                          stackTraceElementProxy.getStackTraceElement());
      Assert.assertEquals(iStackTraceElementProxy.getClassPackagingData().getCodeLocation(),
                          stackTraceElementProxy.getClassPackagingData().getCodeLocation());
      Assert.assertEquals(iStackTraceElementProxy.getClassPackagingData().getVersion(),
                          stackTraceElementProxy.getClassPackagingData().getVersion());
      Assert.assertEquals(iStackTraceElementProxy.getClassPackagingData().isExact(),
                          stackTraceElementProxy.getClassPackagingData().isExact());
    } else {
      Assert.assertNull(stackTraceElementProxy);
    }
  }

  private void testLoggerContextVO(LoggerContextVO iLoggingEvent, co.cask.cdap.api.log.LoggerContextVO loggingEvent) {
    Assert.assertEquals(iLoggingEvent.getBirthTime(), loggingEvent.getBirthTime());
    Assert.assertEquals(iLoggingEvent.getName(), loggingEvent.getName());
    Assert.assertEquals(iLoggingEvent.getPropertyMap(), loggingEvent.getPropertyMap());
  }

  @Test
  public void testEmptySerialize() throws Exception {
    Logger logger = LoggerFactory.getLogger(LoggingEventTest.class);
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent(
      getClass().getName(), (ch.qos.logback.classic.Logger) logger, Level.ERROR, null, null, null);

    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/logging/schema/LoggingEvent.avsc"));
    GenericRecord datum = LoggingEvent.encode(schema, iLoggingEvent, LoggingContextAccessor.getLoggingContext());

    LoggingEvent actualEvent = new LoggingEvent(LoggingEvent.decode(datum));
    LoggingEventSerializerTest.assertLoggingEventEquals(iLoggingEvent, actualEvent);
  }
}
