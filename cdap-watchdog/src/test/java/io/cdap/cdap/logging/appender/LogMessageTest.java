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

package io.cdap.cdap.logging.appender;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import io.cdap.cdap.common.logging.NamespaceLoggingContext;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link LogMessage}.
 */
public class LogMessageTest {

  @Test
  public void testToStringFormatAndDelegation() {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(Level.WARN);
    event.setLoggerName("io.cdap.TestLogger");
    event.setMessage("hello {}");
    event.setArgumentArray(new Object[]{42});
    event.setThreadName("worker-thread");
    event.setTimeStamp(123456789L);
    event.setMDCPropertyMap(Collections.singletonMap("userKey", "userVal"));

    WorkerLoggingContext context =
        new WorkerLoggingContext("ns1", "app1", "worker1", "run1", "inst1");
    LogMessage logMessage = new LogMessage(event, context);

    String expected = "LogMessage{loggingEvent=" + event + ", loggingContext=" + context + "}";
    Assert.assertEquals(expected, logMessage.toString());
    Assert.assertTrue(logMessage.toString().startsWith("LogMessage{loggingEvent="));

    Assert.assertSame(context, logMessage.getLoggingContext());
    Assert.assertEquals("ns1", logMessage.getMDCPropertyMap().get(
        NamespaceLoggingContext.TAG_NAMESPACE_ID));
    Assert.assertEquals("userVal", logMessage.getMDCPropertyMap().get("userKey"));

    logMessage.prepareForDeferredProcessing();
    Assert.assertArrayEquals(new Object[]{"42"}, logMessage.getArgumentArray());
  }
}
