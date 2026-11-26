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

package io.cdap.cdap.etl.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cdap.cdap.etl.common.Constants;
import java.time.Instant;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.event.KeyValuePair;

/**
 * Tests for {@link LogStageAppender} SLF4J 2.x / Logback 1.6 delegation.
 */
public class LogStageAppenderTest {

  @Test
  public void testStageEventDelegatesSlf4j2Fields() {
    ListAppender<ILoggingEvent> delegate = new ListAppender<>();
    delegate.start();
    LogStageAppender appender = new LogStageAppender(Collections.singletonList(delegate));
    appender.start();

    Marker marker = MarkerFactory.getMarker("STAGE");
    KeyValuePair kvp = new KeyValuePair("stageKey", "stageVal");
    LoggingEvent event = new LoggingEvent();
    event.setLevel(Level.INFO);
    event.setLoggerName("test.stage");
    event.setMessage("processing");
    event.setInstant(Instant.ofEpochSecond(10L, 123_456_789L));
    event.setSequenceNumber(7L);
    event.addMarker(marker);
    event.addKeyValuePair(kvp);
    event.setMDCPropertyMap(Collections.singletonMap(Constants.MDC_STAGE_KEY, "Transform"));

    appender.doAppend(event);
    appender.stop();

    Assert.assertEquals(1, delegate.list.size());
    ILoggingEvent captured = delegate.list.get(0);
    Assert.assertEquals("Transform - processing", captured.getMessage());
    Assert.assertEquals("Transform - processing", captured.getFormattedMessage());
    Assert.assertEquals(123_456_789, captured.getNanoseconds());
    Assert.assertEquals(7L, captured.getSequenceNumber());
    Assert.assertEquals(Collections.singletonList(marker), captured.getMarkerList());
    Assert.assertEquals(Collections.singletonList(kvp), captured.getKeyValuePairs());
  }
}
