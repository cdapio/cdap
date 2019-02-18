/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.pipeline.logbuffer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.context.WorkerLoggingContext;
import co.cask.cdap.logging.logbuffer.LogBufferEvent;
import co.cask.cdap.logging.logbuffer.LogBufferReader;
import co.cask.cdap.logging.logbuffer.LogBufferWriter;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests for {@link LogBufferReader}.
 */
public class LogBufferReaderTest {
  private final LoggingEventSerializer serializer = new LoggingEventSerializer();

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testLogReader() throws Exception {
    String absolutePath = TMP_FOLDER.newFolder().getAbsolutePath();

    LogBufferWriter writer = new LogBufferWriter(absolutePath, 250);
    ImmutableList<byte[]> events = getLoggingEvents();
    Iterable<LogBufferEvent> writtenEvents = writer.write(events.iterator());
    writer.close();

    List<LogBufferEvent> logBufferEvents = new LinkedList<>();
    // read from start positions, tests case where no checkpoints are persisted
    LogBufferReader reader = new LogBufferReader(absolutePath, 2, -1, -1);
    Iterator<LogBufferEvent> iterator = writtenEvents.iterator();
    verifyEvents(logBufferEvents, reader, iterator);
    reader.close();

    // this should skip first and second event, this is because log buffer offsets are offset for event that is
    // already stored. so in this case, skip first event and skip second event as second event is the last stored event
    reader = new LogBufferReader(absolutePath, 2, 0, 145);
    iterator = writtenEvents.iterator();
    iterator.next();
    iterator.next();
    verifyEvents(logBufferEvents, reader, iterator);
    reader.close();
  }

  private void verifyEvents(List<LogBufferEvent> logBufferEvents, LogBufferReader reader,
                            Iterator<LogBufferEvent> iterator) throws IOException {
    while (reader.readEvents(logBufferEvents) > 0) {
      for (LogBufferEvent event : logBufferEvents) {
        LogBufferEvent next = iterator.next();
        Assert.assertEquals(next.getOffset(), event.getOffset());
        Assert.assertEquals(next.getEventSize(), event.getEventSize());
        Assert.assertEquals(next.getLogEvent().getMessage(), event.getLogEvent().getMessage());
      }
      logBufferEvents.clear();
    }
  }

  private ImmutableList<byte[]> getLoggingEvents() {
    WorkerLoggingContext loggingContext =
      new WorkerLoggingContext("default", "app1", "worker1", "run1", "instance1");
    long now = System.currentTimeMillis();
    return ImmutableList.of(
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "0", now - 1000, loggingContext)),
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "1", now - 900, loggingContext)),
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "2", now - 700, loggingContext)),
      serializer.toBytes(createLoggingEvent("test.logger", Level.DEBUG, "3", now - 600, loggingContext)),
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "4", now - 500, loggingContext)),
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "5", now - 100, loggingContext)));
  }

  private ILoggingEvent createLoggingEvent(String loggerName, Level level, String message, long timestamp,
                                           LoggingContext loggingContext) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(level);
    event.setLoggerName(loggerName);
    event.setMessage(message);
    event.setTimeStamp(timestamp);
    return new LogMessage(event, loggingContext);
  }
}
