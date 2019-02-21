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

package co.cask.cdap.logging.logbuffer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.context.WorkerLoggingContext;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Tests for {@link LogBufferWriter}.
 */
public class LogBufferWriterTest {
  private final LoggingEventSerializer serializer = new LoggingEventSerializer();

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testLogBufferWriter() throws Exception {
    String absolutePath = TMP_FOLDER.newFolder().getAbsolutePath();

    LogBufferWriter writer = new LogBufferWriter(absolutePath, 100000);
    ImmutableList<byte[]> events = getLoggingEvents();
    Iterator<LogBufferEvent> writtenEvents = writer.write(events.iterator()).iterator();
    writer.close();

    int i = 0;
    int startPos = 0;
    // verify if correct offsets were set without file rotation
    while (writtenEvents.hasNext()) {
      LogBufferEvent bufferEvent = writtenEvents.next();
      Assert.assertEquals("" + i++, bufferEvent.getLogEvent().getMessage());
      startPos = startPos + Bytes.SIZEOF_INT + serializer.toBytes(bufferEvent.getLogEvent()).length;
      // There will not be any rotation.
      Assert.assertEquals(bufferEvent.getOffset().getFilePos(), startPos);
    }

    // verify if the events were serialized and written correctly
    try (DataInputStream dis = new DataInputStream(new FileInputStream(absolutePath + "/0.buf"))) {
      for (byte[] eventBytes : events) {
        ILoggingEvent event = serializer.fromBytes(ByteBuffer.wrap(eventBytes));
        Assert.assertEquals(event.getMessage(), getEvent(dis, serializer.toBytes(event).length).getMessage());
      }
    }
  }

  @Test
  public void testFileRotation() throws Exception {
    // Make sure rotation happens after every event is written
    LogBufferWriter writer = new LogBufferWriter(TMP_FOLDER.newFolder().getAbsolutePath(), 10);
    ImmutableList<byte[]> events = getLoggingEvents();
    Iterator<LogBufferEvent> writtenEvents = writer.write(events.iterator()).iterator();
    writer.close();

    int i = 0;
    // verify if correct offsets and file id were set with file rotation
    while (writtenEvents.hasNext()) {
      LogBufferEvent bufferEvent = writtenEvents.next();
      Assert.assertEquals(bufferEvent.getLogEvent().getMessage(), "" + i++);
      // There will be 2 events in one file.
      Assert.assertEquals(i + ".buf", bufferEvent.getOffset().getFileId() + ".buf");
      Assert.assertEquals(0, bufferEvent.getOffset().getFilePos());
    }
  }

  @Test (expected = IOException.class)
  public void testWritesOnClosedWriter() throws IOException {
    LogBufferWriter writer = new LogBufferWriter(TMP_FOLDER.newFolder().getAbsolutePath(), 100000);
    writer.close();
    // should throw IOException
    writer.write(ImmutableList.of(
      serializer.toBytes(createLoggingEvent("test.logger", Level.INFO, "0", 1,
                                            new WorkerLoggingContext("default", "app1", "worker1", "run1",
                                                                     "instance1")))).iterator());
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

  private ILoggingEvent getEvent(DataInputStream dis, int actualLen) throws IOException {
    Assert.assertEquals(actualLen, dis.readInt());
    byte[] eventBytes = new byte[actualLen];
    dis.read(eventBytes, 0, actualLen);
    return serializer.fromBytes(ByteBuffer.wrap(eventBytes));
  }
}
