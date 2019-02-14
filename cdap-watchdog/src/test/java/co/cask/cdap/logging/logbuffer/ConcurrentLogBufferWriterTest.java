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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.context.WorkerLoggingContext;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ConcurrentLogBufferWriter}.
 */
public class ConcurrentLogBufferWriterTest {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentLogBufferWriterTest.class);
  private static final LoggingEventSerializer serializer = new LoggingEventSerializer();

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testWrites() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    String absolutePath = TMP_FOLDER.newFolder().getAbsolutePath();
    cConf.set(Constants.LogBuffer.LOG_BUFFER_BASE_DIR, absolutePath);
    cConf.setLong(Constants.LogBuffer.LOG_BUFFER_MAX_FILE_SIZE_BYTES, 100000);

    ConcurrentLogBufferWriter writer = new ConcurrentLogBufferWriter(cConf);
    ImmutableList<byte[]> events = getLoggingEvents();
    writer.process(new LogBufferRequest(0, events));

    // verify if the events were written to log buffer
    try (DataInputStream dis = new DataInputStream(new FileInputStream(absolutePath + "/0.buf"))) {
      for (byte[] eventBytes : events) {
        ILoggingEvent event = serializer.fromBytes(ByteBuffer.wrap(eventBytes));
        Assert.assertEquals(event.getMessage(), getEvent(dis, serializer.toBytes(event).length).getMessage());
      }
    }
  }

  @Test
  public void testConcurrentWrites() throws Exception {
    int threadCount = 20;

    CConfiguration cConf = CConfiguration.create();
    String absolutePath = TMP_FOLDER.newFolder().getAbsolutePath();
    cConf.set(Constants.LogBuffer.LOG_BUFFER_BASE_DIR, absolutePath);
    cConf.setLong(Constants.LogBuffer.LOG_BUFFER_MAX_FILE_SIZE_BYTES, 100000);

    ConcurrentLogBufferWriter writer = new ConcurrentLogBufferWriter(cConf);
    ImmutableList<byte[]> events = getLoggingEvents();

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          barrier.await();
          writer.process(new LogBufferRequest(0, events));
        } catch (Exception e) {
          LOG.error("Exception raised when processing log events.", e);
        }
      });
    }

    barrier.await();
    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));

    // verify if the events were written to log buffer
    try (DataInputStream dis = new DataInputStream(new FileInputStream(absolutePath + "/0.buf"))) {
      for (int i = 0; i < threadCount; i++) {
        for (byte[] eventBytes : events) {
          ILoggingEvent event = serializer.fromBytes(ByteBuffer.wrap(eventBytes));
          Assert.assertEquals(event.getMessage(), getEvent(dis, serializer.toBytes(event).length).getMessage());
        }
      }
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

  private ILoggingEvent getEvent(DataInputStream dis, int actualLen) throws IOException {
    Assert.assertEquals(actualLen, dis.readInt());
    byte[] eventBytes = new byte[actualLen];
    dis.read(eventBytes, 0, actualLen);
    return serializer.fromBytes(ByteBuffer.wrap(eventBytes));
  }
}
