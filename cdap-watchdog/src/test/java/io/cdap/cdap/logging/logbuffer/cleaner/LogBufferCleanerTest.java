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

package io.cdap.cdap.logging.logbuffer.cleaner;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.logbuffer.LogBufferFileOffset;
import io.cdap.cdap.logging.logbuffer.LogBufferWriter;
import io.cdap.cdap.logging.logbuffer.MockCheckpointManager;
import io.cdap.cdap.logging.meta.Checkpoint;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for {@link LogBufferCleaner}.
 */
public class LogBufferCleanerTest {
  private static final LoggingEventSerializer serializer = new LoggingEventSerializer();

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testLogBufferCleanerService() throws Exception {
    String absolutePath = TMP_FOLDER.newFolder().getAbsolutePath();

    MockCheckpointManager checkpointManager = new MockCheckpointManager();
    // update checkpoints
    checkpointManager.saveCheckpoints(ImmutableMap.of(0, new TestCheckpoint(2L, 0L, 1L)));

    // write directly to log buffer, keep file size 10 bytes so that more files are created
    LogBufferWriter writer = new LogBufferWriter(absolutePath, 10,
                                                 new LogBufferCleaner(ImmutableList.of(checkpointManager),
                                                                      absolutePath, new AtomicBoolean(true)));
    ImmutableList<byte[]> events = getLoggingEvents();
    List<byte[]> subset = new ArrayList<>();
    subset.add(events.get(0));
    subset.add(events.get(1));
    subset.add(events.get(2));
    writer.write(subset.iterator()).iterator();

    // should delete file 0 an1
    File file0 = new File(absolutePath, "0.buff");
    File file1 = new File(absolutePath, "1.buff");
    Tasks.waitFor(true, () -> !file0.exists() && !file1.exists(), 120, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // update checkpoints
    checkpointManager.saveCheckpoints(ImmutableMap.of(0, new TestCheckpoint(5L, 0L, 1L)));

    subset.add(events.get(3));
    subset.add(events.get(4));
    subset.add(events.get(5));
    writer.write(subset.iterator()).iterator();
    writer.close();

    // should delete file 2, 3 and 4
    File file2 = new File(absolutePath, "2.buff");
    File file3 = new File(absolutePath, "3.buff");
    File file4 = new File(absolutePath, "4.buff");
    Tasks.waitFor(true, () -> !file2.exists() && !file3.exists() && !file4.exists(), 120, TimeUnit.SECONDS,
                  100, TimeUnit.MILLISECONDS);

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

  private static final class TestCheckpoint extends Checkpoint<LogBufferFileOffset> {

    TestCheckpoint(long fileId, long fileOffset, long maxEventTime) {
      super(new LogBufferFileOffset(fileId, fileOffset), maxEventTime);
    }
  }
}
