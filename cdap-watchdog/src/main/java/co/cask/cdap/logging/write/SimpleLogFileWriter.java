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

package co.cask.cdap.logging.write;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * LogFileWriter to write avro log files.
 */
public class SimpleLogFileWriter implements LogFileWriter<LogWriteEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleLogFileWriter.class);

  private final AvroFileWriter avroFileWriter;
  private final long flushIntervalMs;

  private long lastCheckpointTime = System.currentTimeMillis();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public SimpleLogFileWriter(AvroFileWriter avroFileWriter, long flushIntervalMs) {
    this.avroFileWriter = avroFileWriter;
    this.flushIntervalMs = flushIntervalMs;
  }

  @Override
  public void append(List<LogWriteEvent> events) throws Exception {
    avroFileWriter.append(events);
    flush(false);
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    flush();
    avroFileWriter.close();
  }

  @Override
  public void flush() throws IOException {
      flush(true);
  }

  public void flush(boolean force) throws IOException {
    try {
      long currentTs = System.currentTimeMillis();
      if (!force && currentTs - lastCheckpointTime < flushIntervalMs) {
        return;
      }

      avroFileWriter.flush();
      lastCheckpointTime = currentTs;
    } catch (Exception e) {
      LOG.error("Got exception: ", e);
      throw new IOException(e);
    }
  }
}
