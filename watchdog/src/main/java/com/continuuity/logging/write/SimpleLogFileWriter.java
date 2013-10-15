package com.continuuity.logging.write;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * LogFileWriter to write avro log files.
 */
public class SimpleLogFileWriter implements LogFileWriter<LogWriteEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleLogFileWriter.class);

  private final AvroFileWriter avroFileWriter;
  private final long flushIntervalMs;

  private long lastCheckpointTime = System.currentTimeMillis();

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
    flush();
    avroFileWriter.close();
  }

  @Override
  public void flush() throws IOException {
    try {
      flush(true);
    } catch (Exception e) {
      LOG.error("Got exception: ", e);
      throw new IOException(e);
    }
  }

  private void flush(boolean force) throws Exception {
    long currentTs = System.currentTimeMillis();
    if (!force && currentTs - lastCheckpointTime < flushIntervalMs) {
      return;
    }

    avroFileWriter.flush();
    lastCheckpointTime = currentTs;
  }
}
