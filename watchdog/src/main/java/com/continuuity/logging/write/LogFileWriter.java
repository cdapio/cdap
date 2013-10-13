package com.continuuity.logging.write;

import java.io.Closeable;
import java.io.Flushable;
import java.util.List;

/**
 * Interface to write log files.
 *
 * @param <T> type of log event.
 */
public interface LogFileWriter<T extends LogWriteEvent> extends Closeable, Flushable {

  /**
   * Appends a log event to an appropriate Avro file based on LoggingContext. If the log event does not contain
   * LoggingContext then the event will be dropped.
   * @param events Log event
   * @throws java.io.IOException
   */
  void append(List<T> events) throws Exception;
}
