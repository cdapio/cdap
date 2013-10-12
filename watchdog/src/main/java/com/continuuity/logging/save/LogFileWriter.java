package com.continuuity.logging.save;

import com.continuuity.logging.kafka.KafkaLogEvent;

import java.io.Closeable;
import java.io.Flushable;
import java.util.List;

/**
 * Interface to write log files.
 */
public interface LogFileWriter extends Closeable, Flushable {

  /**
   * Appends a log event to an appropriate Avro file based on LoggingContext. If the log event does not contain
   * LoggingContext then the event will be dropped.
   * @param events Log event
   * @throws java.io.IOException
   */
  void append(List<KafkaLogEvent> events) throws Exception;
}
