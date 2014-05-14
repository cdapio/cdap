package com.continuuity.common.logging;

import java.io.IOException;

/**
 * Represents classes that can write log.
 */
public interface LogWriter {

  /**
   * Configures this writer.
   * @param config specifies what log to write to
   */
  public void configure(LogConfiguration config) throws IOException;

  /**
   * logs a single event. This must ensure that - if desired - the event is
   * persisted.
   */
  public void log(LogEvent event) throws IOException;

  /**
   * return the current write position. This is used as a hint for the length
   * of the current log file - it has not been closed and hence the file system
   * status does not reflect its true size.
   */
  public long getWritePosition() throws IOException;

  /**
   * closes the writer and flushes and closes all open files.
   */
  public void close() throws IOException;
}
