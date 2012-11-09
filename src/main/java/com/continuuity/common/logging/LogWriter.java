package com.continuuity.common.logging;

import java.io.IOException;

public interface LogWriter {

  /**
   * Configures this writer
   * @param config
   */
  public void configure(LogConfiguration config) throws IOException;

  /**
   * logs a single event. This must ensure that - if desired - the event is
   * persisted.
   */
  public void log(LogEvent event) throws IOException;

}
