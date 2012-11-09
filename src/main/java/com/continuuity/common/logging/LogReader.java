package com.continuuity.common.logging;

import java.io.IOException;
import java.util.List;

public interface LogReader {

  /**
   * Configures this reader
   * @param config specifies what log to read
   */
  public void configure(LogConfiguration config) throws IOException;

  /**
   * retrieves the tail of the log, up to size bytes, line by line.
   * @param size limits the number of bytes to read
   */
  public List<String> tail(int size) throws IOException;
}
