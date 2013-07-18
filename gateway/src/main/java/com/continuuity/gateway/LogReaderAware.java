package com.continuuity.gateway;


import com.continuuity.logging.read.LogReader;

/**
 * Defines an interface for classes that use @link{com.continuuity.logging.read.LogReader} interface.
 */
public interface LogReaderAware {
  /**
   * Set LogReader object.
   * @param logReader LogReader object.
   */
  public void setLogReader(LogReader logReader);
}
