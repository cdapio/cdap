package com.continuuity.logging.filter;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Represents a generic filter to filter ILoggingEvent objects.
 */
public interface Filter {
  boolean match(ILoggingEvent event);

  static Filter EMPTY_FILTER = new EmptyFilter();

  /**
   * Empty filter.
   */
  class EmptyFilter implements Filter {
    @Override
    public boolean match(ILoggingEvent event) {
      return true;
    }
  }
}
