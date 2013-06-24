package com.continuuity.logging.filter;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Represents an expression that can match a key,value in MDC.
 */
public class MdcExpression implements Filter {
  private final String key;
  private final String value;

  public MdcExpression(String key, String value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public boolean match(ILoggingEvent event) {
    String value = event.getMDCPropertyMap().get(getKey());
    return value != null && value.equals(getValue());
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }
}
