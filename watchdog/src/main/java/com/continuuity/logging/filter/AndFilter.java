package com.continuuity.logging.filter;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Represents an And Filter where all sub expressions are and-ed together.
 */
public class AndFilter implements Filter {
  private final List<? extends Filter> expressions;

  public AndFilter(List<? extends Filter> expressions) {
    this.expressions = ImmutableList.copyOf(expressions);
  }

  @Override
  public boolean match(ILoggingEvent event) {
    for (Filter expression : expressions) {
      if (!expression.match(event)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("expressions", expressions)
      .toString();
  }
}
