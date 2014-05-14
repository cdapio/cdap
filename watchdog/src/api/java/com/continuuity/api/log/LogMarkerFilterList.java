/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.log;

/**
 * Implementation of {@link LogMarkerFilter} that represents an ordered List of LogMarkerFilter
 * which will be evaluated with a specified boolean operator {@link Operator#MUST_PASS_ALL}
 * (<code>!AND</code>) or {@link Operator#MUST_PASS_ONE} (<code>!OR</code>).
 * Since you can use Filter Lists as children of Filter Lists, you can create a
 * hierarchy of filters to be evaluated.
 * Defaults to {@link Operator#MUST_PASS_ALL}.
 */
public class LogMarkerFilterList implements LogMarkerFilter {
  /** Set operator. */
  public static enum Operator {
    /* !AND */
    MUST_PASS_ALL,
    /* !OR */
    MUST_PASS_ONE
  }

  /**
   * Constructor that takes a set of {@link LogMarkerFilter}s and an operator.
   *
   * @param operator Operator to process filter set with.
   * @param filters set of filters.
   */
  public LogMarkerFilterList(Operator operator, LogMarkerFilter... filters) {
    // TODO
  }

  public LogMarkerFilterList add(LogMarkerFilter filter) {
    // TODO
    return null;
  }
}
