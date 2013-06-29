/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.transport;

import com.google.common.base.Objects;

/**
 *
 */
public final class TagMetric {

  private final String tag;
  private final int value;

  public TagMetric(String tag, int value) {
    this.tag = tag;
    this.value = value;
  }

  public String getTag() {
    return tag;
  }

  public int getValue() {
    return value;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(TagMetric.class)
      .add("tag", tag)
      .add("value", value)
      .toString();
  }
}
