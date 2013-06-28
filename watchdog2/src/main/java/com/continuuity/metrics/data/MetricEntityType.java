/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

/**
*
*/
public enum MetricEntityType {
  CONTEXT("c"),
  METRIC("m"),
  TAG("t");

  private final String type;

  private MetricEntityType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }
}
