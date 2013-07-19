/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

/**
 * An enum representing different types of entities.
 */
public enum MetricsEntityType {
  CONTEXT("c"),
  RUN("r"),
  METRIC("m"),
  TAG("t");

  private final String type;

  private MetricsEntityType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }
}
