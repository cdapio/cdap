/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;
/**
 * Raised when there is an issue in parsing a metrics REST API, indicating there is something in the path
 * that could not be found.
 */
public class MetricsPathException extends Exception {
  public MetricsPathException(String reason) {
    super(reason);
  }
}
