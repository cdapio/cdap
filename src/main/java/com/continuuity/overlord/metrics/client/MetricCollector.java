/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client;

import java.io.IOException;

/**
  * MetricCollector provides a way to specify different Sinks for client.  E.g. are
  * FileMetricCollector, FMMetricCollector, GangliaSink ...
  */
public interface MetricCollector {
  /**
    * Initializes the MetricCollector system
    */
  void init() throws IOException;

  /**
    * Pushes a record to the underlying metric system.
    * @param record
    */
  void push(Record record) throws IOException;

  /**
    * If implemented flushes the client to downstream system.
    */
  void flush() throws IOException;

  /**
    * Deinitializes the underlying metric collection system.
    */
  void destroy() throws IOException;
}
