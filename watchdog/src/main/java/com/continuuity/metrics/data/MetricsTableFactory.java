/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.metrics.process.KafkaConsumerMetaTable;

/**
 * Factory to encapsulate creation of {@link TimeSeriesTable}.
 */
public interface MetricsTableFactory {

  /**
   * Creates a new instance of {@link TimeSeriesTable} with the given resolution.
   * @param namespace Name prefix of the table name
   * @param resolution The resolution that the table represents.
   * @return A new instance of {@link TimeSeriesTable}.
   */
  TimeSeriesTable createTimeSeries(String namespace, int resolution);

  /**
   * Creates a new instance of {@link AggregatesTable}.
   * @param namespace Name prefix of the table name
   * @return A new instance of {@link AggregatesTable}.
   */
  AggregatesTable createAggregates(String namespace);

  /**
   * Creates a new instance of {@link KafkaConsumerMetaTable}.
   * @param namespace Name prefix of the table name
   * @return A new instance of {@link KafkaConsumerMetaTable}.
   */
  KafkaConsumerMetaTable createKafkaConsumerMeta(String namespace);

  /**
   * Returns whether the underlying table supports TTL.
   */
  boolean isTTLSupported();

  /**
   * Performs upgrade of metrics tables.
   */
  void upgrade() throws Exception;
}
