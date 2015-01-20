/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.metrics.data;

import co.cask.cdap.metrics.process.KafkaConsumerMetaTable;

/**
 * Factory to encapsulate creation of {@link TimeSeriesTable}.
 */
public interface MetricsTableFactory {

  /**
   * Creates a new instance of {@link TimeSeriesTable} with the given resolution.
   *
   * @param resolution The resolution that the table represents.
   * @return A new instance of {@link TimeSeriesTable}.
   */
  TimeSeriesTable createTimeSeries(int resolution);

  /**
   * Creates a new instance of {@link AggregatesTable}.
   * @return A new instance of {@link AggregatesTable}.
   */
  AggregatesTable createAggregates();

  /**
   * Creates a new instance of {@link KafkaConsumerMetaTable}.
   * @return A new instance of {@link KafkaConsumerMetaTable}.
   */
  KafkaConsumerMetaTable createKafkaConsumerMeta();

  /**
   * Returns whether the underlying table supports TTL.
   */
  boolean isTTLSupported();

  /**
   * Performs upgrade of metrics tables.
   */
  void upgrade() throws Exception;
}
