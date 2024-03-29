/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.StageMetrics;
import java.util.Map;

/**
 * Wrapper around another emitter that tracks how many records were emitted.
 *
 * @param <T> the type of object to emit
 */
public class TrackedEmitter<T> implements Emitter<T> {

  private final Emitter<T> delegate;
  private final StageMetrics stageMetrics;
  private final String emitMetricName;
  private final DataTracer dataTracer;
  private final StageStatisticsCollector collector;

  public TrackedEmitter(Emitter<T> delegate, StageMetrics stageMetrics, String emitMetricName,
      DataTracer dataTracer,
      StageStatisticsCollector collector) {
    this.delegate = delegate;
    this.stageMetrics = stageMetrics;
    this.emitMetricName = emitMetricName;
    this.dataTracer = dataTracer;
    this.collector = collector;
  }

  @Override
  public void emit(T value) {
    stageMetrics.count(emitMetricName, 1);
    if (emitMetricName.equals(Constants.Metrics.RECORDS_OUT)) {
      collector.incrementOutputRecordCount();
    }
    if (dataTracer.isEnabled()) {
      dataTracer.info(emitMetricName, value);
    }
    delegate.emit(value);
  }

  @Override
  public void emitError(InvalidEntry<T> value) {
    stageMetrics.count(Constants.Metrics.RECORDS_ERROR, 1);
    collector.incrementErrorRecordCount();
    if (dataTracer.isEnabled()) {
      dataTracer.info(Constants.Metrics.RECORDS_ERROR, value);
    }
    delegate.emitError(value);
  }

  @Override
  public void emitAlert(Map<String, String> payload) {
    stageMetrics.count(Constants.Metrics.RECORDS_ALERT, 1);
    if (dataTracer.isEnabled()) {
      dataTracer.info(Constants.Metrics.RECORDS_ALERT, payload);
    }
    delegate.emitAlert(payload);
  }
}
