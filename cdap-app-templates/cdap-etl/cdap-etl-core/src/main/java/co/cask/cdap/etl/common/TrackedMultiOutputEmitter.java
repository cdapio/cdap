/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.MultiOutputEmitter;
import co.cask.cdap.etl.api.StageMetrics;

import java.util.Map;

/**
 * Wrapper around another MultiOutputEmitter that tracks how many records were emitted to each port.
 *
 * @param <E> the type of error object to emit
 */
public class TrackedMultiOutputEmitter<E> implements MultiOutputEmitter<E> {
  private final MultiOutputEmitter<E> delegate;
  private final StageMetrics stageMetrics;
  private final DataTracer dataTracer;

  public TrackedMultiOutputEmitter(MultiOutputEmitter<E> delegate, StageMetrics stageMetrics, DataTracer dataTracer) {
    this.delegate = delegate;
    this.stageMetrics = stageMetrics;
    this.dataTracer = dataTracer;
  }

  @Override
  public void emit(String port, Object value) {
    String metricName = Constants.Metrics.RECORDS_OUT + "." + port;
    stageMetrics.count(metricName, 1);
    if (dataTracer.isEnabled()) {
      dataTracer.info(metricName, value);
    }
    delegate.emit(port, value);
  }

  @Override
  public void emitError(InvalidEntry<E> value) {
    stageMetrics.count(Constants.Metrics.RECORDS_ERROR, 1);
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
