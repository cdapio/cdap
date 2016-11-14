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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.StageMetrics;

/**
 * Wrapper around another emitter that tracks how many records were emitted.
 *
 * @param <T> the type of object to emit
 */
public class TrackedEmitter<T> implements Emitter<T> {
  private static final String RECORDS_ERROR = "records.error";
  private final Emitter<T> delegate;
  private final StageMetrics stageMetrics;
  private final String emitMetricName;
  private final DataTracer dataTracer;

  public TrackedEmitter(Emitter<T> delegate, StageMetrics stageMetrics, String emitMetricName, DataTracer dataTracer) {
    this.delegate = delegate;
    this.stageMetrics = stageMetrics;
    this.emitMetricName = emitMetricName;
    this.dataTracer = dataTracer;
  }

  @Override
  public void emit(T value) {
    delegate.emit(value);
    stageMetrics.count(emitMetricName, 1);
    if (dataTracer.isEnabled()) {
      dataTracer.info(emitMetricName, value);
    }
  }

  @Override
  public void emitError(InvalidEntry<T> value) {
    delegate.emitError(value);
    stageMetrics.count(RECORDS_ERROR, 1);
    if (dataTracer.isEnabled()) {
      dataTracer.info(RECORDS_ERROR, value);
    }
  }
}
