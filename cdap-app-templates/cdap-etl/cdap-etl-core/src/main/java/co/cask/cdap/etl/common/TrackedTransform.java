/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.Debugger;
import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transformation;

import javax.annotation.Nullable;

/**
 * A {@link Transformation} that delegates transform operations while emitting metrics
 * around how many records were input into the transform and output by it.
 *
 * @param <IN> Type of input object
 * @param <OUT> Type of output object
 */
public class TrackedTransform<IN, OUT> implements Transformation<IN, OUT>, Destroyable {
  public static final String RECORDS_IN = "records.in";
  public static final String RECORDS_OUT = "records.out";
  private final Transformation<IN, OUT> transform;
  private final StageMetrics metrics;
  private final String metricInName;
  private final String metricOutName;
  private final String stageName;
  private final Debugger debugger;

  public TrackedTransform(Transformation<IN, OUT> transform, StageMetrics metrics, String stageName,
                          Debugger debugger) {
    this(transform, metrics, RECORDS_IN, RECORDS_OUT, stageName, debugger);
  }

  public TrackedTransform(Transformation<IN, OUT> transform, StageMetrics metrics,
                          @Nullable String metricInName, @Nullable String metricOutName, String stageName,
                          Debugger debugger) {
    this.transform = transform;
    this.metrics = metrics;
    this.metricInName = metricInName;
    this.metricOutName = metricOutName;
    this.stageName = stageName;
    this.debugger = debugger;
  }

  @Override
  public void transform(IN input, Emitter<OUT> emitter) throws Exception {
    if (metricInName != null) {
      metrics.count(metricInName, 1);
    }
    if (debugger.isPreviewEnabled()) {
      debugger.getPreviewLogger(stageName).log("input.records", input);
    }
    transform.transform(input, metricOutName == null ? emitter :
      new TrackedEmitter<>(emitter, metrics, metricOutName, stageName, debugger));
  }

  @Override
  public void destroy() {
    if (transform instanceof Destroyable) {
      ((Destroyable) transform).destroy();
    }
  }
}
