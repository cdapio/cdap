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
import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.api.MultiOutputEmitter;
import co.cask.cdap.etl.api.MultiOutputTransformation;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transformation;


/**
 * A {@link Transformation} that delegates transform operations while emitting metrics
 * around how many records were input into the transform and output by it.
 *
 * @param <IN> Type of input object
 * @param <ERROR> Type of error object
 */
public class TrackedMultiOutputTransform<IN, ERROR> implements MultiOutputTransformation<IN, ERROR>, Destroyable {
  private final MultiOutputTransformation<IN, ERROR> transform;
  private final StageMetrics metrics;
  private final DataTracer dataTracer;
  private final StageStatisticsCollector collector;

  public TrackedMultiOutputTransform(MultiOutputTransformation<IN, ERROR> transform, StageMetrics metrics,
                                     DataTracer dataTracer) {
    this(transform, metrics, dataTracer, new NoopStageStatisticsCollector());
  }

  public TrackedMultiOutputTransform(MultiOutputTransformation<IN, ERROR> transform, StageMetrics metrics,
                                     DataTracer dataTracer, StageStatisticsCollector collector) {
    this.transform = transform;
    this.metrics = metrics;
    this.dataTracer = dataTracer;
    this.collector = collector;
  }

  @Override
  public void transform(IN input, MultiOutputEmitter<ERROR> emitter) throws Exception {
    metrics.count(Constants.Metrics.RECORDS_IN, 1);
    collector.incrementInputRecordCount();
    transform.transform(input, new TrackedMultiOutputEmitter<>(emitter, metrics, dataTracer, collector));
  }

  @Override
  public void destroy() {
    if (transform instanceof Destroyable) {
      ((Destroyable) transform).destroy();
    }
  }
}
