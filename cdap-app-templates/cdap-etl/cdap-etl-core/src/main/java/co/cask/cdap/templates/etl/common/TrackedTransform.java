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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Transform;

/**
 * A {@link Transform} that delegates transform operations to another Transform while emitting metrics
 * around how many records were input into the transform.
 *
 * @param <IN> Type of input object
 * @param <OUT> Type of output object
 */
public class TrackedTransform<IN, OUT> implements Transform<IN, OUT> {
  private final Transform transform;
  private final Metrics metrics;

  public TrackedTransform(Transform transform, Metrics metrics) {
    this.transform = transform;
    this.metrics = metrics;
  }

  @Override
  public void transform(IN input, Emitter<OUT> emitter) throws Exception {
    metrics.count("records.in", 1);
    transform.transform(input, emitter);
  }
}
