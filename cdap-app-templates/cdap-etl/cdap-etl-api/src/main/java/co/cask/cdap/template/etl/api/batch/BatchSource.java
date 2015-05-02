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

package co.cask.cdap.template.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.Transformation;

/**
 * Batch Source forms the first stage of a Batch ETL Pipeline. Along with configuring the Batch run, it
 * also transforms the key value pairs provided by the Batch run into a single output type to be consumed by
 * subsequent transforms. By default, the value of the key value pair will be emitted.
 *
 * {@link BatchSource#initialize}, {@link BatchSource#transform} and {@link BatchSource#destroy} methods are called
 * inside the Batch Run while {@link BatchSource#prepareRun} and {@link BatchSource#onRunFinish} methods are called
 * on the client side, which launches the Batch run, before the Batch run starts and after it finishes respectively.
 *
 * @param <KEY_IN> the type of input key from the Batch run
 * @param <VAL_IN> the type of input value from the Batch run
 * @param <OUT> the type of output for the source
 */
@Beta
public abstract class BatchSource<KEY_IN, VAL_IN, OUT> extends BatchEndPointStage<BatchSourceContext>
  implements Transformation<KeyValue<KEY_IN, VAL_IN>, OUT, BatchSourceContext> {

  @Override
  public void initialize(BatchSourceContext context) throws Exception {
    // no-op
  }

  /**
   * Transform the {@link KeyValue} pair produced by the input, as set in {@link BatchSource#prepareRun},
   * to a single object and emit it to the next stage. By default it emits the value.
   *
   * @param input the input to transform
   * @param emitter {@link Emitter} to emit data to the next stage
   * @throws Exception if there's an error during this method invocation
   */
  @Override
  public void transform(KeyValue<KEY_IN, VAL_IN> input, Emitter<OUT> emitter) throws Exception {
    emitter.emit((OUT) input.getValue());
  }

  @Override
  public void destroy() {
    // no-op
  }
}
