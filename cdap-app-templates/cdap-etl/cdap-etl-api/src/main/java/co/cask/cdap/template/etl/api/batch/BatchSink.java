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
import co.cask.cdap.template.etl.api.StageLifecycle;
import co.cask.cdap.template.etl.api.Transformation;

/**
 * Batch Sink forms the last stage of a Batch ETL Pipeline. In addition to configuring the Batch run, it
 * also transforms a single input object into a key value pair that the Batch run will output. By default, the input
 * object is used as both the key and value.
 *
 * {@link BatchSink#initialize}, {@link BatchSink#transform} and {@link BatchSink#destroy} methods are called inside
 * the Batch Run while {@link BatchSink#prepareRun} and {@link BatchSink#onRunFinish} methods are called on the
 * client side, which launches the Batch run, before the Batch run starts and after it finishes respectively.
 *
 * @param <IN> the type of input object to the sink
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
@Beta
public abstract class BatchSink<IN, KEY_OUT, VAL_OUT> extends BatchConfigurable<BatchSinkContext>
  implements Transformation<IN, KeyValue<KEY_OUT, VAL_OUT>>, StageLifecycle<BatchSinkContext> {

  /**
   * Initialize the Batch Sink stage. Executed inside the Batch Run. This method is guaranteed to be invoked
   * before any calls to {@link BatchSink#transform} are made.
   *
   * @param context {@link BatchSinkContext}
   * @throws Exception if there is any error during initialization
   */
  @Override
  public void initialize(BatchSinkContext context) throws Exception {
    // no-op
  }

  /**
   * Transform the input received from previous stage to a {@link KeyValue} pair which can be consumed by the output,
   * as set in {@link BatchSink#prepareRun}. By default, the input object is used as both key and value.
   * This method is invoked inside the Batch run.
   *
   * @param input the input to transform
   * @param emitter {@link Emitter} to emit data to the next stage
   * @throws Exception if there's an error during this method invocation
   */
  @Override
  public void transform(IN input, Emitter<KeyValue<KEY_OUT, VAL_OUT>> emitter) throws Exception {
    emitter.emit(new KeyValue<>((KEY_OUT) input, (VAL_OUT) input));
  }

  /**
   * Destroy the Batch Sink stage. Executed at the end of the Batch Run.
   */
  @Override
  public void destroy() {
    // no-op
  }
}
