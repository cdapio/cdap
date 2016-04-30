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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;

/**
 * Wrapper around writing to a mapreduce context.
 * This abstraction is required because if there is just one output for a mapreduce,
 * we must do context.write(key, value) instead of context.write(name, key, value).
 * See CDAP-3628 for more detail
 *
 * @param <KEY_OUT> the output key type
 * @param <VAL_OUT> the output value type
 */
abstract class OutputWriter<KEY_OUT, VAL_OUT> {
  protected final MapReduceTaskContext<KEY_OUT, VAL_OUT> context;

  public OutputWriter(MapReduceTaskContext<KEY_OUT, VAL_OUT> context) {
    this.context = context;
  }

  protected abstract void write(String sinkName, KeyValue<KEY_OUT, VAL_OUT> output) throws Exception;
}
