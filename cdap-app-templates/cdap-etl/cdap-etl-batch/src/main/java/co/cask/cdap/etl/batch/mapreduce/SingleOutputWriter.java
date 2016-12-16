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
 * Writes to a single output.
 *
 * @param <KEY_OUT> the output key type
 * @param <VAL_OUT> the output value type
 */
public class SingleOutputWriter<KEY_OUT, VAL_OUT> extends OutputWriter<KEY_OUT, VAL_OUT> {
  public SingleOutputWriter(MapReduceTaskContext<KEY_OUT, VAL_OUT> context) {
    super(context);
  }

  public void write(String sinkName, KeyValue<KEY_OUT, VAL_OUT> output) throws Exception {
    context.write(output.getKey(), output.getValue());
  }
}
