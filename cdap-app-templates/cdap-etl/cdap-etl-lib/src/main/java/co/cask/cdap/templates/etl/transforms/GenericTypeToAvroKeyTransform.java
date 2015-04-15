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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Transform;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import javax.annotation.Nullable;

/**
 * Transform to AvroKey<T>
 * @param <T> type to convert to AvroKey
 */
public class GenericTypeToAvroKeyTransform<T> extends Transform<LongWritable, T, AvroKey<T>, NullWritable> {
  @Override
  public void transform(@Nullable LongWritable inputKey, T value,
                        Emitter<AvroKey<T>, NullWritable> emitter) throws Exception {


    emitter.emit(new AvroKey<T>(value), NullWritable.get());
  }
}
