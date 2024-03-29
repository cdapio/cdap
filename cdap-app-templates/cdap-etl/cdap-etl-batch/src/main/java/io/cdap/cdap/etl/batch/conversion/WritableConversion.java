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

package io.cdap.cdap.etl.batch.conversion;

import com.google.common.reflect.TypeToken;
import org.apache.hadoop.io.Writable;

/**
 * Functions to convert common classes to their Writable equivalent and vice versa.
 *
 * @param <T> type of object to convert to a Writable
 * @param <W> the Writable type to convert to
 */
public abstract class WritableConversion<T, W extends Writable> {

  private final Class<? super W> cls = new TypeToken<W>(getClass()) {
  }.getRawType();

  public Class<? super W> getWritableClass() {
    return cls;
  }

  public abstract W toWritable(T val);

  public abstract T fromWritable(W val);

}
