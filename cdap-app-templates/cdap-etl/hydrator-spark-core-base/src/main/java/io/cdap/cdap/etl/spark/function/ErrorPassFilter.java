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

package io.cdap.cdap.etl.spark.function;

import com.google.common.collect.Iterators;
import io.cdap.cdap.etl.api.ErrorRecord;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.RecordType;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;

/**
 * Filters a SparkCollection containing both output and errors to one that just contains errors.
 *
 * @param <T> type of error record
 */
public class ErrorPassFilter<T> implements FlatMapFunction<RecordInfo<Object>, ErrorRecord<T>> {

  @Override
  public Iterator<ErrorRecord<T>> call(RecordInfo<Object> input) throws Exception {
    //noinspection unchecked
    return input.getType() == RecordType.ERROR ?
      Iterators.singletonIterator((ErrorRecord<T>) input.getValue()) : Iterators.emptyIterator();
  }
}
