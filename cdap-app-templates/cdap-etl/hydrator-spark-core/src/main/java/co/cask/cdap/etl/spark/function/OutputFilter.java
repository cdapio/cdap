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

package co.cask.cdap.etl.spark.function;

import co.cask.cdap.etl.api.ErrorRecord;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Collections;

/**
 * Filters a SparkCollection containing both output and errors to one that just contains errors.
 *
 * @param <T> type of error record
 */
public class OutputFilter<T> implements FlatMapFunction<Tuple2<Boolean, Object>, ErrorRecord<T>> {

  @Override
  public Iterable<ErrorRecord<T>> call(Tuple2<Boolean, Object> input) throws Exception {
    //noinspection unchecked
    return input._1() ?
      Collections.singletonList((ErrorRecord<T>) input._2()) : Collections.<ErrorRecord<T>>emptyList();
  }
}
