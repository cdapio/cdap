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

import scala.Tuple2;

import java.util.Collections;

/**
 * Filters a SparkCollection containing both output and errors to one that just contains output.
 *
 * @param <T> type of output record
 */
public class ErrorFilter<T> implements FlatMapFunc<Tuple2<Boolean, Object>, T> {

  @Override
  public Iterable<T> call(Tuple2<Boolean, Object> input) throws Exception {
    //noinspection unchecked
    return input._1() ? Collections.<T>emptyList() : Collections.singletonList((T) input._2());
  }
}
