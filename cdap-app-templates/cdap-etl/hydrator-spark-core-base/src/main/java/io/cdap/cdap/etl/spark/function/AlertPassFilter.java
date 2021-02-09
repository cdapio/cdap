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
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.RecordType;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Collections;
import java.util.Iterator;

/**
 * Filters a SparkCollection containing both output and errors to one that just contains alerts.
 */
public class AlertPassFilter implements FlatMapFunction<RecordInfo<Object>, Alert> {

  @Override
  public Iterator<Alert> call(RecordInfo<Object> input) throws Exception {
    //noinspection unchecked
    return input.getType() == RecordType.ALERT ?
      Iterators.singletonIterator(((Alert) input.getValue())) : Iterators.emptyIterator();
  }
}
