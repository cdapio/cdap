/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.streaming.function;

import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.RecordType;
import org.apache.spark.api.java.function.Function;

/**
 * Wraps output records into a RecordInfo object, which contains which stage the record was emitted from.
 *
 * @param <T> type of output record.
 */
public class RecordInfoWrapper<T> implements Function<T, RecordInfo<T>> {
  private final String stageName;

  public RecordInfoWrapper(String stageName) {
    this.stageName = stageName;
  }

  @Override
  public RecordInfo<T> call(T input) throws Exception {
    return RecordInfo.builder(input, stageName, RecordType.OUTPUT).build();
  }
}
