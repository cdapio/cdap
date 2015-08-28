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

package co.cask.cdap.dq.functions;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.dq.DataQualityWritable;

/**
 * Average Aggregation Function
 */
public class Mean implements BasicAggregationFunction {
  private Double runningSum = 0.0;
  private Integer lengthValues = 0;

  @Override
  public Double deserialize(byte[] value) {
    return value == null ? null : Bytes.toDouble(value);
  }

  @Override
  public void add(DataQualityWritable value) {
    runningSum += Double.parseDouble(value.get().toString());
    lengthValues += 1;
  }

  @Override
  public byte[] aggregate() {
    return Bytes.toBytes(runningSum / lengthValues);
  }
}
