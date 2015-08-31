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
 * Streaming standard Deviation Aggregation Function
 * Calculates standard deviation with one pass
 */
public class StandardDeviation implements BasicAggregationFunction {
  private Double sumOfValues = 0.0;
  private Double sumOfSquaresOfValues = 0.0;
  private Integer numberOfValues = 0;

  @Override
  public Double deserialize(byte[] value) {
    return value == null ? null : Bytes.toDouble(value);
  }

  @Override
  public void add(DataQualityWritable value) {
    Double newVal = Double.parseDouble(value.get().toString());
    sumOfValues += newVal;
    sumOfSquaresOfValues += (newVal * newVal);
    numberOfValues += 1;
  }

  @Override
  public byte[] aggregate() {
    Double mean = sumOfValues / numberOfValues;
    Double squareOfMean = Math.pow(mean, 2.0);
    Double meanOfSquares = sumOfSquaresOfValues / numberOfValues;
    Double variance = meanOfSquares - squareOfMean;
    Double standardDeviation = Math.sqrt(variance);
    return Bytes.toBytes(standardDeviation);
  }
}
