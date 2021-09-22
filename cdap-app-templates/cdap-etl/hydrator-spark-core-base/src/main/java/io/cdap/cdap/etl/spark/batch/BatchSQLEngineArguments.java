/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.etl.api.dl.DLArguments;
import io.cdap.cdap.etl.api.dl.DLDataSet;

import java.util.HashMap;
import java.util.Map;

public class BatchSQLEngineArguments implements DLArguments {
  private final DLDataSet inputDataSet;
  private final Map<String, DLDataSet> inputMap;
  private DLDataSet outputDataSet;


  public BatchSQLEngineArguments(DLDataSet inputDataSet, Map<String, DLDataSet> inputMap) {

    this.inputDataSet = inputDataSet;
    this.inputMap = inputMap;
  }

  @Override
  public DLDataSet getInputDataSet() {
    return inputDataSet;
  }

  @Override
  public DLDataSet getInputDataSet(String inputStage) {
    return inputMap.get(inputStage);
  }

  @Override
  public void setOutputDataSet(DLDataSet outputDataSet) {
    this.outputDataSet = outputDataSet;
  }

  @Override
  public void setOutputDataSet(String portName, DLDataSet outputDataSet) {
    throw new UnsupportedOperationException("Only single output is supported");
  }

  public DLDataSet getOutputDataSet() {
    return outputDataSet;
  }
}
