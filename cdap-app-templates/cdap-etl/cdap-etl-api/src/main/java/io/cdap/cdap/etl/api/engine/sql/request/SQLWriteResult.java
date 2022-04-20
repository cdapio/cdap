/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.engine.sql.request;

import io.cdap.cdap.api.annotation.Beta;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * A request to perform write operation
 */
@Beta
public class SQLWriteResult implements Serializable {
  private final String datasetName;
  private final SQLWriteOperationResult result;
  private final long numRecords;
  private final Map<String, Long> metrics;

  private static final long serialVersionUID = 7843665889511527477L;

  /**
   * Creates a new SQLWriteResult instance
   *
   * @param datasetName The name of the dataset (stage) that is being written.
   * @param result      result of this write operation.
   * @param numRecords  number of written records (if any).
   */
  public SQLWriteResult(String datasetName, SQLWriteOperationResult result, long numRecords) {
    this.datasetName = datasetName;
    this.result = result;
    this.numRecords = numRecords;
    this.metrics = Collections.emptyMap();
  }

  /**
   * Creates a new SQLWriteResult instance
   *
   * @param datasetName The name of the dataset (stage) that is being written.
   * @param result      result of this write operation.
   * @param numRecords  number of written records (if any).
   * @param metrics  map containing metrics from the write operation.
   */
  public SQLWriteResult(String datasetName,
                        SQLWriteOperationResult result,
                        long numRecords,
                        Map<String, Long> metrics) {
    this.datasetName = datasetName;
    this.result = result;
    this.numRecords = numRecords;
    this.metrics = metrics;
  }

  /**
   * Utility method to create an instance with a successful result
   *
   * @param datasetName dataset name
   * @param numRecords  number of written records
   * @return new instance with a Success result and the number of specified records.
   */
  public static SQLWriteResult success(String datasetName, long numRecords) {
    return new SQLWriteResult(datasetName, SQLWriteOperationResult.SUCCESS, numRecords);
  }

  /**
   * Utility method to create an instance with a successful result
   *
   * @param datasetName dataset name.
   * @param numRecords  number of written records.
   * @param metrics  map containing metrics from the write operation.
   * @return new instance with a Success result and the number of specified records.
   */
  public static SQLWriteResult success(String datasetName, long numRecords, Map<String, Long> metrics) {
    return new SQLWriteResult(datasetName, SQLWriteOperationResult.SUCCESS, numRecords, metrics);
  }

  /**
   * Utility method to create an instance with an unsupported result
   *
   * @param datasetName dataset name
   * @return new instance with an unsupported result status and no output records.
   */
  public static SQLWriteResult unsupported(String datasetName) {
    return new SQLWriteResult(datasetName, SQLWriteOperationResult.UNSUPPORTED, 0);
  }

  /**
   * Utility method to create an instance with a failed result
   *
   * @param datasetName dataset name
   * @return new instance with an unsupported failed status and no output records.
   */
  public static SQLWriteResult faiure(String datasetName) {
    return new SQLWriteResult(datasetName, SQLWriteOperationResult.FAILURE, 0);
  }

  /**
   * Utility method to create an instance with a failed result
   *
   * @param datasetName dataset name.
   * @param metrics  map containing metrics from the write operation.
   * @return new instance with an unsupported failed status and no output records.
   */
  public static SQLWriteResult faiure(String datasetName, Map<String, Long> metrics) {
    return new SQLWriteResult(datasetName, SQLWriteOperationResult.FAILURE, 0, metrics);
  }

  /**
   * Get the name of the dataset that should be written to the sink.
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Get the result of the execution of this write operation.
   */
  public SQLWriteOperationResult getResult() {
    return result;
  }

  /**
   * Get the number of records that were written into the sink, if any.
   */
  public long getNumRecords() {
    return numRecords;
  }

  /**
   * Used to check if the write operation was successful
   *
   * @return true if successful, false otherwise.
   */
  public boolean isSuccessful() {
    return result == SQLWriteOperationResult.SUCCESS;
  }

  /**
   * Get metrics for the write operation
   *
   * @return map containing metrics for the write operation.
   */
  public Map<String, Long> getMetrics() {
    return metrics;
  }

  public enum SQLWriteOperationResult {
    SUCCESS,
    FAILURE,
    UNSUPPORTED
  }
}
