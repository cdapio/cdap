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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;

import java.io.Serializable;

/**
 * A request to perform read operation
 */
@Beta
public class SQLReadResult implements Serializable {
  private final String datasetName;
  private final SQLReadOperationResult result;
  private final SQLDataset sqlDataset;

  private static final long serialVersionUID = 7843665889511527477L;

  /**
   * Creates a new SQLReadResult instance
   * @param datasetName The name of the dataset (stage) that is being written
   * @param result result of this read operation
   * @param sqlDataset the SQL Dataset
   */
  public SQLReadResult(String datasetName,
                       SQLReadOperationResult result,
                       SQLDataset sqlDataset) {
    this.datasetName = datasetName;
    this.result = result;
    this.sqlDataset = sqlDataset;
  }

  /**
   * Utility method to create an instance with a successful result
   * @param datasetName dataset name
   * @param sqlDataset the SQL Dataset
   * @return new instance with a Success result and the number of specified records.
   */
  public static SQLReadResult success(String datasetName, SQLDataset sqlDataset) {
    return new SQLReadResult(datasetName, SQLReadOperationResult.SUCCESS, sqlDataset);
  }

  /**
   * Utility method to create an instance with an unsupported result
   * @param datasetName dataset name
   * @return new instance with an unsupported result status and no output records.
   */
  public static SQLReadResult unsupported(String datasetName) {
    return new SQLReadResult(datasetName, SQLReadOperationResult.UNSUPPORTED, InvalidSQLDataset.get());
  }

  /**
   * Utility method to create an instance with a failed result
   * @param datasetName dataset name
   * @return new instance with an unsupported failed status and no output records.
   */
  public static SQLReadResult faiure(String datasetName) {
    return new SQLReadResult(datasetName, SQLReadOperationResult.FAILURE, InvalidSQLDataset.get());
  }

  /**
   * Get the name of the dataset that should be written to the sink.
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Get the result of the execution of this read operation.
   */
  public SQLReadOperationResult getResult() {
    return result;
  }

  /**
   * Get the SQL Dataset instance
   * @return
   */
  public SQLDataset getSqlDataset() {
    return sqlDataset;
  }

  /**
   * Used to check if the read operation was successful
   * @return true if successful, false otherwise.
   */
  public boolean isSuccessful() {
    return result == SQLReadOperationResult.SUCCESS;
  }

  public enum SQLReadOperationResult {
    SUCCESS,
    FAILURE,
    UNSUPPORTED
  }

  /**
   * Represents an invalid SQL dataset resulting from a failed read operation
   */
  private static class InvalidSQLDataset implements SQLDataset {
    static final InvalidSQLDataset INSTANCE = new InvalidSQLDataset();

    @Override
    public long getNumRows() {
      return -1;
    }

    @Override
    public String getDatasetName() {
      return null;
    }

    @Override
    public Schema getSchema() {
      return null;
    }

    @Override
    public boolean isValid() {
      return false;
    }

    public static InvalidSQLDataset get() {
      return INSTANCE;
    }
  }
}
