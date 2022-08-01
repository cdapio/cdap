/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.engine.sql.dataset;

/**
 * Represents a dataset that resides in a SQL engine outside of spark.
 */
public interface SQLDataset extends SQLDatasetDescription {

  /**
   * Get the number of rows stored in this dataset.
   */
  long getNumRows();

  /**
   * Method to determine if this SQL dataset is a valid dataset that is correctly pushed into the SQL engine
   * @return boolean specifying if this stage is pushed into the SQL engine
   */
  default boolean isValid() {
    return true;
  }

}
