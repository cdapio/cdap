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

package io.cdap.cdap.etl.api.engine.sql.dataset;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Represents a dataset that resides in a SQL engine outside of spark.
 */
public interface SQLDataset {

  /**
   * Get the name of this dataset. This can be used to link datasets in the SQL engine with Datasets stored in Spark.
   */
  String getDatasetName();

  /**
   * Get the schema for the records stored in this dataset.
   */
  Schema getSchema();

  /**
   * Get the number of rows stored in this dataset.
   */
  long getNumRows();

}
