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

package io.cdap.cdap.etl.api.streaming;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.etl.api.batch.BatchContext;
import org.apache.tephra.TransactionFailureException;

import javax.annotation.Nullable;

/**
 * Context passed to streaming source during prepare run phase.
 */
public interface StreamingSourceContext extends BatchContext {
  /**
   * Register dataset lineage for this Spark program using the given reference name
   *
   * @param referenceName reference name used for source
   * @param schema schema for this dataset
   *
   * @throws DatasetManagementException thrown if there was an error in creating reference dataset
   * @throws TransactionFailureException thrown if there was an error while fetching the dataset to register usage
   */
  void registerLineage(String referenceName,
                       @Nullable Schema schema)
    throws DatasetManagementException, TransactionFailureException, AccessException;

  /**
   * Indicates whether the pipeline is running in preview.
   *
   * @return a boolean value which indicates the pipeline is running in preview mode.
   */
  boolean isPreviewEnabled();
}
