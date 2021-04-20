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

package io.cdap.cdap.etl.api.engine.sql;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FromKeyValueTransform;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.cdap.etl.api.ToKeyValueTransform;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;

/**
 * A SQL Engine can be used to pushdown certain dataset operations.
 * <p>
 * SQL Engines are implemented as plugins.
 * <p>
 * Internally, the SQL Engine needs to handle retries and only surface {@link SQLEngineException} errors when there is a
 * problem that cannot be recovered from and pipeline must be stopped.
 * <p>
 * Note that this operation may be refactored at a later release, and thus this method signature is not guaranteed to
 * remain stable.
 *
 * @param <KEY_OUT>   The type for the Output Key when mapping a StructuredRecord
 * @param <VALUE_OUT> The type for the Output Value when mapping a StructuredRecord
 * @param <KEY_IN>    The type for the Input Key when building a StructuredRecord
 * @param <VALUE_IN>  The type for the Input Value when building a StructuredRecord
 */
@Beta
public interface SQLEngine<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> extends PipelineConfigurable,
  SubmitterLifecycle<BatchContext>, FromKeyValueTransform<StructuredRecord, KEY_IN, VALUE_IN>,
  ToKeyValueTransform<StructuredRecord, KEY_OUT, VALUE_OUT> {
  String PLUGIN_TYPE = "sqlengine";

  /**
   * Creates an Output Format Provided that can be used to push records into a SQL Engine.
   * <p>
   * After created, this dataset will be considered "locked" until the output has been committed.
   *
   * @param pushRequest the request containing information about the dataset name and schema.t
   * @return an {@link OutputFormatProvider} instance that can be used to write records to the SQL Engine into the
   * specified dataset.
   */
  OutputFormatProvider getPushProvider(SQLPushRequest pushRequest) throws SQLEngineException;

  /**
   * Creates an InputFormatProvider that can be used to pull records from the specified dataset.
   *
   * @param pullRequest the request containing information about the dataset name and schema.
   * @return AN {@link InputFormatProvider} instance that can be used to read records form the specified dataset in the
   * SQL Engine.
   */
  InputFormatProvider getPullProvider(SQLPullRequest pullRequest) throws SQLEngineException;

  /**
   * Check if this dataset exists in the SQL Engine.
   * <p>
   * This is a blocking call. if the process to write records into a dataset is ongoing, this method will block until
   * the process completes. This ensures an accurate result for this operation.
   *
   * @param datasetName the dataset name.
   * @return boolean specifying if this dataset exists in the remote engine.
   */
  boolean exists(String datasetName) throws SQLEngineException;

  /**
   * Check if the supplied Join Definition can be executed in this engine.
   *
   * @param joinRequest the join request to validate.
   * @return boolean specifying if this join operation can be executed in the SQl Engine.
   */
  boolean canJoin(SQLJoinRequest joinRequest);

  /**
   * Executes the join operation defined by the supplied join request.
   * <p>
   * The returned {@link SQLOperationResult} can be used to determine the status of this task.
   *
   * @param joinRequest the join request to execute.
   * @return the {@link SQLOperationResult} instance with information about the execution of this task.
   */
  SQLOperationResult join(SQLJoinRequest joinRequest) throws SQLEngineException;

  /**
   * Deletes all temporary datasets and cleans up all temporary data from the SQL engine.
   *
   * @param datasetName boolean specifying if all running tasks should be stopped at this time (if any are running).
   */
  void cleanup(String datasetName) throws SQLEngineException;
}
