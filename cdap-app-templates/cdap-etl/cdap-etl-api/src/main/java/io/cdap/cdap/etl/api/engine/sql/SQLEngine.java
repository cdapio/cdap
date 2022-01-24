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

import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.cdap.etl.api.engine.sql.capability.PullCapability;
import io.cdap.cdap.etl.api.engine.sql.capability.PushCapability;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetConsumer;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetProducer;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPullDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLRelationDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLTransformDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLTransformRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLWriteRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLWriteResult;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.Relation;

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;

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
public interface SQLEngine<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT>
  extends PipelineConfigurable, SubmitterLifecycle<RuntimeContext> {

  /**
   * Creates an Output Format Provided that can be used to push records into a SQL Engine.
   * <p>
   * After created, this dataset will be considered "locked" until the output has been committed.
   *
   * @param pushRequest the request containing information about the dataset name and schema.t
   * @return {@link SQLPushDataset} instance that can be used to write records to the SQL Engine.
   */
  SQLPushDataset<StructuredRecord, KEY_OUT, VALUE_OUT> getPushProvider(SQLPushRequest pushRequest)
    throws SQLEngineException;

  /**
   * Creates an InputFormatProvider that can be used to pull records from the specified dataset.
   *
   * @param pullRequest the request containing information about the dataset name and schema.
   * @return {@link SQLPullDataset} instance that can be used to read records from the SQL engine.
   */
  SQLPullDataset<StructuredRecord, KEY_IN, VALUE_IN> getPullProvider(SQLPullRequest pullRequest)
    throws SQLEngineException;

  /**
   * Creates a {@link SQLDatasetConsumer} for a given {@link SQLPushRequest} with support for
   * a supplied{@link PushCapability}.
   *
   * If the return value is null, we must assume that the supplied {@link SQLPushRequest} is not supported
   * for this capability.
   *
   * @param pushRequest push request used to create a consumer
   * @param capability the push capability we wish to use to upload
   * @return a dataset consumer for this request using the specified capability.
   */
  @Nullable
  default SQLDatasetConsumer getConsumer(SQLPushRequest pushRequest, PushCapability capability) {
    return null;
  }

  /**
   * Creates a {@link SQLDatasetProducer} for a given {@link SQLPullRequest} with support for
   * a supplied {@link PullCapability}.
   *
   * If the return value is null, we must assume that the supplied {@link SQLPullRequest} is not supported
   * for this capability.
   *
   * @param pullRequest pull request used to create a producer
   * @param capability the capability we want to use to upload
   * @return a dataset consumer for this request using the specified capability.
   */
  @Nullable
  default SQLDatasetProducer getProducer(SQLPullRequest pullRequest, PullCapability capability) {
    return null;
  }

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
   * @param joinDefinition the join definition to validate
   * @return boolean specifying if this join operation can be executed in the SQl Engine.
   */
  boolean canJoin(SQLJoinDefinition joinDefinition);

  /**
   *
   * @return if engine supports relational plugins
   * @see io.cdap.cdap.etl.api.relational.RelationalTransform
   */
  default boolean supportsRelationalTranform() {
    return false;
  };

  /**
   * Final check if the requested transformations can be executed in the SQL Engine.
   * @param transformDefinition SQL transformation to validate
   * @return if transformations can be executed in the SQL Egine
   */
  default boolean canTransform(SQLTransformDefinition transformDefinition) {
    return false;
  }

  /**
   * Executes the join operation defined by the supplied join request.
   * <p>
   * All datasets involved in this joinRequest must be pushed to the SQL engine by calling the
   * {@link SQLEngine#getPushProvider(SQLPushRequest)} method, or as a result of another operation.
   * <p>
   * The returned {@link SQLDataset} represents the resulting record form this operation
   *
   * @param joinRequest the join request to execute.
   * @return the {@link SQLDataset} instance representing the output of this operation.
   */
  SQLDataset join(SQLJoinRequest joinRequest) throws SQLEngineException;

  /**
   * Consume a {@link SQLWriteRequest} and write this output into the SQL engine if possible
   * @param writeRequest write request to consume
   * @return true if the write request could be consumed, false otherwise
   * @throws SQLEngineException if the write process fails unexpectedly.
   */
  default SQLWriteResult write(SQLWriteRequest writeRequest) throws SQLEngineException {
    return SQLWriteResult.unsupported(writeRequest.getDatasetName());
  }

  /**
   * Deletes all temporary datasets and cleans up all temporary data from the SQL engine.
   *
   * @param datasetName boolean specifying if all running tasks should be stopped at this time (if any are running).
   */
  void cleanup(String datasetName) throws SQLEngineException;

  /**
   *
   * @return engine to use for relational tranform. Will be called only if {@link #supportsRelationalTranform()}
   * is true
   * @throws SQLEngineException
   */
  default Engine getRelationalEngine() {
    throw new UnsupportedOperationException("Relational transform is not supported by the engine");
  }

  /**
   * Prepares Relational plugin input based on provided descripton and dataset supplier.
   * @param relationDefinition dataset name and schema
   * @return a relation for the dataset definition
   */
  default Relation getRelation(SQLRelationDefinition relationDefinition) {
    throw new UnsupportedOperationException("Relational transform is not supported by the engine");
  }

  /**
   * Performs transformation of a single relation.
   * @param context transformation context with transformation definition, input datasets and setter for output
   * @return output datasets for the transform requested
   * @throws SQLEngineException
   */
  default SQLDataset transform(SQLTransformRequest context)
    throws SQLEngineException {
    throw new UnsupportedOperationException("Relational transform is not supported by the engine");
  }

  /**
   * Defines pull capabilities supported by this SQL Engine
   * @return Set detailing pull capabilities supported by this engine.
   */
  default Set<PullCapability> getPullCapabilities() {
    return Collections.emptySet();
  }

  /**
   * Defines push capabilities supported by this SQL Engine
   * @return Set detailing push capabilities supported by this engine.
   */
  default Set<PushCapability> getPushCapabilities() {
    return Collections.emptySet();
  }
}
