/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.action;

import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.lineage.field.LineageRecorder;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.cdap.etl.api.lineage.AccessType;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import org.apache.tephra.TransactionFailureException;

import java.util.Collection;
import java.util.List;

/**
 * Represents the context available to the action plugin during runtime.
 */
public interface ActionContext extends StageContext, Transactional, SecureStore, SecureStoreManager, LineageRecorder {

  /**
   * Returns settable pipeline arguments. These arguments are shared by all pipeline stages, so plugins should be
   * careful to prefix any arguments that should not be clobbered by other pipeline stages.
   *
   * @return settable pipeline arguments
   */
  @Override
  SettableArguments getArguments();

  /**
   * This method is based on the assumption that the plugin has an input schema or output schema and the DAG of the
   * operations are known. Actions do not have input and output schema, and the DAG is unknown to us. Therefore,
   * this method cannot be used to emit field lineage information.
   *
   * @param fieldOperations the operations to be recorded
   * @deprecated use {@link #record(Collection)} to emit field operations for actions, that method requires the input
   *             field to contain the previous originated operation name. Using that DAG can be computed so the field
   *             linage can be computed.
   */
  @Override
  @Deprecated
  default void record(List<FieldOperation> fieldOperations) {
    throw new UnsupportedOperationException("Lineage recording is not supported.");
  }

  /**
   * Register lineage for this Spark program using the given reference name
   *
   * @param referenceName reference name used for source
   * @param accessType the access type of the lineage
   * @throws DatasetManagementException thrown if there was an error in creating reference dataset
   * @throws TransactionFailureException thrown if there was an error while fetching the dataset to register usage
   */
 default void registerLineage(String referenceName,
                              AccessType accessType)
   throws DatasetManagementException, TransactionFailureException, AccessException {
   // no-op
 }
}
