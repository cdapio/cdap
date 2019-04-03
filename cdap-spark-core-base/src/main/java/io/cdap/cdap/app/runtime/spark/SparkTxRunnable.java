/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data.LineageDatasetContext;
import co.cask.cdap.data2.metadata.lineage.AccessType;

/**
 * An interface for executing task inside transaction. It is similar to {@link TxRunnable} but it provides
 * addition access to set {@link AccessType} of dataset.
 *
 * TODO (CDAP-5363): It should be unified with TxRunnable in some way.
 */
public interface SparkTxRunnable {

  /**
   * Provides a {@link LineageDatasetContext} to get instances of {@link Dataset}s.
   *
   * <p>
   *   Operations executed on a dataset within the execution of this method are committed as a single transaction.
   *   The transaction is started before this method is invoked and is committed upon successful execution.
   *   Exceptions thrown while committing the transaction or thrown by user-code result in a rollback of the
   *   transaction.
   * </p>
   *
   * @param context to get datasets from
   */
  void run(LineageDatasetContext context) throws Exception;
}
