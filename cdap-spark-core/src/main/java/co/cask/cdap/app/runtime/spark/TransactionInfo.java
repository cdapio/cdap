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

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionFailureException;
import org.apache.spark.scheduler.SparkListener;

import javax.annotation.Nullable;

/**
 * Interface containing information about {@link Transaction} being used for the Spark job execution.
 * It is used as a communicate channel between the {@link SparkTransactional} and the {@link SparkListener} inside
 * {@link DefaultSparkExecutionContext}.
 */
interface TransactionInfo {

  /**
   * Returns the active transaction or {@code null} if there is no active transaction.
   */
  @Nullable
  Transaction getTransaction();

  /**
   * Returns {@code true} if the transaction needs to be committed when the job ended.
   */
  boolean commitOnJobEnded();

  /**
   * Callback when a job started.
   */
  void onJobStarted();

  /**
   * Callback when a job completed and the transaction is committed. This method only gets called if
   * {@link #commitOnJobEnded()} returns {@code true}.
   *
   * @param jobSucceeded {@code true} if the job execution succeeded or {@code false} otherwise.
   * @param failureCause The {@link TransactionFailureException} if failed to commit or invalidate the transaction;
   *                     otherwise it will be {@code null}.
   */
  void onTransactionCompleted(boolean jobSucceeded, @Nullable TransactionFailureException failureCause);
}
