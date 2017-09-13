/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.data2.transaction.Transactions;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Provides transaction management for Spark job and stage executors. It also expose an endpoint for stage executors
 * to get {@link Transaction} information associated with the stage. For detail design, please refer to the
 * <a href="https://wiki.cask.co/display/CE/Spark+Revamp">design documentation</a>.
 */
public final class SparkTransactionHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTransactionHandler.class);
  private static final TransactionCodec TX_CODEC = new TransactionCodec();
  private static final TransactionInfo IMPLICIT_TX_INFO = new TransactionInfo() {
    @Nullable
    @Override
    public Transaction getTransaction() {
      return null;
    }

    @Override
    public boolean commitOnJobEnded() {
      return true;
    }

    @Override
    public void onJobStarted() {
      // no-op
    }

    @Override
    public void onTransactionCompleted(boolean jobSucceeded, @Nullable TransactionFailureException failureCause) {
      // no-op
    }
  };

  private final TransactionSystemClient txClient;

  // Map from StageId to JobId. It is needed because Spark JobId is only available on the driver.
  // In the executor node, there is only StageId. The Spark StageId is unique across job, so it's ok to use a map.
  private final ConcurrentMap<Integer, Integer> stageToJob;
  private final ConcurrentMap<Integer, JobTransaction> jobTransactions;

  SparkTransactionHandler(TransactionSystemClient txClient) {
    this.txClient = txClient;
    this.stageToJob = new ConcurrentHashMap<>();
    this.jobTransactions = new ConcurrentHashMap<>();
  }

  /**
   * Notifies the given job execution started without any active transaction. A transaction will be started
   * on demand and commit when the job ended.
   *
   * @param jobId the unique id that identifies the job.
   * @param stageIds set of stage ids that are associated with the given job.
   */
  void jobStarted(Integer jobId, Set<Integer> stageIds) {
    jobStarted(jobId, stageIds, IMPLICIT_TX_INFO);
  }

  /**
   * Notifies the given job execution started.
   *
   * @param jobId the unique id that identifies the job.
   * @param stageIds set of stage ids that are associated with the given job.
   * @param transactionInfo information about the transaction to be used for the job.
   */
  void jobStarted(Integer jobId, Set<Integer> stageIds, TransactionInfo transactionInfo) {
    JobTransaction jobTransaction = new JobTransaction(jobId, stageIds, transactionInfo);
    LOG.debug("Spark job started: {}", jobTransaction);

    // Remember the job Id. We won't start a new transaction here until a stage requested for it.
    // This is because there can be job that doesn't need transaction or explicit transaction is being used
    JobTransaction existingJobTx = jobTransactions.putIfAbsent(jobId, jobTransaction);
    if (existingJobTx != null) {
      // Shouldn't happen as Spark generates unique Job Id.
      // If that really happen, just log and return
      LOG.error("Job already running: {}", existingJobTx);
      return;
    }

    // Build the stageId => jobId map first instead of putting to the concurrent map one by one
    Map<Integer, Integer> stageToJob = new HashMap<>();
    for (Integer stageId : stageIds) {
      stageToJob.put(stageId, jobId);
    }
    this.stageToJob.putAll(stageToJob);
  }

  /**
   * Notifies the given job execution completed.
   *
   * @param jobId the unique id that identifies the job.
   * @param succeeded {@code true} if the job execution completed successfully.
   */
  void jobEnded(Integer jobId, boolean succeeded) throws TransactionFailureException {
    JobTransaction jobTransaction = jobTransactions.remove(jobId);
    if (jobTransaction == null) {
      // Shouldn't happen, otherwise something very wrong. Can't do much, just log and return
      LOG.error("Transaction for job {} not found.", jobId);
      return;
    }

    LOG.debug("Spark job ended: {}", jobTransaction);

    // Cleanup the stage to job map
    stageToJob.keySet().removeAll(jobTransaction.getStageIds());

    // Complete the transaction
    jobTransaction.completed(succeeded);
  }

  /**
   * Handler method to get a serialized {@link Transaction} for the given stage.
   */
  @GET
  @Path("/spark/stages/{stage}/transaction")
  public void getTransaction(HttpRequest request, HttpResponder responder, @PathParam("stage") int stageId) {
    // Lookup the jobId from the stageId
    Integer jobId = stageToJob.get(stageId);
    if (jobId == null) {
      // If the JobId is not there, it's either the job hasn't been registered yet (because it's async) or
      // the job is already finished. For either case, return 404 and let the client to handle retry if necessary.
      responder.sendString(HttpResponseStatus.NOT_FOUND, "JobId not found for stage " + stageId);
      return;
    }

    // Get the transaction
    JobTransaction jobTransaction = jobTransactions.get(jobId);
    if (jobTransaction == null) {
      // The only reason we can find the jobId from the stageToJob map but not the job transaction is because
      // the job is completed, hence the transaction get removed. In normal case, it shouldn't happen
      // as a job won't complete if there are still stages running and
      // this method only gets called from stage running in executor node.
      responder.sendString(HttpResponseStatus.GONE,
                           "No transaction associated with the stage " + stageId + " of job " + jobId);
      return;
    }

    Transaction transaction = jobTransaction.getTransaction();
    if (transaction == null) {
      // Job failed to start a transaction. Response with GONE as well so that the stage execution can fail itself
      responder.sendString(HttpResponseStatus.GONE,
                           "Failed to start transaction for stage " + stageId + " of job " + jobId);
      return;
    }

    // Serialize the transaction and send it back
    try {
      responder.sendByteArray(HttpResponseStatus.OK, TX_CODEC.encode(transaction), null);
    } catch (IOException e) {
      // Shouldn't happen
      LOG.error("Failed to encode Transaction {}", jobTransaction, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Failed to encode transaction: " + e.getMessage());
    }
  }


  /**
   * A private class for handling the {@link Transaction} lifecycle for a job.
   */
  private final class JobTransaction {
    private final Integer jobId;
    private final Set<Integer> stageIds;
    private final TransactionInfo transactionInfo;
    private volatile Optional<Transaction> transaction;

    /**
     * Creates a {@link JobTransaction}.
     *
     * @param jobId the Spark job id
     * @param stageIds set of stage ids that are part of the job.
     * @param transactionInfo transaction information associated with this job.
     */
    JobTransaction(Integer jobId, Set<Integer> stageIds, TransactionInfo transactionInfo) {
      this.jobId = jobId;
      this.stageIds = ImmutableSet.copyOf(stageIds);
      this.transactionInfo = transactionInfo;

      Transaction tx = transactionInfo.getTransaction();
      this.transaction = tx == null ? null : Optional.of(tx);
    }

    /**
     * Returns the job id.
     */
    Integer getJobId() {
      return jobId;
    }

    /**
     * Returns id of all stages of the job.
     */
    Set<Integer> getStageIds() {
      return stageIds;
    }

    /**
     * Returns the {@link Transaction} associated with the job. If transaction hasn't been started, a new long
     * transaction will be started.
     *
     * @return the job's {@link Transaction} or {@code null} if it failed to start transaction for the job.
     */
    @Nullable
    public Transaction getTransaction() {
      Optional<Transaction> tx = transaction;
      if (tx == null) {
        // double-checked locking
        synchronized (this) {
          tx = transaction;
          if (tx == null) {
            try {
              tx = transaction = Optional.of(txClient.startLong());
            } catch (Throwable t) {
              LOG.error("Failed to start transaction for job {}", jobId, t);
              // Set the transaction to an absent Optional to indicate the starting of transaction failed.
              // This will prevent future call to this method to attempt to start a transaction again
              tx = transaction = Optional.absent();
            }
          }
        }
      }
      return tx.orNull();
    }

    /**
     * Completes the job {@link Transaction} by either committing or invalidating the transaction, based on the
     * job result.
     *
     * @param succeeded {@code true} if the job execution completed successfully.
     */
    public void completed(boolean succeeded) throws TransactionFailureException {
      // Return if doesn't need to commit the transaction
      if (!transactionInfo.commitOnJobEnded()) {
        return;
      }

      // Get the transaction to commit.
      Optional<Transaction> tx = transaction;
      if (tx == null || !tx.isPresent()) {
        // No transaction was started for the job, hence nothing to do.
        return;
      }

      Transaction jobTx = tx.get();
      try {
        if (succeeded) {
          LOG.debug("Committing transaction for job {}", jobId);
          try {
            txClient.commitOrThrow(jobTx);
            transactionInfo.onTransactionCompleted(succeeded, null);
          } catch (Throwable t) {
            // Any failure will invalidate the transaction
            Transactions.invalidateQuietly(txClient, jobTx);
            // Since the failure is unexpected, propagate it
            throw t;
          }
        } else {
          LOG.debug("Invalidating transaction for job {}", jobId);
          if (!txClient.invalidate(jobTx.getWritePointer())) {
            throw new TransactionFailureException("Failed to invalid transaction on job failure. JobId: "
                                                    + jobId + ", transaction: " + jobTx);
          }
          transactionInfo.onTransactionCompleted(succeeded, null);
        }
      } catch (Throwable t) {
        TransactionFailureException failureCause = Transactions.asTransactionFailure(t);
        transactionInfo.onTransactionCompleted(succeeded, failureCause);
        throw failureCause;
      }
    }

    @Override
    public String toString() {
      return "JobTransaction{" +
        "jobId=" + jobId +
        ", stageIds=" + stageIds +
        ", transaction=" + (transaction == null ? null : transaction.orNull()) +
        '}';
    }
  }
}
