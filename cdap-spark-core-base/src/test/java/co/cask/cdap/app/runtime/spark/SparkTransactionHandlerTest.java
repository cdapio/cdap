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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.persist.TransactionSnapshot;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Unit tests for the {@link SparkTransactionHandler}.
 */
public class SparkTransactionHandlerTest {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTransactionHandlerTest.class);

  private static TransactionManager txManager;
  private static TransactionSystemClient txClient;
  private static SparkTransactionHandler sparkTxHandler;
  private static SparkDriverHttpService httpService;
  private static SparkTransactionClient sparkTxClient;

  @BeforeClass
  public static void init() throws UnknownHostException {
    txManager = new TransactionManager(new Configuration());
    txManager.startAndWait();

    txClient = new InMemoryTxSystemClient(txManager);

    sparkTxHandler = new SparkTransactionHandler(txClient);
    httpService = new SparkDriverHttpService("test", InetAddress.getLoopbackAddress().getCanonicalHostName(),
                                             sparkTxHandler);
    httpService.startAndWait();

    sparkTxClient = new SparkTransactionClient(httpService.getBaseURI());
  }

  @AfterClass
  public static void finish() {
    httpService.stopAndWait();
    txManager.stopAndWait();
  }

  /**
   * Tests the basic flow of starting a job, make a tx request from a stage and ending the job.
   */
  @Test
  public void testBasicJobRun() throws Exception {
    AtomicInteger jobIdGen = new AtomicInteger();
    AtomicInteger stageIdGen = new AtomicInteger();

    // A successful job run
    testRunJob(jobIdGen.getAndIncrement(), generateStages(stageIdGen, 3), true);

    // A failure job run
    testRunJob(jobIdGen.getAndIncrement(), generateStages(stageIdGen, 4), false);
  }

  /**
   * Tests concurrent jobs submission.
   */
  @Test(timeout = 120000L)
  public void testConcurrentJobRun() throws Exception {
    final AtomicInteger jobIdGen = new AtomicInteger();
    final AtomicInteger stageIdGen = new AtomicInteger();

    // Start 30 jobs concurrently
    int threads = 30;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      final CyclicBarrier barrier = new CyclicBarrier(threads);
      final Random random = new Random();
      CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);

      // For each run, return the verification result
      for (int i = 0; i < threads; i++) {
        completionService.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            barrier.await();
            try {
              // Run job with 2-5 stages, with job either succeeded or failed
              testRunJob(jobIdGen.getAndIncrement(), generateStages(stageIdGen, 2 + random.nextInt(4)),
                         random.nextBoolean());
              return true;
            } catch (Throwable t) {
              LOG.error("testRunJob failed.", t);
              return false;
            }
          }
        });
      }

      // All testRunJob must be completed successfully
      boolean result = true;
      for (int i = 0; i < threads; i++) {
        result = result && completionService.take().get();
      }

      Assert.assertTrue(result);
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Tests the explicit transaction that covers multiple jobs.
   */
  @Test
  public void testExplicitTransaction() throws Exception {
    final AtomicInteger jobIdGen = new AtomicInteger();
    final AtomicInteger stageIdGen = new AtomicInteger();

    Transaction transaction = txClient.startLong();

    // Execute two jobs with the same explicit transaction
    testRunJob(jobIdGen.getAndIncrement(), generateStages(stageIdGen, 2), true, transaction);
    testRunJob(jobIdGen.getAndIncrement(), generateStages(stageIdGen, 3), true, transaction);

    // Should be able to commit the transaction
    Assert.assertTrue(txClient.commit(transaction));
  }

  /**
   * Tests the case where starting of transaction failed.
   */
  @Test
  public void testFailureTransaction() throws Exception {
    TransactionManager txManager = new TransactionManager(new Configuration()) {
      @Override
      public Transaction startLong() {
        throw new IllegalStateException("Cannot start long transaction");
      }
    };
    txManager.startAndWait();
    try {
      SparkTransactionHandler txHandler = new SparkTransactionHandler(new InMemoryTxSystemClient(txManager));
      SparkDriverHttpService httpService = new SparkDriverHttpService(
        "test", InetAddress.getLoopbackAddress().getCanonicalHostName(), txHandler);
      httpService.startAndWait();
      try {
        // Start a job
        txHandler.jobStarted(1, ImmutableSet.of(2));

        // Make a call to the stage transaction endpoint, it should throw TransactionFailureException
        try {
          new SparkTransactionClient(httpService.getBaseURI()).getTransaction(2, 1, TimeUnit.SECONDS);
          Assert.fail("Should failed to get transaction");
        } catch (TransactionFailureException e) {
          // expected
        }

        // End the job
        txHandler.jobEnded(1, false);
      } finally {
        httpService.stopAndWait();
      }
    } finally {
      txManager.stopAndWait();
    }
  }

  /**
   * Tests the retry timeout logic in the {@link SparkTransactionClient}.
   */
  @Test
  public void testClientRetry() throws Exception {
    final Set<Integer> stages = ImmutableSet.of(2);

    // Delay the call to jobStarted by 3 seconds
    Executors.newSingleThreadScheduledExecutor().schedule(new Runnable() {
      @Override
      public void run() {
        sparkTxHandler.jobStarted(1, stages);
      }
    }, 3, TimeUnit.SECONDS);

    // Should be able to get the transaction, hence no exception
    sparkTxClient.getTransaction(2, 10, TimeUnit.SECONDS);

    sparkTxHandler.jobEnded(1, true);
  }

  /**
   * Simulates a single job run which contains multiple stages.
   *
   * @param jobId the job id
   * @param stages stages of the job
   * @param jobSucceeded end result of the job
   */
  private void testRunJob(int jobId, Set<Integer> stages, boolean jobSucceeded) throws Exception {
    testRunJob(jobId, stages, jobSucceeded, null);
  }

  /**
   * Simulates a single job run which contains multiple stages with an optional explicit {@link Transaction} to use.
   *
   * @param jobId the job id
   * @param stages stages of the job
   * @param jobSucceeded end result of the job
   * @param explicitTransaction the job transaction to use if not {@code null}
   */
  private void testRunJob(int jobId, Set<Integer> stages, boolean jobSucceeded,
                          @Nullable final Transaction explicitTransaction) throws Exception {
    // Before job start, no transaction will be associated with the stages
    verifyStagesTransactions(stages, new ClientTransactionVerifier() {
      @Override
      public boolean verify(@Nullable Transaction transaction, @Nullable Throwable failureCause) throws Exception {
        return transaction == null && failureCause instanceof TimeoutException;
      }
    });

    // Now start the job
    if (explicitTransaction == null) {
      sparkTxHandler.jobStarted(jobId, stages);
    } else {
      sparkTxHandler.jobStarted(jobId, stages, new TransactionInfo() {
        @Override
        public Transaction getTransaction() {
          return explicitTransaction;
        }

        @Override
        public boolean commitOnJobEnded() {
          return false;
        }

        @Override
        public void onJobStarted() {
          // no-op
        }

        @Override
        public void onTransactionCompleted(boolean jobSucceeded, @Nullable TransactionFailureException failureCause) {
          // no-op
        }
      });
    }

    // For all stages, it should get the same transaction
    final Set<Transaction> transactions = Collections.newSetFromMap(new ConcurrentHashMap<Transaction, Boolean>());
    verifyStagesTransactions(stages, new ClientTransactionVerifier() {
      @Override
      public boolean verify(@Nullable Transaction transaction, @Nullable Throwable failureCause) throws Exception {
        transactions.add(new TransactionWrapper(transaction));
        return transaction != null;
      }
    });

    // Transactions returned for all stages belonging to the same job must return the same transaction
    Assert.assertEquals(1, transactions.size());

    // The transaction must be in progress
    Transaction transaction = transactions.iterator().next();
    Assert.assertTrue(txManager.getCurrentState().getInProgress().containsKey(transaction.getWritePointer()));

    // If run with an explicit transaction, then all stages' transactions must be the same as the explicit transaction
    if (explicitTransaction != null) {
      Assert.assertEquals(new TransactionWrapper(explicitTransaction), transaction);
    }

    // Now finish the job
    sparkTxHandler.jobEnded(jobId, jobSucceeded);

    // After job finished, no transaction will be associated with the stages
    verifyStagesTransactions(stages, new ClientTransactionVerifier() {
      @Override
      public boolean verify(@Nullable Transaction transaction, @Nullable Throwable failureCause) throws Exception {
        return transaction == null && failureCause instanceof TimeoutException;
      }
    });

    // Check the transaction state based on the job result
    TransactionSnapshot txState = txManager.getCurrentState();

    // If explicit transaction is used, the transaction should still be in-progress
    if (explicitTransaction != null) {
      Assert.assertTrue(txState.getInProgress().containsKey(transaction.getWritePointer()));
    } else {
      // With implicit transaction, after job completed, the tx shouldn't be in-progress
      Assert.assertFalse(txState.getInProgress().containsKey(transaction.getWritePointer()));

      if (jobSucceeded) {
        // Transaction must not be in the invalid list
        Assert.assertFalse(txState.getInvalid().contains(transaction.getWritePointer()));
      } else {
        // Transaction must be in the invalid list
        Assert.assertTrue(txState.getInvalid().contains(transaction.getWritePointer()));
      }
    }
  }

  /**
   * Creates a new set of stage ids.
   */
  private Set<Integer> generateStages(AtomicInteger idGen, int stages) {
    Set<Integer> result = new LinkedHashSet<>();
    for (int i = 0; i < stages; i++) {
      result.add(idGen.getAndIncrement());
    }
    return result;
  }

  /**
   * Verifies the result of get stage transaction for the given set of stages.
   * The get transaction will be called concurrently for all stages.
   *
   * @param stages set of stages to verify
   * @param verifier a {@link ClientTransactionVerifier} to verify the http call result.
   */
  private void verifyStagesTransactions(Set<Integer> stages,
                                        final ClientTransactionVerifier verifier) throws Exception {
    final CyclicBarrier barrier = new CyclicBarrier(stages.size());
    final ExecutorService executor = Executors.newFixedThreadPool(stages.size());
    try {
      CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
      for (final int stageId : stages) {
        completionService.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            barrier.await();
            try {
              return verifier.verify(sparkTxClient.getTransaction(stageId, 0, TimeUnit.SECONDS), null);
            } catch (Throwable t) {
              return verifier.verify(null, t);
            }
          }
        });
      }

      boolean result = true;
      for (int i = 0; i < stages.size(); i++) {
        result = result && completionService.poll(10, TimeUnit.SECONDS).get();
      }

      // All verifications must be true
      Assert.assertTrue(result);
    } finally {
      executor.shutdown();
    }
  }

  private interface ClientTransactionVerifier {
    /**
     * Verifies the result of a call to the transaction service.
     */
    boolean verify(@Nullable Transaction transaction, @Nullable Throwable failureCause) throws Exception;
  }

  /**
   * A wrapper class for Transaction to provide equals and hashCode method.
   */
  private static final class TransactionWrapper extends Transaction {

    private final Transaction transaction;

    TransactionWrapper(Transaction tx) {
      super(tx.getReadPointer(), tx.getTransactionId(), tx.getWritePointer(), tx.getInvalids(), tx.getInProgress(),
            tx.getFirstShortInProgress(), tx.getType(), tx.getCheckpointWritePointers(), tx.getVisibilityLevel());
      this.transaction = tx;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Transaction other = ((TransactionWrapper) o).transaction;
      return (transaction.getReadPointer() == other.getReadPointer())
        && (transaction.getTransactionId() == other.getTransactionId())
        && (transaction.getWritePointer() == other.getWritePointer())
        && Arrays.equals(transaction.getInvalids(), other.getInvalids())
        && Arrays.equals(transaction.getInProgress(), other.getInProgress())
        && (transaction.getFirstShortInProgress() == other.getFirstShortInProgress())
        && (transaction.getType() == other.getType())
        && Arrays.equals(transaction.getCheckpointWritePointers(), other.getCheckpointWritePointers())
        && (transaction.getVisibilityLevel() == other.getVisibilityLevel());
    }

    @Override
    public int hashCode() {
      return Objects.hash(
        transaction.getReadPointer(),
        transaction.getTransactionId(),
        transaction.getWritePointer(),
        Arrays.hashCode(transaction.getInvalids()),
        Arrays.hashCode(transaction.getInProgress()),
        transaction.getFirstShortInProgress(),
        transaction.getType(),
        Arrays.hashCode(transaction.getCheckpointWritePointers()),
        transaction.getVisibilityLevel()
      );
    }
  }
}
