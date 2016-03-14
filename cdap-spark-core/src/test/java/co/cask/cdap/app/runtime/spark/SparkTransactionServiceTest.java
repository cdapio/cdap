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
import co.cask.tephra.TransactionCodec;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import co.cask.tephra.persist.TransactionSnapshot;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
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
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Unit tests for the {@link SparkTransactionService}.
 */
public class SparkTransactionServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTransactionServiceTest.class);
  private static final TransactionCodec TX_CODEC = new TransactionCodec();

  private static TransactionManager txManager;
  private static TransactionSystemClient txClient;
  private static SparkTransactionService sparkTxService;

  @BeforeClass
  public static void init() {
    txManager = new TransactionManager(new Configuration());
    txManager.startAndWait();

    txClient = new InMemoryTxSystemClient(txManager);

    sparkTxService = new SparkTransactionService(txClient);
    sparkTxService.startAndWait();
  }

  @AfterClass
  public static void finish() {
    sparkTxService.stopAndWait();
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
  public void testFailureTransaction() throws IOException {
    TransactionManager txManager = new TransactionManager(new Configuration()) {
      @Override
      public Transaction startLong() {
        throw new IllegalStateException("Cannot start long transaction");
      }
    };
    txManager.startAndWait();
    try {
      SparkTransactionService sparkTxService = new SparkTransactionService(new InMemoryTxSystemClient(txManager));
      sparkTxService.startAndWait();
      try {
        // Start a job
        sparkTxService.jobStarted(1, ImmutableSet.of(2));

        // Make a call to the stage transaction endpoint, it should get a 410 Gone response.
        HttpURLConnection urlConn = open(sparkTxService.getBaseURI().resolve("/spark/stages/2/transaction"));
        try {
          Assert.assertEquals(HttpResponseStatus.GONE.getCode(), urlConn.getResponseCode());
        } finally {
          urlConn.disconnect();
        }

        // End the job
        sparkTxService.jobEnded(1, false);
      } finally {
        sparkTxService.stopAndWait();
      }
    } finally {
      txManager.stopAndWait();
    }
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
                          @Nullable Transaction explicitTransaction) throws Exception {
    URI baseURI = sparkTxService.getBaseURI().resolve("/spark/stages/");

    // Before job start, getting transaction should return 404
    verifyStagesTransactions(stages, baseURI, new Verifier() {
      @Override
      public boolean verify(HttpURLConnection urlConn) throws Exception {
        return HttpResponseStatus.NOT_FOUND.getCode() == urlConn.getResponseCode();
      }
    });

    // Now start the job
    if (explicitTransaction == null) {
      sparkTxService.jobStarted(jobId, stages);
    } else {
      sparkTxService.jobStarted(jobId, stages, explicitTransaction);
    }

    // For all stages, it should get 200 with the same transaction returned
    final Set<Transaction> transactions = Collections.newSetFromMap(new ConcurrentHashMap<Transaction, Boolean>());
    verifyStagesTransactions(stages, baseURI, new Verifier() {
      @Override
      public boolean verify(HttpURLConnection urlConn) throws Exception {
        Transaction transaction = TX_CODEC.decode(ByteStreams.toByteArray(urlConn.getInputStream()));
        transactions.add(new TransactionWrapper(transaction));
        return HttpResponseStatus.OK.getCode() == urlConn.getResponseCode();
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
    sparkTxService.jobEnded(jobId, jobSucceeded);

    // For all stages, it should get a 404 Not found again after the job completed
    verifyStagesTransactions(stages, baseURI, new Verifier() {
      @Override
      public boolean verify(HttpURLConnection urlConn) throws Exception {
        return HttpResponseStatus.NOT_FOUND.getCode() == urlConn.getResponseCode();
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
   * Opens a {@link HttpURLConnection} from the {@link URL} created from the given {@link URI}.
   */
  private HttpURLConnection open(URI uri) throws IOException {
    return (HttpURLConnection) uri.toURL().openConnection();
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
   * Verifies the result of get stage transaction endpoint for the given set of stages.
   * The get transaction endpoint will be hit concurrently for all stages.
   *
   * @param stages set of stages to verify
   * @param baseURI the base URI for constructing the complete endpoint
   * @param verifier a {@link Verifier} to verify the http call result.
   */
  private void verifyStagesTransactions(Set<Integer> stages, final URI baseURI,
                                        final Verifier verifier) throws Exception {
    final CyclicBarrier barrier = new CyclicBarrier(stages.size());
    final ExecutorService executor = Executors.newFixedThreadPool(stages.size());
    try {
      CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
      for (final int stageId : stages) {
        completionService.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            barrier.await();
            HttpURLConnection urlConn = open(baseURI.resolve(stageId + "/transaction"));
            try {
              return verifier.verify(urlConn);
            } finally {
              urlConn.disconnect();
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

  private interface Verifier {
    /**
     * Verifies the given http call.
     */
    boolean verify(HttpURLConnection urlConn) throws Exception;
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
