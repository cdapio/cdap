/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.distributed;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSizeException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.runtime.ConfigModule;
import org.apache.tephra.runtime.DiscoveryModules;
import org.apache.tephra.runtime.TransactionClientModule;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.tephra.runtime.ZKModule;
import org.apache.tephra.util.ConfigurationFactory;
import org.apache.thrift.TException;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * A tx service client
 */
public class TransactionServiceClient implements TransactionSystemClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(TransactionServiceClient.class);

  // we will use this to provide every call with an tx client
  private ThriftClientProvider clientProvider;

  // the retry strategy we will use
  private final RetryStrategyProvider retryStrategyProvider;

  // client id that is used to identify the transactions
  private final String clientId;

  private final int changeSetCountLimit;
  private final int changeSetCountThreshold;
  private final long changeSetSizeLimit;
  private final long changeSetSizeThreshold;

  /**
   * Utility to be used for basic verification of transaction system availability and functioning
   * @param args arguments list, accepts single option "-v" that makes it to print out more details about started tx
   */
  public static void main(String[] args) throws Exception {
    if (args.length > 1 || (args.length == 1 && !"-v".equals(args[0]))) {
      System.out.println("USAGE: TransactionServiceClient [-v]");
    }

    boolean verbose = false;
    if (args.length == 1 && "-v".equals(args[0])) {
      verbose = true;
    }
    doMain(verbose, new ConfigurationFactory().get());
  }

  @VisibleForTesting
  public static void doMain(boolean verbose, Configuration conf) throws Exception {
    LOG.info("Starting tx server client test.");
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new ZKModule(),
      new DiscoveryModules().getDistributedModules(),
      new TransactionModules().getDistributedModules(),
      new TransactionClientModule()
    );

    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAsync().awaitRunning();

    try {
      TransactionServiceClient client = injector.getInstance(TransactionServiceClient.class);
      LOG.info("Starting tx...");
      Transaction tx = client.startShort();
      if (verbose) {
        LOG.info("Started tx details: " + tx.toString());
      } else {
        LOG.info("Started tx: " + tx.getTransactionId() +
                   ", readPointer: " + tx.getReadPointer() +
                   ", invalids: " + tx.getInvalids().length +
                   ", inProgress: " + tx.getInProgress().length);
      }
      try {
        LOG.info("Checking if canCommit tx...");
        client.canCommitOrThrow(tx, Collections.<byte[]>emptyList());
        LOG.info("canCommit: success");
        LOG.info("Committing tx...");
        client.commitOrThrow(tx);
        LOG.info("Committed tx: success");
      } catch (TransactionConflictException e) {
        LOG.info("Aborting tx...");
        client.abort(tx);
        LOG.info("Aborted tx...");
      }
    } finally {
      zkClient.stopAsync().awaitTerminated();
    }
  }

  /**
   * Create from a configuration. This will first attempt to find a zookeeper for service discovery.
   * This constructor can be used if Guice dependency injection is not used. JVM name will be used for the client id.
   * @param config a configuration containing the zookeeper properties
   */
  public TransactionServiceClient(Configuration config, ThriftClientProvider clientProvider) {
    this(config, clientProvider, ManagementFactory.getRuntimeMXBean().getName());
  }

  /**
   * Create from a configuration. This will first attempt to find a zookeeper
   * for service discovery. Otherwise it will look for the port in the
   * config and use localhost.
   * @param config a configuration containing the zookeeper properties
   * @param clientId id of the client that identifies it when it starts a transaction
   */
  @Inject
  public TransactionServiceClient(Configuration config, ThriftClientProvider clientProvider,
                                  @Named(TxConstants.CLIENT_ID) String clientId) {

    // initialize the retry logic
    String retryStrat = config.get(
        TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY,
        TxConstants.Service.DEFAULT_DATA_TX_CLIENT_RETRY_STRATEGY);
    if ("backoff".equals(retryStrat)) {
      this.retryStrategyProvider = new RetryWithBackoff.Provider();
    } else if ("n-times".equals(retryStrat)) {
      this.retryStrategyProvider = new RetryNTimes.Provider();
    } else {
      try {
        this.retryStrategyProvider = (RetryStrategyProvider) Class.forName(retryStrat).newInstance();
      } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
        throw new IllegalArgumentException(
          String.format("Unable to instantiate RetryStrategyProvider '%s'", retryStrat), e);
      }
    }
    this.retryStrategyProvider.configure(config);
    LOG.debug("Retry strategy is " + this.retryStrategyProvider);

    this.clientProvider = clientProvider;
    this.clientId = clientId;

    changeSetCountLimit = config.getInt(TxConstants.Manager.CFG_TX_CHANGESET_COUNT_LIMIT,
                                        TxConstants.Manager.DEFAULT_TX_CHANGESET_COUNT_LIMIT);
    changeSetCountThreshold = config.getInt(TxConstants.Manager.CFG_TX_CHANGESET_COUNT_WARN_THRESHOLD,
                                            TxConstants.Manager.DEFAULT_TX_CHANGESET_COUNT_WARN_THRESHOLD);
    changeSetSizeLimit = config.getLong(TxConstants.Manager.CFG_TX_CHANGESET_SIZE_LIMIT,
                                        TxConstants.Manager.DEFAULT_TX_CHANGESET_SIZE_LIMIT);
    changeSetSizeThreshold = config.getLong(TxConstants.Manager.CFG_TX_CHANGESET_SIZE_WARN_THRESHOLD,
                                            TxConstants.Manager.DEFAULT_TX_CHANGESET_SIZE_WARN_THRESHOLD);
  }

  /**
   * This is an abstract class that encapsulates an operation. It provides a
   * method to attempt the actual operation, and it can throw an operation
   * exception.
   * @param <T> The return type of the operation
   */
  abstract static class Operation<T> {

    /** the name of the operation. */
    String name;

    /** constructor with name of operation. */
    Operation(String name) {
      this.name = name;
    }

    /** return the name of the operation. */
    String getName() {
      return name;
    }

    /** execute the operation, given an tx client. */
    abstract T execute(TransactionServiceThriftClient client)
        throws Exception;
  }

  /** see execute(operation, client). */
  private <T> T execute(Operation<T> operation) throws Exception {
    return execute(operation, null);
  }

  /**
   * This is a generic method implementing the somewhat complex execution
   * and retry logic for operations, to avoid repetitive code.
   *
   * Attempts to execute one operation, by obtaining an tx client from
   * the client provider and passing the operation to the client. If the
   * call fails with a Thrift exception, apply the retry strategy. If no
   * more retries are to be made according to the strategy, call the
   * operation's error method to obtain a value to return. Note that error()
   * may also throw an exception. Note also that the retry logic is only
   * applied for thrift exceptions.
   *
   * @param operation The operation to be executed
   * @param provider An tx client provider. If null, then a client will be
   *                 obtained using the client provider
   * @param <T> The return type of the operation
   * @return the result of the operation, or a value returned by error()
   */
  private <T> T execute(Operation<T> operation, ThriftClientProvider provider) throws Exception {
    RetryStrategy retryStrategy = retryStrategyProvider.newRetryStrategy();
    while (true) {
      // did we get a custom client provider or do we use the default?
      if (provider == null) {
        provider = this.clientProvider;
      }
      // this will throw a TException if it cannot get a client
      try (CloseableThriftClient closeable = provider.getCloseableClient()) {
        // note that this can throw exceptions other than TException
        return operation.execute(closeable.getThriftClient());

      } catch (TException te) {
        // determine whether we should retry
        boolean retry = retryStrategy.failOnce();
        if (!retry) {
          // retry strategy is exceeded, throw an operation exception
          String message =
              "Thrift error for " + operation + ": " + te.getMessage();
          LOG.error(message);
          LOG.debug(message, te);
          throw te;
        } else {
          // call retry strategy before retrying
          retryStrategy.beforeRetry();
          String msg = "Retrying " + operation.getName() + " after Thrift error: " + te.getMessage();
          LOG.info(msg);
          LOG.debug(msg, te);
        }
      }
    }
  }

  @Override
  public Transaction startLong() {
    try {
      return execute(
        new Operation<Transaction>("startLong") {
          @Override
          public Transaction execute(TransactionServiceThriftClient client)
            throws TException {
            return client.startLong(clientId);
          }
        });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Transaction startShort() {
    try {
      return execute(
        new Operation<Transaction>("startShort") {
          @Override
          public Transaction execute(TransactionServiceThriftClient client)
            throws TException {
            return client.startShort(clientId);
          }
        });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Transaction startShort(final int timeout) {
    try {
      return execute(
        new Operation<Transaction>("startShort") {
          @Override
          public Transaction execute(TransactionServiceThriftClient client)
            throws TException {
            return client.startShort(clientId, timeout);
          }
        });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean canCommit(final Transaction tx, final Collection<byte[]> changeIds)
    throws TransactionNotInProgressException {
    try {
      canCommitOrThrow(tx, changeIds);
      return true;
    } catch (TransactionFailureException e) {
      return false;
    }
  }

  @Override
  public void canCommitOrThrow(final Transaction tx, final Collection<byte[]> changeIds)
    throws TransactionFailureException {

    // we want to validate the size of the change set here before sending it over the wire.
    // if the change set is large, it can cause memory issues on the server side.
    if (changeIds.size() > changeSetCountLimit) {
      throw new TransactionSizeException(String.format(
        "Change set for transaction %d has %d entries and exceeds the limit of %d",
        tx.getTransactionId(), changeIds.size(), changeSetCountLimit));
    } else if (changeIds.size() > changeSetCountThreshold) {
      LOG.warn("Change set for transaction {} has {} entries. " +
                 "It is recommended to limit the number of changes to {}, or to use a long-running transaction. ",
               tx.getTransactionId(), changeIds.size(), changeSetCountThreshold);
    }
    long byteCount = 0L;
    for (byte[] change : changeIds) {
      byteCount += change.length;
    }
    if (byteCount > changeSetSizeLimit) {
      throw new TransactionSizeException(String.format(
        "Change set for transaction %d has total size of %d bytes and exceeds the limit of %d bytes",
        tx.getTransactionId(), byteCount, changeSetSizeLimit));
    } else if (byteCount > changeSetSizeThreshold) {
      LOG.warn("Change set for transaction {} has total size of {} bytes. " +
                 "It is recommended to limit the total size to {} bytes, or to use a long-running transaction. ",
               tx.getTransactionId(), byteCount, changeSetSizeThreshold);
    }

    try {
      execute(
        new Operation<Void>("canCommit") {
          @Override
          public Void execute(TransactionServiceThriftClient client)
            throws Exception {
            client.canCommit(tx, changeIds);
            return null;
          }
        });
    } catch (TransactionNotInProgressException | TransactionSizeException | TransactionConflictException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean commit(final Transaction tx) throws TransactionNotInProgressException {
    try {
      commitOrThrow(tx);
      return true;
    } catch (TransactionFailureException e) {
      return false;
    }
  }

  @Override
  public void commitOrThrow(final Transaction tx)
    throws TransactionFailureException {
    try {
      execute(
        new Operation<Void>("commit") {
          @Override
          public Void execute(TransactionServiceThriftClient client) throws Exception {
            client.commit(tx.getTransactionId(), tx.getWritePointer());
            return null;
          }
        });
    } catch (TransactionNotInProgressException | TransactionConflictException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void abort(final Transaction tx) {
    try {
      this.execute(
        new Operation<Boolean>("abort") {
          @Override
          public Boolean execute(TransactionServiceThriftClient client)
            throws TException {
            client.abort(tx);
            return true;
          }
        });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean invalidate(final long tx) {
    try {
      return this.execute(
        new Operation<Boolean>("invalidate") {
          @Override
          public Boolean execute(TransactionServiceThriftClient client)
            throws TException {
            return client.invalidate(tx);
          }
        });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Transaction checkpoint(final Transaction tx) throws TransactionNotInProgressException {
    try {
      return this.execute(
          new Operation<Transaction>("checkpoint") {
            @Override
            Transaction execute(TransactionServiceThriftClient client) throws Exception {
              return client.checkpoint(tx);
            }
          }
      );
    } catch (TransactionNotInProgressException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    try {
      return this.execute(
          new Operation<InputStream>("takeSnapshot") {
            @Override
            public InputStream execute(TransactionServiceThriftClient client)
                throws Exception {
              return client.getSnapshotStream();
            }
          });
    } catch (TransactionCouldNotTakeSnapshotException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String status() {
    try {
      return this.execute(
        new Operation<String>("status") {
          @Override
          public String execute(TransactionServiceThriftClient client) throws Exception {
            return client.status();
          }
        });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void resetState() {
    try {
      this.execute(
          new Operation<Boolean>("resetState") {
            @Override
            public Boolean execute(TransactionServiceThriftClient client)
                throws TException {
              client.resetState();
              return true;
            }
          });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean truncateInvalidTx(final Set<Long> invalidTxIds) {
    try {
      return this.execute(
        new Operation<Boolean>("truncateInvalidTx") {
          @Override
          public Boolean execute(TransactionServiceThriftClient client) throws TException {
            return client.truncateInvalidTx(invalidTxIds);
          }
        });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean truncateInvalidTxBefore(final long time) throws InvalidTruncateTimeException {
    try {
      return this.execute(
        new Operation<Boolean>("truncateInvalidTxBefore") {
          @Override
          public Boolean execute(TransactionServiceThriftClient client) throws Exception {
            return client.truncateInvalidTxBefore(time);
          }
        });
    } catch (InvalidTruncateTimeException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getInvalidSize() {
    try {
      return this.execute(
        new Operation<Integer>("getInvalidSize") {
          @Override
          public Integer execute(TransactionServiceThriftClient client) throws TException {
            return client.getInvalidSize();
          }
        });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void pruneNow() {
    try {
      this.execute(
        new Operation<Void>("pruneNow") {
          @Override
          public Void execute(TransactionServiceThriftClient client)
            throws TException {
            client.pruneNow();
            return null;
          }
        });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

}
