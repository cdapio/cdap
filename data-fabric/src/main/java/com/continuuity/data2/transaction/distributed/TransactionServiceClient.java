package com.continuuity.data2.transaction.distributed;

import com.continuuity.data2.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * A tx service client
 */
public class TransactionServiceClient implements TransactionSystemClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(TransactionServiceClient.class);

  // we will use this to provide every call with an tx client
  private ThriftClientProvider clientProvider;

  // the retry strategy we will use
  RetryStrategyProvider retryStrategyProvider;

  /**
   * Create from a configuration. This will first attempt to find a zookeeper
   * for service discovery. Otherwise it will look for the port in the
   * config and use localhost.
   * @param config a configuration containing the zookeeper properties
   * @throws TException
   */
  @Inject
  public TransactionServiceClient(@Named("TransactionServerClientConfig") CConfiguration config) throws TException {

    // initialize the retry logic
    String retryStrat = config.get(
        Constants.Transaction.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY,
        Constants.Transaction.Service.DEFAULT_DATA_TX_CLIENT_RETRY_STRATEGY);
    if ("backoff".equals(retryStrat)) {
      this.retryStrategyProvider = new RetryWithBackoff.Provider();
    } else if ("n-times".equals(retryStrat)) {
      this.retryStrategyProvider = new RetryNTimes.Provider();
    } else {
      String message = "Unknown Retry Strategy '" + retryStrat + "'.";
      LOG.error(message);
      throw new TException(message);
    }
    this.retryStrategyProvider.configure(config);
    LOG.info("Retry strategy is " + this.retryStrategyProvider);

    // configure the client provider
    String provider = config.get(Constants.Transaction.Service.CFG_DATA_TX_CLIENT_PROVIDER,
        Constants.Transaction.Service.DEFAULT_DATA_TX_CLIENT_PROVIDER);
    if ("pool".equals(provider)) {
      this.clientProvider = new PooledClientProvider(config);
    } else if ("thread-local".equals(provider)) {
      this.clientProvider = new ThreadLocalClientProvider(config);
    } else {
      String message = "Unknown Operation Service Client Provider '"
          + provider + "'.";
      LOG.error(message);
      throw new TException(message);
    }
    this.clientProvider.initialize();
    LOG.info("Tx client provider is " + this.clientProvider);

    // configure the client provider for long-running operations
    // for this we use a provider that creates a new connection every time,
    // and closes the connection after the call. The reason is that these
    // operations are very rare, and it is not worth keeping another pool of
    // open thrift connections around.
    int longTimeout = config.getInt(Constants.Transaction.Service.CFG_DATA_TX_CLIENT_LONG_TIMEOUT,
        Constants.Transaction.Service.DEFAULT_DATA_TX_CLIENT_LONG_TIMEOUT);
    ThriftClientProvider longClientProvider = new SingleUseClientProvider(config, longTimeout);
    longClientProvider.initialize();
    LOG.info("Tx client provider for long-runnig operations is "
               + longClientProvider);
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
        throws TException;
  }

  /** see execute(operation, client). */
  private <T> T execute(Operation<T> operation) {
    try {
      return execute(operation, null);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
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
   * @throws OperationException if the operation fails with an exception
   *    other than TException
   */
  private <T> T execute(Operation<T> operation, ThriftClientProvider provider) throws Exception {
    RetryStrategy retryStrategy = retryStrategyProvider.newRetryStrategy();
    while (true) {
      // did we get a custom client provider or do we use the default?
      if (provider == null) {
        provider = this.clientProvider;
      }
      TransactionServiceThriftClient client = null;
      try {
        // this will throw a TException if it cannot get a client
        client = provider.getClient();

        // note that this can throw exceptions other than TException
        // hence the finally clause at the end
        return operation.execute(client);

      } catch (TException te) {
        // a thrift error occurred, the thrift client may be in a bad state
        if (client != null) {
          provider.discardClient(client);
          client = null;
        }

        // determine whether we should retry
        boolean retry = retryStrategy.failOnce();
        if (!retry) {
          // retry strategy is exceeded, throw an operation exception
          String message =
              "Thrift error for " + operation + ": " + te.getMessage();
          LOG.error(message, te);
          throw new Exception(message, te);
        } else {
          // call retry strategy before retrying
          retryStrategy.beforeRetry();
          LOG.info("Retrying " + operation.getName() + " after Thrift error.", te);
        }

      } finally {
        // in case any other exception happens (other than TException), and
        // also in case of succeess, the client must be returned to the pool.
        if (client != null) {
          provider.returnClient(client);
        }
      }
    }
  }

  @Override
  public com.continuuity.data2.transaction.Transaction startLong() {
    return this.execute(
      new Operation<com.continuuity.data2.transaction.Transaction>("startLong") {
        @Override
        public Transaction execute(TransactionServiceThriftClient client)
          throws TException {
          return client.startLong();
        }
      });
  }

  @Override
  public Transaction startShort() {
    return this.execute(
      new Operation<Transaction>("startShort") {
        @Override
        public Transaction execute(TransactionServiceThriftClient client)
          throws TException {
          return client.startShort();
        }
      });
  }

  @Override
  public Transaction startShort(final int timeout) {
    return this.execute(
      new Operation<Transaction>("startShort") {
        @Override
        public Transaction execute(TransactionServiceThriftClient client)
          throws TException {
          return client.startShort(timeout);
        }
      });
  }

  @Override
  public boolean canCommit(final Transaction tx, final Collection<byte[]> changeIds) {
    return this.execute(
      new Operation<Boolean>("canCommit") {
        @Override
        public Boolean execute(TransactionServiceThriftClient client)
          throws TException {
          return client.canCommit(tx, changeIds);
        }
      });
  }

  @Override
  public boolean commit(final Transaction tx) {
    return this.execute(
      new Operation<Boolean>("commit") {
        @Override
        public Boolean execute(TransactionServiceThriftClient client)
          throws TException {
          return client.commit(tx);
        }
      });
  }

  @Override
  public void abort(final Transaction tx) {
    this.execute(
      new Operation<Boolean>("abort") {
        @Override
        public Boolean execute(TransactionServiceThriftClient client)
          throws TException {
          client.abort(tx);
          return true;
        }
      });
  }

  @Override
  public void invalidate(final Transaction tx) {
    this.execute(
      new Operation<Boolean>("invalidate") {
        @Override
        public Boolean execute(TransactionServiceThriftClient client)
          throws TException {
          client.invalidate(tx);
          return true;
        }
      });
  }

}
