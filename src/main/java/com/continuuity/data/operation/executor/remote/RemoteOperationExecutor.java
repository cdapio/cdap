package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAdmin;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.GetQueueInfo;
import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

/**
 * An operation executor that delegates all operations to a remote
 * thrift service, which itself implements the operation executor
 * interface. This is used in distributed mode to decouple the
 * data fabric from the clients.
 */
public class RemoteOperationExecutor
    extends ConverterUtils
    implements OperationExecutor {

  private static final Logger Log =
      LoggerFactory.getLogger(RemoteOperationExecutor.class);

  // we will use this to provide every call with an opex client
  private OpexClientProvider clientProvider;

  // we will use this for getting clients for long-running operations
  private OpexClientProvider longClientProvider;

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
  public RemoteOperationExecutor(
      @Named("RemoteOperationExecutorConfig")CConfiguration config)
      throws TException { // TODO change this to throw OperationException

    // initialize the retry logic
    String retryStrat = config.get(
        Constants.CFG_DATA_OPEX_CLIENT_RETRY_STRATEGY,
        Constants.DEFAULT_DATA_OPEX_CLIENT_RETRY_STRATEGY);
    if ("backoff".equals(retryStrat)) {
      this.retryStrategyProvider = new RetryWithBackoff.Provider();
    } else if ("n-times".equals(retryStrat)) {
      this.retryStrategyProvider = new RetryNTimes.Provider();
    } else {
      String message = "Unknown Retry Strategy '" + retryStrat + "'.";
      Log.error(message);
      throw new TException(message);
    }
    this.retryStrategyProvider.configure(config);
    Log.info("Retry strategy is " + this.retryStrategyProvider);

    // configure the client provider
    String provider = config.get(Constants.CFG_DATA_OPEX_CLIENT_PROVIDER,
        Constants.DEFAULT_DATA_OPEX_CLIENT_PROVIDER);
    if ("pool".equals(provider)) {
      this.clientProvider = new PooledClientProvider(config);
    } else if ("thread-local".equals(provider)) {
      this.clientProvider = new ThreadLocalClientProvider(config);
    } else {
      String message = "Unknown Operation Service Client Provider '"
          + provider + "'.";
      Log.error(message);
      throw new TException(message);
    }
    this.clientProvider.initialize();
    Log.info("Opex client provider is " + this.clientProvider);

    // configure the client provider for long-running operations
    // for this we use a provider that creates a new connection every time,
    // and closes the connection after the call. The reason is that these
    // operations are very rare, and it is not worth keeping another pool of
    // open thrift connections around.
    int longTimeout = config.getInt(Constants.CFG_DATA_OPEX_CLIENT_LONG_TIMEOUT,
        Constants.DEFAULT_DATA_OPEX_CLIENT_LONG_TIMEOUT);
    this.longClientProvider = new SingleUseClientProvider(config, longTimeout);
    this.longClientProvider.initialize();
    Log.info("Opex client provider for long-runnig operations is "
        + this.longClientProvider);
  }

  /**
   * This is an abstract class that encapsulates an operation. It provides a
   * method to attempt the actual operation, and it can throw an operation
   * exception.
   * @param <T> The return type of the operation
   */
  abstract static class Operation<T> {

    /* the name of the operation */
    String name;

    /** constructor with name of operation */
    Operation(String name) {
      this.name = name;
    }

    /** return the name of the operation */
    String getName() {
      return name;
    }

    /** execute the operation, given an opex client */
    abstract T execute(OperationExecutorClient client)
        throws TException, OperationException;
  }

  /** see execute(operation, client) */
  private <T> T execute(Operation<T> operation) throws OperationException {
    return execute(operation, null);
  }

  /**
   * This is a generic method implementing the somewhat complex execution
   * and retry logic for operations, to avoid repetitive code.
   *
   * Attempts to execute one operation, by obtaining an opex client from
   * the client provider and passing the operation to the client. If the
   * call fails with a Thrift exception, apply the retry strategy. If no
   * more retries are to be made according to the strategy, call the
   * operation's error method to obtain a value to return. Note that error()
   * may also throw an exception. Note also that the retry logic is only
   * applied for thrift exceptions.
   *
   * @param operation The operation to be executed
   * @param provider An opex client provider. If null, then a client will be
   *                 obtained using the client provider
   * @param <T> The return type of the operation
   * @return the result of the operation, or a value returned by error()
   * @throws OperationException if the operation fails with an exception
   *    other than TException
   */
  private <T> T execute(Operation<T> operation, OpexClientProvider provider)
  throws OperationException {
    RetryStrategy retryStrategy = retryStrategyProvider.newRetryStrategy();
    while (true) {
      // did we get a custom client provider or do we use the default?
      if (provider == null) {
        provider = this.clientProvider;
      }
      OperationExecutorClient client = null;
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
          Log.error(message, te);
          throw new OperationException(StatusCode.THRIFT_ERROR, message, te);
        } else {
          // call retry strategy before retrying
          retryStrategy.beforeRetry();
          Log.info("Retrying " + operation.getName() + " after Thrift error.");
        }

      } finally {
        // in case any other exception happens (other than TException), and
        // also in case of succeess, the client must be returned to the pool.
        if (client != null)
          provider.returnClient(client);
      }
    }
  }

  @Override
  public void commit(final OperationContext context, final List<WriteOperation> writes)
      throws OperationException {
    this.execute(
        new Operation<Boolean>("Batch") {
          @Override
          public Boolean execute(OperationExecutorClient client)
              throws OperationException, TException {
            client.execute(context, writes);
            return true;
          }
        });
  }

  @Override
  public Transaction startTransaction(OperationContext context)
    throws OperationException {
    // TODO implement this properly
    return null;
  }

  @Override
  public Transaction execute(final OperationContext context,
                             final Transaction transaction,
                             final List<WriteOperation> writes)
    throws OperationException {
    // TODO implement this properly
    commit(context, writes);
    return null;
  }

  @Override
  public void commit(OperationContext context,
                     Transaction transaction)
    throws OperationException {
    // TODO implement this properly
  }

  @Override
  public void commit(OperationContext context,
                     Transaction transaction,
                     List<WriteOperation> writes)
    throws OperationException {
    // TODO implement this properly
    commit(context, writes);
  }

  @Override
  public void abort(OperationContext context,
                    Transaction transaction)
    throws OperationException {
    // TODO implement this properly
  }

  @Override
  public Map<byte[], Long> increment(OperationContext context, Increment increment)
    throws OperationException {
    // TODO implement this properly
    commit(context, increment);
    return Collections.EMPTY_MAP;
  }

  @Override
  public Map<byte[], Long> increment(OperationContext context,
                                     Transaction transaction,
                                                      Increment increment)
    throws OperationException {
    // TODO implement this properly
    execute(context, transaction, Collections.singletonList((WriteOperation)increment));
    return Collections.EMPTY_MAP;
  }

  @Override
  public DequeueResult execute(final OperationContext context,
                               final QueueDequeue dequeue)
      throws OperationException {
    return this.execute(
        new Operation<DequeueResult>("DequeuePayload") {
          @Override
          public DequeueResult execute(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(context, dequeue);
          }
        });
  }

  @Override
  public long execute(final OperationContext context,
                      final QueueAdmin.GetGroupID getGroupID)
      throws OperationException {
    return this.execute(
        new Operation<Long>("GetGroupID") {
          @Override
          public Long execute(OperationExecutorClient client)
              throws TException, OperationException {
            return client.execute(context, getGroupID);
          }
        });
  }

  @Override
  public OperationResult<QueueInfo> execute(final OperationContext context,
                                            final GetQueueInfo getQueueInfo)
      throws OperationException {
    return this.execute(new Operation<
            OperationResult<QueueInfo>>("GetQueueInfo") {
          @Override
          public OperationResult<QueueInfo>
          execute(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(context, getQueueInfo);
          }
        }, this.longClientProvider);
  }

  @Override
  public void execute(final OperationContext context,
                      final ClearFabric clearFabric)
      throws OperationException {
    this.execute(
        new Operation<Boolean>("ClearFabric") {
          @Override
          public Boolean execute(OperationExecutorClient client)
              throws TException, OperationException {
            client.execute(context, clearFabric);
            return true;
          }
        }, this.longClientProvider);
  }

  @Override
  public void execute(final OperationContext context,
                      final OpenTable openTable) throws OperationException {
    this.execute(
        new Operation<Boolean>("OpenTable") {
          @Override
          public Boolean execute(OperationExecutorClient client)
              throws TException, OperationException {
            client.execute(context, openTable);
            return true;
          }
        });
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  execute(final OperationContext context,
          final Read read)
      throws OperationException {
    return this.execute(
        new Operation<OperationResult<Map<byte[],byte[]>>>("Read") {
          @Override
          public OperationResult<Map<byte[], byte[]>>
          execute(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(context, read);
          }
        });
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Transaction transaction,
                                                      Read read)
    throws OperationException {
    // TODO implement this properly
    return execute(context, read);
  }

  @Override
  public OperationResult<List<byte[]>>
  execute(final OperationContext context,
          final ReadAllKeys readAllKeys)
      throws OperationException {
    return this.execute(
        new Operation<OperationResult<List<byte[]>>>("ReadAllKeys") {
          @Override
          public OperationResult<List<byte[]>>
          execute(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(context, readAllKeys);
          }
        });
  }

  @Override
  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               Transaction transaction,
                                               ReadAllKeys readKeys)
    throws OperationException {
    // TODO implement this properly
    return execute(context, readKeys);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  execute(final OperationContext context,
          final ReadColumnRange readColumnRange)
      throws OperationException {
    return this.execute(new Operation<
            OperationResult<Map<byte[],byte[]>>>("ReadColumnRange") {
          @Override
          public OperationResult<Map<byte[], byte[]>>
          execute(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(context, readColumnRange);
          }
        });
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Transaction transaction,
                                                      ReadColumnRange readColumnRange)
    throws OperationException {
    // TODO implement this properly
    return execute(context, readColumnRange);
  }

  @Override
  public void commit(final OperationContext context, final WriteOperation write)
      throws OperationException {
    this.commit(context, Collections.singletonList(write));
  }

  @Override
  public String getName() {
    return "remote";
  }
}
