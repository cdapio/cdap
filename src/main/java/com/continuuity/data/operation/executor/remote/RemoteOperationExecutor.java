package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.*;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

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
  }

  /**
   * This is an abstract class that encapsulates an operation. It provides a
   * method to attempt the actual operation, and it can throw an operation
   * exception.
   * @param <T> The return type of the operation
   */
  abstract static class Opexeptionable <T> {

    /* the name of the operation */
    String operation;

    /** constructor with name of operation */
    Opexeptionable(String operation) {
      this.operation = operation;
    }

    /** return the name of the operation */
    String getName() {
      return operation;
    }

    /** execute the operation, given an opex client */
    abstract T call(OperationExecutorClient client)
        throws TException, OperationException;

    /** produce a return value in the case of a thrift exception */
    T error(TException te) throws OperationException {
      String message =
          "Thrift error for " + operation + ": " + te.getMessage();
      Log.error(message, te);
      throw new OperationException(StatusCode.THRIFT_ERROR, message, te);
    }
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
   * @param <T> The return type of the operation
   * @return the result of the operation, or a value returned by error()
   * @throws OperationException if the operation fails with an exception
   *    other than TException
   */
  private <T> T execute(Opexeptionable<T> operation)
      throws OperationException {
    RetryStrategy retryStrategy = retryStrategyProvider.newRetryStrategy();
    while (true) {
      OperationExecutorClient client = null;
      try {
        // this will throw a TException if it cannot get a client
        client = this.clientProvider.getClient();

        // note that this can throw exceptions other than TException
        // hence the finally clause at the end
        return operation.call(client);

      } catch (TException te) {
        // a thrift error occurred, the thrift client may be in a bad state
        if (client != null) {
          this.clientProvider.discardClient(client);
          client = null;
        }

        // determine whether we should retry
        boolean retry = retryStrategy.failOnce();
        if (!retry) {
          // retry strategy is exceeded, return an error
          // note that this can throw an exception of type E
          return operation.error(te);
        } else {
          // call retry strategy before retrying
          retryStrategy.beforeRetry();
          Log.info("Retrying " + operation.getName() + " after Thrift error.");
        }

      } finally {
        // in case any other exception happens (other than TException), and
        // also in case of succeess, the client must be returned to the pool.
        if (client != null)
          this.clientProvider.returnClient(client);
      }
    }
  }

  @Override
  public void execute(final List<WriteOperation> writes)
      throws OperationException {
    this.execute(
        new Opexeptionable<Boolean>("Batch") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws OperationException, TException {
            client.execute(writes);
            return true;
          }
        });
  }

  @Override
  public DequeueResult execute(final QueueDequeue dequeue)
      throws OperationException {
    return this.execute(
        new Opexeptionable<DequeueResult>("Dequeue") {
          @Override
          public DequeueResult call(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(dequeue);
          }
        });
  }

  @Override
  public long execute(final QueueAdmin.GetGroupID getGroupID)
      throws OperationException {
    return this.execute(
        new Opexeptionable<Long>("GetGroupID") {
          @Override
          public Long call(OperationExecutorClient client)
              throws TException, OperationException {
            return client.execute(getGroupID);
          }
        });
  }

  @Override
  public OperationResult<QueueAdmin.QueueMeta>
  execute(final QueueAdmin.GetQueueMeta getQM) throws OperationException {
    return this.execute(new Opexeptionable<
        OperationResult<QueueAdmin.QueueMeta>>("GetQueueMeta") {
          @Override
          public OperationResult<QueueAdmin.QueueMeta>
          call(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(getQM);
          }
        });
  }

  @Override
  public void execute(final ClearFabric clearFabric) throws OperationException {
    this.execute(
        new Opexeptionable<Boolean>("ClearFabric") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException, OperationException {
            client.execute(clearFabric);
            return true;
          }
        });
  }

  @Override
  public OperationResult<byte[]> execute(final ReadKey readKey)
      throws OperationException {
    return this.execute(
        new Opexeptionable<OperationResult<byte[]>>("ReadKey") {
          @Override
          public OperationResult<byte[]> call(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(readKey);
          }
        });
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(final Read read)
      throws OperationException {
    return this.execute(
        new Opexeptionable<OperationResult<Map<byte[],byte[]>>>("Read") {
          @Override
          public OperationResult<Map<byte[], byte[]>>
          call(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(read);
          }
        });
  }

  @Override
  public OperationResult<List<byte[]>> execute(final ReadAllKeys readAllKeys)
      throws OperationException {
    return this.execute(
        new Opexeptionable<OperationResult<List<byte[]>>>("ReadAllKeys") {
          @Override
          public OperationResult<List<byte[]>>
          call(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(readAllKeys);
          }
        });
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  execute(final ReadColumnRange readColumnRange)
      throws OperationException {
    return this.execute(new Opexeptionable<
        OperationResult<Map<byte[],byte[]>>>("ReadColumnRange") {
          @Override
          public OperationResult<Map<byte[], byte[]>>
          call(OperationExecutorClient client)
              throws OperationException, TException {
            return client.execute(readColumnRange);
          }
        });
  }

  @Override
  public void execute(final Write write) throws OperationException {
    this.execute(
        new Opexeptionable<Boolean>("Write") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException, OperationException {
            client.execute(write);
            return true;
          }
        });
  }

  @Override
  public void execute(final Delete delete) throws OperationException {
    this.execute(
        new Opexeptionable<Boolean>("Delete") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException, OperationException {
            client.execute(delete);
            return true;
          }
        });
  }

  @Override
  public void execute(final Increment increment) throws OperationException {
    this.execute(
        new Opexeptionable<Boolean>("Increment") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException, OperationException {
            client.execute(increment);
            return true;
          }
        });
  }

  @Override
  public void execute(final CompareAndSwap compareAndSwap)
      throws OperationException {
    this.execute(
        new Opexeptionable<Boolean>("CompareAndSwap") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException, OperationException {
            client.execute(compareAndSwap);
            return true;
          }
        });
  }

  @Override
  public void execute(final QueueEnqueue enqueue) throws OperationException {
    this.execute(
        new Opexeptionable<Boolean>("Enqueue") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException, OperationException {
            client.execute(enqueue);
            return true;
          }
        });
  }

  @Override
  public void execute(final QueueAck ack) throws OperationException {
    this.execute(
        new Opexeptionable<Boolean>("Ack") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException, OperationException {
            client.execute(ack);
            return true;
          }
        });
  }

  @Override
  public String getName() {
    return "remote";
  }
}
