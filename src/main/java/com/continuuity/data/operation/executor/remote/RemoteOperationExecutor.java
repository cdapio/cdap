package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.BatchOperationException;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.*;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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
   * @throws IOException
   */
  @Inject
  public RemoteOperationExecutor(
      @Named("RemoteOperationExecutorConfig")CConfiguration config)
      throws IOException {

    // initialize the retry logic
    int numAttempts = config.getInt(
        Constants.CFG_DATA_OPEX_CLIENT_ATTEMPTS,
        Constants.DEFAULT_DATA_OPEX_CLIENT_ATTEMPTS);
    this.retryStrategyProvider = new RetryNTimes.Provider(numAttempts);
    Log.info("Retry strategy is " + this.retryStrategyProvider);

    // configure the client provider
    String provider = config.get(Constants.CFG_DATA_OPEX_CLIENT_PROVIDER,
        Constants.DEFAULT_DATA_OPEX_CLIENT_PROVIDER);
    if ("pool".equals(provider)) {
      Log.info("Using pooled operation service client provider");
      this.clientProvider = new PooledClientProvider(config);
    } else if ("thread-local".equals(provider)) {
      Log.info("Using thread-local operation service client provider");
      this.clientProvider = new ThreadLocalClientProvider(config);
    } else {
      Log.error("Unknown Operation Service Client Provider '"
          + provider + "'.");
      throw new IOException("Unknown  Operation Service Client Provider '"
          + provider + "'.");
    }
    this.clientProvider.initialize();
  }

  /**
   * This is an abstract class that encapsulates an operation. It provides a
   * method to attempt the actual operation, and a method to generate a
   * return value in case of a thrift exception. Both methods can throw an
   * exception, of the same type as the actual operation would throw.
   * @param <T> The return type of the operation
   * @param <E> The exception type thrown by the operation
   */
  abstract static class Opexeptionable <T, E extends Exception> {

    /** the name of the operation */
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
    abstract T call(OperationExecutorClient client) throws TException, E;

    /** produce a return value in the case of a thrift exception */
    T error(TException te) throws E {
      return null;
    }
  }

  /**
   * This is an abstract class that encapsulates an operation. It provides a
   * method to attempt the actual operation, and a method to generate a
   * return value in case of a thrift exception. Note that this is the same
   * as Opexeptionable for operations that do not throw exceptions.
   * @param <T> The return type of the operation
   */
  abstract static class Opexable<T>
      extends Opexeptionable <T, RuntimeException> {

    /** constructor with name of operation */
    Opexable(String operation) {
      super(operation);
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
   * @param <E> The exception (other than TException) expected from the
   *           operation
   * @return the result of the operation, or a value returned by error()
   * @throws E if the operation fails with an exception other than TException
   */
  private <T, E extends Exception> T execute(Opexeptionable<T, E> operation)
      throws E {
    RetryStrategy retryStrategy = retryStrategyProvider.newRetryStrategy();
    while (true) {
      OperationExecutorClient client = this.clientProvider.getClient();
      try {
        // note that this can throw exceptions other than TException
        // hence the finally clause at the end
        return operation.call(client);

      } catch (TException te) {
        // make sure the error gets logged
        Log.error("Thrift error for operation " +
            operation.getName() + " : " + te.getMessage());

        // a thrift error occurred, the thrift client may be in a bad state
        this.clientProvider.discardClient(client);
        client = null;

        // determine whether we should retry
        boolean retry = retryStrategy.failOnce();
        if (!retry) {
          // retry strategy is exceeded, return an error
          // note that this can throw an exception of type E
          return operation.error(te);
        } else {
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
  public BatchOperationResult execute(final List<WriteOperation> writes)
      throws BatchOperationException {
    return this.execute(
        new Opexeptionable<BatchOperationResult,
            BatchOperationException>("Batch") {
          @Override
          public BatchOperationResult call(OperationExecutorClient client)
              throws BatchOperationException, TException {
            return client.execute(writes);
          }
          @Override
          public BatchOperationResult error(TException te) throws
              BatchOperationException {
            String message = "Thrift error for Batch: " + te.getMessage();
            throw new BatchOperationException(message, te);
          }
        });
  }

  @Override
  public DequeueResult execute(final QueueDequeue dequeue) {
    return this.execute(
        new Opexable<DequeueResult>("Dequeue") {
          @Override
          public DequeueResult call(OperationExecutorClient client)
              throws TException {
            return client.execute(dequeue);
          }
          @Override
          public DequeueResult error(TException te) {
            String message = "Thrift error for Dequeue: " + te.getMessage();
            return new
                DequeueResult(DequeueResult.DequeueStatus.FAILURE, message);
          }
        });
  }

  @Override
  public long execute(final QueueAdmin.GetGroupID getGroupID) {
    return this.execute(
        new Opexable<Long>("GetGroupID") {
          @Override
          public Long call(OperationExecutorClient client) throws TException {
            return client.execute(getGroupID);
          }
          @Override
          public Long error(TException te) {
            return 0L;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public QueueAdmin.QueueMeta execute(final QueueAdmin.GetQueueMeta getQM) {
    return this.execute(
        new Opexable<QueueAdmin.QueueMeta>("GetQueueMeta") {
          @Override
          public QueueAdmin.QueueMeta call(OperationExecutorClient client)
              throws TException {
            return client.execute(getQM);
          }
          @Override
          public QueueAdmin.QueueMeta error(TException te) {
            return null;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public void execute(final ClearFabric clearFabric) {
    this.execute(
        new Opexable<Boolean>("ClearFabric") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException {
            client.execute(clearFabric);
            return true;
          }
          @Override
          public Boolean error(TException te) {
            return null;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public byte[] execute(final ReadKey readKey) {
    return this.execute(
        new Opexable<byte[]>("ReadKey") {
          @Override
          public byte[] call(OperationExecutorClient client) throws TException {
            return client.execute(readKey);
          }
          @Override
          public byte[] error(TException te) {
            return null;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public Map<byte[], byte[]> execute(final Read read) {
    return this.execute(
        new Opexable<Map<byte[], byte[]>>("Read") {
          @Override
          public Map<byte[], byte[]> call(OperationExecutorClient client)
              throws TException {
            return client.execute(read);
          }
          @Override
          public Map<byte[], byte[]> error(TException te) {
            return null;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public List<byte[]> execute(final ReadAllKeys readAllKeys) {
    return this.execute(
        new Opexable<List<byte[]>>("ReadAllKeys") {
          @Override
          public List<byte[]> call(OperationExecutorClient client)
              throws TException {
            return client.execute(readAllKeys);
          }
          @Override
          public List<byte[]> error(TException te) {
            return new ArrayList<byte[]>(0);
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public Map<byte[], byte[]> execute(final ReadColumnRange readColumnRange) {
    return this.execute(
        new Opexable<Map<byte[], byte[]>>("ReadColumnRange") {
          @Override
          public Map<byte[], byte[]> call(OperationExecutorClient client)
              throws TException {
            return client.execute(readColumnRange);
          }
          @Override
          public Map<byte[], byte[]> error(TException te) {
            return null;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public boolean execute(final Write write) {
    return this.execute(
        new Opexable<Boolean>("Write") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException {
            return client.execute(write);
          }
          @Override
          public Boolean error(TException te) {
            return false;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public boolean execute(final Delete delete) {
    return this.execute(
        new Opexable<Boolean>("Delete") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException {
            return client.execute(delete);
          }
          @Override
          public Boolean error(TException te) {
            return false;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public boolean execute(final Increment increment) {
    return this.execute(
        new Opexable<Boolean>("Increment") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException {
            return client.execute(increment);
          }
          @Override
          public Boolean error(TException te) {
            return false;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public boolean execute(final CompareAndSwap compareAndSwap) {
    return this.execute(
        new Opexable<Boolean>("CompareAndSwap") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException {
            return client.execute(compareAndSwap);
          }
          @Override
          public Boolean error(TException te) {
            return false;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public boolean execute(final QueueEnqueue enqueue) {
    return this.execute(
        new Opexable<Boolean>("Enqueue") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException {
            return client.execute(enqueue);
          }
          @Override
          public Boolean error(TException te) {
            return false;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public boolean execute(final QueueAck ack) {
    return this.execute(
        new Opexable<Boolean>("Ack") {
          @Override
          public Boolean call(OperationExecutorClient client)
              throws TException {
            return client.execute(ack);
          }
          @Override
          public Boolean error(TException te) {
            return false;
            // TODO execute() must be able to return an error
          }
        });
  }

  @Override
  public String getName() {
    return "remote";
  }
}
