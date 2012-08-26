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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  @Override
  public BatchOperationResult execute(final List<WriteOperation> writes)
      throws BatchOperationException {
    return this.clientProvider.call(
        new Opexeptionable<BatchOperationResult, BatchOperationException>() {
          @Override
          public BatchOperationResult call(OperationExecutorClient client)
              throws BatchOperationException {
            return client.execute(writes);
          }
        });
  }

  @Override
  public DequeueResult execute(final QueueDequeue dequeue) {
    return this.clientProvider.call(
        new Opexable<DequeueResult>() {
          @Override
          public DequeueResult call(OperationExecutorClient client) {
            return client.execute(dequeue);
          }
        });
  }

  @Override
  public long execute(final QueueAdmin.GetGroupID getGroupID) {
    return this.clientProvider.call(
        new Opexable<Long>() {
          @Override
          public Long call(OperationExecutorClient client) {
            return client.execute(getGroupID);
          }
        });
  }

  @Override
  public QueueAdmin.QueueMeta execute(final QueueAdmin.GetQueueMeta getQM) {
    return this.clientProvider.call(
        new Opexable<QueueAdmin.QueueMeta>() {
          @Override
          public QueueAdmin.QueueMeta call(OperationExecutorClient client) {
            return client.execute(getQM);
          }
        });
  }

  @Override
  public void execute(final ClearFabric clearFabric) {
    this.clientProvider.call(
        new Opexable<Boolean>() {
          @Override
          public Boolean call(OperationExecutorClient client) {
            client.execute(clearFabric);
            return true;
          }
        });
  }

  @Override
  public byte[] execute(final ReadKey readKey) {
    return this.clientProvider.call(
        new Opexable<byte[]>() {
          @Override
          public byte[] call(OperationExecutorClient client) {
            return client.execute(readKey);
          }
        });
  }

  @Override
  public Map<byte[], byte[]> execute(final Read read) {
    return this.clientProvider.call(
        new Opexable<Map<byte[],byte[]>>() {
          @Override
          public Map<byte[],byte[]> call(OperationExecutorClient client) {
            return client.execute(read);
          }
        });
  }

  @Override
  public List<byte[]> execute(final ReadAllKeys readAllKeys) {
    return this.clientProvider.call(
        new Opexable<List<byte[]>>() {
          @Override
          public List<byte[]> call(OperationExecutorClient client) {
            return client.execute(readAllKeys);
          }
        });
  }

  @Override
  public Map<byte[], byte[]> execute(final ReadColumnRange readColumnRange) {
    return this.clientProvider.call(
        new Opexable<Map<byte[],byte[]>>() {
          @Override
          public Map<byte[],byte[]> call(OperationExecutorClient client) {
            return client.execute(readColumnRange);
          }
        });
  }

  @Override
  public boolean execute(final Write write) {
    return this.clientProvider.call(
        new Opexable<Boolean>() {
          @Override
          public Boolean call(OperationExecutorClient client) {
            return client.execute(write);
          }
        });
  }


  @Override
  public boolean execute(final Delete delete) {
    return this.clientProvider.call(
        new Opexable<Boolean>() {
          @Override
          public Boolean call(OperationExecutorClient client) {
            return client.execute(delete);
          }
        });
  }

  @Override
  public boolean execute(final Increment increment) {
    return this.clientProvider.call(
        new Opexable<Boolean>() {
          @Override
          public Boolean call(OperationExecutorClient client) {
            return client.execute(increment);
          }
        });
  }

  @Override
  public boolean execute(final CompareAndSwap compareAndSwap) {
    return this.clientProvider.call(
        new Opexable<Boolean>() {
          @Override
          public Boolean call(OperationExecutorClient client) {
            return client.execute(compareAndSwap);
          }
        });
  }

  @Override
  public boolean execute(final QueueEnqueue enqueue) {
    return this.clientProvider.call(
        new Opexable<Boolean>() {
          @Override
          public Boolean call(OperationExecutorClient client) {
            return client.execute(enqueue);
          }
        });
  }

  @Override
  public boolean execute(final QueueAck ack) {
    return this.clientProvider.call(
        new Opexable<Boolean>() {
          @Override
          public Boolean call(OperationExecutorClient client) {
            return client.execute(ack);
          }
        });
  }

  @Override
  public String getName() {
    return "remote";
  }
}
