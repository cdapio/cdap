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
      this.clientProvider = new PooledClientProvider(config);
    } else if ("thread-local".equals(provider)) {
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
  public BatchOperationResult execute(List<WriteOperation> writes)
      throws BatchOperationException {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(writes);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public DequeueResult execute(QueueDequeue operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public long execute(QueueAdmin.GetGroupID operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public QueueAdmin.QueueMeta execute(QueueAdmin.GetQueueMeta operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public void execute(ClearFabric operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public byte[] execute(ReadKey operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public Map<byte[], byte[]> execute(Read operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public List<byte[]> execute(ReadAllKeys operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public Map<byte[], byte[]> execute(ReadColumnRange operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public boolean execute(Write operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }


  @Override
  public boolean execute(Delete operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public boolean execute(Increment operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public boolean execute(CompareAndSwap operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public boolean execute(QueueEnqueue operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }

  @Override
  public boolean execute(QueueAck operation) {
    OperationExecutorClient myClient = this.clientProvider.getClient();
    try {
      return myClient.execute(operation);
    } finally {
      this.clientProvider.returnClient(myClient);
    }
  }
}
