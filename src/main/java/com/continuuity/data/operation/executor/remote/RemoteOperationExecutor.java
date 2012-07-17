package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.BatchOperationException;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.*;
import com.continuuity.data.operation.ttqueue.*;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
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

  // thrift client
  TOperationExecutor.Client client;

  // the client to discover where opex service is running
  ServiceDiscoveryClient discoveryClient;

  /**
   * Create with explicit hostname and port. This will skip service
   * discovery.
   * @param host the host where the opex service is running
   * @param port the port where the opex service is running
   * @throws IOException
   */
  @SuppressWarnings("unused")
  public RemoteOperationExecutor(String host, int port) throws IOException {
    this.init(host, port);
  }

  /**
   * Create from a configuration. This will first attempt to find a zookeeper
   * for service discovery. Otherwise it will look for the port in the
   * config and use localhost.
   * @param config a configuration containing the zookeeper properties
   * @throws IOException
   */
  @Inject
  public RemoteOperationExecutor(
      @Named("RemoteOperationExecutorConfig")CConfiguration config) throws IOException {
    this.init(config);
  }

  /**
   * Initialize from a configuration. This will first attempt to find a
   * zookeeper for service discovery. Otherwise it will look for the port
   * in the config and use localhost.
   * @param config a configuration containing the zookeeper properties
   * @throws IOException
   */
  private void init(CConfiguration config) throws IOException {
    // try to find the zookeeper ensemble in the config
    String zookeeper = config.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
    if (zookeeper == null) {
      // no zookeeper, look for the port and use localhost
      Log.info("Zookeeper Ensemble not configured. Skipping service discovery");
      Log.info("Trying to read address and port from configuration.");
      String address = config.get(Constants.CFG_DATA_OPEX_SERVER_PORT,
          Constants.DEFAULT_DATA_OPEX_SERVER_ADDRESS);
      int port = config.getInt(Constants.CFG_DATA_OPEX_SERVER_PORT,
          Constants.DEFAULT_DATA_OPEX_SERVER_PORT);
      this.init(address, port);
      return;
    }
    // attempt to discover the service
    try {
      this.discoveryClient = new ServiceDiscoveryClient(zookeeper);
      Log.info("Connected to service discovery. ");
    } catch (ServiceDiscoveryClientException e) {
      Log.error("Unable to start service discovery client: " + e.getMessage());
      throw new IOException("Unable to start service dicovery client.", e);
    }
    String host;
    int port;
    try {
      ServiceDiscoveryClient.ServiceProvider provider =
          this.discoveryClient.getServiceProvider(
              OperationExecutorService.SERVICE_NAME);
      ProviderStrategy<ServiceDiscoveryClient.ServicePayload> strategy =
          new RandomStrategy<ServiceDiscoveryClient.ServicePayload>();
      ServiceInstance<ServiceDiscoveryClient.ServicePayload>
          instance = strategy.getInstance(provider);
      // found an instance, get its host name and port
      host = instance.getAddress();
      port = instance.getPort();
      Log.info("Service discovered at " + host + ":" + port);
    } catch (Exception e) {
      Log.error("Unable to discover opex service: " + e.getMessage());
      throw new IOException("Unable to discover opex service.", e);
    }
    // and initialize with the found address
    this.init(host, port);
    // the discovery client is now no longer needed
    this.discoveryClient.close();
  }

  /**
   * Initialize from explicit hostname and port. This will skip service
   * discovery.
   * @param host the host where the opex service is running
   * @param port the port where the opex service is running
   * @throws IOException
   */
  private void init(String host, int port) throws IOException {
    Log.info("Attempting to connect to Operation Executor service at " +
        host + ":" + port);
    // thrift transport layer
    TTransport transport = new TFramedTransport(new TSocket(host, port));
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new IOException("Unable to connect to service", e);
    }
    // thrift protocol layer, we use binary because so does the service
    TProtocol protocol = new TBinaryProtocol(transport);
    // and create a thrift client
    client = new TOperationExecutor.Client(protocol);
    Log.info("Connected to Operation Executor service at " +
        host + ":" + port);
  }

  @Override
  synchronized
  public BatchOperationResult execute(List<WriteOperation> writes)
      throws BatchOperationException {
    List<TWriteOperation> tWrites = Lists.newArrayList();
    for (WriteOperation writeOp : writes) {
      TWriteOperation tWriteOp = new TWriteOperation();
      if (writeOp instanceof Write)
        tWriteOp.setWrite(wrap((Write) writeOp));
      else if (writeOp instanceof Delete)
        tWriteOp.setDelet(wrap((Delete) writeOp));
      else if (writeOp instanceof Increment)
        tWriteOp.setIncrement(wrap((Increment) writeOp));
      else if (writeOp instanceof CompareAndSwap)
        tWriteOp.setCompareAndSwap(wrap((CompareAndSwap) writeOp));
      else if (writeOp instanceof QueueEnqueue)
        tWriteOp.setQueueEnqueue(wrap((QueueEnqueue) writeOp));
      else if (writeOp instanceof QueueAck)
        tWriteOp.setQueueAck(wrap((QueueAck) writeOp));
      else {
        Log.error("Internal Error: Received an unknown WriteOperation of class "
            + writeOp.getClass().getName() + ".");
        continue;
      }
      tWrites.add(tWriteOp);
    }
    try {
      TBatchOperationResult result = client.batch(tWrites);
      return new BatchOperationResult(result.isSuccess(), result.getMessage());
    } catch (TBatchOperationException e) {
      throw new BatchOperationException(e.getMessage(), e);
    } catch (TException e) {
      throw new BatchOperationException(e.getMessage(), e);
    }
  }

  @Override
  synchronized
  public DequeueResult execute(QueueDequeue dequeue) {
    try {
      Log.debug("Received QueueDequeue " + dequeue.toString() + ", queue = " +
          dequeue.getKey() + ", consumer = " + dequeue.getConsumer().toString()
          + "config = " + (dequeue.getConfig() == null ? "null" :
          dequeue.getConfig().toString()));

      TQueueDequeue tDequeue = wrap(dequeue);

      Log.debug("Sending TQueueDequeue: " + tDequeue.toString());

      return unwrap(client.dequeue(tDequeue));

    } catch (TException e) {
      String message = "Thrift Call for QueueDequeue failed for queue " +
          new String(dequeue.getKey()) + ": " + e.getMessage();
      Log.error(message);
      return new DequeueResult(DequeueResult.DequeueStatus.FAILURE, message);
    }
  }

  @Override
  synchronized
  public long execute(QueueAdmin.GetGroupID getGroupId) {
    try {
      return client.getGroupId(wrap(getGroupId));
    } catch (TException e) {
      Log.error("Thrift Call for GetGroupId failed for queue " +
          new String(getGroupId.getQueueName()) + ": " + e.getMessage());
      return 0; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public QueueAdmin.QueueMeta execute(QueueAdmin.GetQueueMeta getQueueMeta) {
    try {
      return unwrap(client.getQueueMeta(wrap(getQueueMeta)));
    } catch (TException e) {
      Log.error("Thrift Call for GetQueueMeta failed for queue " +
          new String(getQueueMeta.getQueueName()) + ": " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public void execute(ClearFabric clearFabric) {
    try {
      client.clearFabric(wrap(clearFabric));
    } catch (TException e) {
      Log.error("Thrift Call for ClearFabric failed with message: " +
          e.getMessage());
      // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public byte[] execute(ReadKey readKey) {
    try {
      return unwrap(client.readKey(wrap(readKey)));
    } catch (TException e) {
      Log.error("Thrift Call for ReadKey for key '" +
          new String(readKey.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public Map<byte[], byte[]> execute(Read read) {
    try {
      return unwrap(client.read(wrap(read)));
    } catch (TException e) {
      Log.error("Thrift Call for Read for key '" +
          new String(read.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public List<byte[]> execute(ReadAllKeys readKeys) {
    try {
      return unwrap(client.readAllKeys(wrap(readKeys)));
    } catch (TException e) {
      Log.error("Thrift Call for ReadAllKeys(" + readKeys.getOffset() + ", " +
          readKeys.getLimit() + ") failed with message: " + e.getMessage());
      return new ArrayList<byte[]>(0);
      // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public Map<byte[], byte[]> execute(ReadColumnRange readColumnRange) {
    try {
      return unwrap(client.readColumnRange(wrap(readColumnRange)));
    } catch (TException e) {
      Log.error("Thrift Call for ReadKey for key '" +
          new String(readColumnRange.getKey()) +
          "' failed with message: " + e.getMessage());
      return null; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(Write write) {
    try {
      return client.write(wrap(write));
    } catch (TException e) {
      Log.error("Thrift Call for Write for key '" + new String(write.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(Delete delete) {
    try {
      return client.delet(wrap(delete));
    } catch (TException e) {
      Log.error("Thrift Call for Delete for key '" + new String(delete.getKey())
          + "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(Increment increment) {
    try {
      return client.increment(wrap(increment));
    } catch (TException e) {
      Log.error("Thrift Call for Increment for key '" +
          new String(increment.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(CompareAndSwap compareAndSwap) {
    try {
      return client.compareAndSwap(wrap(compareAndSwap));
    } catch (TException e) {
      Log.error("Thrift Call for CompareAndSwap for key '" +
          new String(compareAndSwap.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(QueueEnqueue enqueue) {
    try {
      return client.queueEnqueue(wrap(enqueue));
    } catch (TException e) {
      Log.error("Thrift Call for QueueEnqueue for queue '" +
          new String(enqueue.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }

  @Override
  synchronized
  public boolean execute(QueueAck ack) {
    try {
      return client.queueAck(wrap(ack));
    } catch (TException e) {
      Log.error("Thrift Call for QueueAck for queue '" +
          new String(ack.getKey()) +
          "' failed with message: " + e.getMessage());
      return false; // TODO execute() must be able to return an error
    }
  }
}
