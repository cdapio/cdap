package com.continuuity.gateway.v2.handlers.v2.dataset;

import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.data.dataset.DataSetInstantiationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.TestUtil;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.v2.txmanager.TxManager;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests ClearFabricHandler.
 */
public class ClearFabricHandlerTest {
  private static final OperationContext context = TestUtil.DEFAULT_CONTEXT;

  @Test
  public void testClearDataAll() throws Exception {
    // setup accessor
    String tableName = "mannamanna1";
    String streamName = "doobdoobee1";
    String queueName = "doobee1";

    // create a stream, a queue, a table
    TableHandlerTest.createTable(tableName);
    createStream(streamName);
    createQueue(queueName);

    // verify they are all there
    Assert.assertTrue(verifyTable(tableName));
    Assert.assertTrue(verifyStream(streamName));
    Assert.assertTrue(verifyQueue(queueName));

    // clear all
    Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/all").getStatusLine().getStatusCode());
    // verify all are gone
    Assert.assertFalse(verifyTable(tableName));
    Assert.assertFalse(verifyStream(streamName));
    Assert.assertFalse(verifyQueue(queueName));
  }

  @Test
  public void testClearDataTable() throws Exception {
    // setup accessor
    String tableName = "mannamanna2";
    String streamName = "doobdoobee2";
    String queueName = "doobee2";

    // create a stream, a queue, a table
    TableHandlerTest.createTable(tableName);
    createStream(streamName);
    createQueue(queueName);

    // verify they are all there
    Assert.assertTrue(verifyTable(tableName));
    Assert.assertTrue(verifyStream(streamName));
    Assert.assertTrue(verifyQueue(queueName));

    // clear all
    Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/datasets").getStatusLine().getStatusCode());
    // verify all are gone
    Assert.assertFalse(verifyTable(tableName));
    Assert.assertTrue(verifyStream(streamName));
    Assert.assertTrue(verifyQueue(queueName));
  }

  @Test
  public void testClearQueues() throws Exception {
    // setup accessor
    String tableName = "mannamanna2";
    String streamName = "doobdoobee2";
    String queueName = "doobee2";

    // create a stream, a queue, a table
    TableHandlerTest.createTable(tableName);
    createStream(streamName);
    createQueue(queueName);

    // verify they are all there
    Assert.assertTrue(verifyTable(tableName));
    Assert.assertTrue(verifyStream(streamName));
    Assert.assertTrue(verifyQueue(queueName));

    // clear all
    Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/queues").getStatusLine().getStatusCode());
    // verify all are gone
    Assert.assertTrue(verifyTable(tableName));
    Assert.assertTrue(verifyStream(streamName));
    Assert.assertFalse(verifyQueue(queueName));
  }

  static final QueueEntry STREAM_ENTRY = new QueueEntry("x".getBytes());

  static WriteOperation addToStream(String name) {
    return new QueueEnqueue(("stream:" + name).getBytes(), STREAM_ENTRY);
  }

  static WriteOperation addToQueue(String name) {
    return new QueueEnqueue(("queue:" + name).getBytes(), STREAM_ENTRY);
  }

  static void createStream(String name) throws Exception {
    Stream stream = new Stream(name);
    stream.setName(name);

    MetadataService mds = GatewayFastTestsSuite.getInjector().getInstance(MetadataService.class);
    mds.assertStream(new Account(context.getAccount()), stream);

    OperationExecutor executor = GatewayFastTestsSuite.getInjector().getInstance(OperationExecutor.class);
    executor.commit(context, addToStream(name));
  }

  static void createQueue(String name) throws Exception {
    OperationExecutor executor = GatewayFastTestsSuite.getInjector().getInstance(OperationExecutor.class);
    executor.commit(context, addToQueue(name));
  }

  boolean dequeueOne(String queue) throws Exception {
    OperationExecutor executor = GatewayFastTestsSuite.getInjector().getInstance(OperationExecutor.class);
    long groupId = executor.execute(context, new GetGroupID(queue.getBytes()));
    QueueConsumer consumer = new QueueConsumer(0, groupId, 1,
                                               new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true));
    executor.execute(context, new QueueConfigure(queue.getBytes(), consumer));
    DequeueResult result = executor.execute(context,
                                            new QueueDequeue(queue.getBytes(), consumer, consumer.getQueueConfig()));
    return !result.isEmpty();
  }

  boolean verifyStream(String name) throws Exception {
    MetadataService mds = GatewayFastTestsSuite.getInjector().getInstance(MetadataService.class);
    Stream stream = mds.getStream(new Account(context.getAccount()), new Stream(name));
    boolean streamExists = stream.isExists();
    boolean dataExists = dequeueOne("stream:" + name);
    return streamExists || dataExists;
  }

  boolean verifyQueue(String name) throws Exception {
    return dequeueOne("queue:" + name);
  }

  boolean verifyTable(String name) throws Exception {
    DataSetInstantiatorFromMetaData instantiator =
      GatewayFastTestsSuite.getInjector().getInstance(DataSetInstantiatorFromMetaData.class);
    TransactionSystemClient txClient = GatewayFastTestsSuite.getInjector().getInstance(TransactionSystemClient.class);
    OperationExecutor executor = GatewayFastTestsSuite.getInjector().getInstance(OperationExecutor.class);

    OperationResult<Map<byte[], byte[]>> result;
    try {
      Table table = instantiator.getDataSet(name, context);
      TxManager txManager = new TxManager(txClient, instantiator.getInstantiator().getTransactionAware());
      txManager.start();
      result = table.read(new Read(new byte[]{'a'}, new byte[]{'b'}));
      txManager.commit();
    } catch (DataSetInstantiationException e) {
      result = executor.execute(
        context, new com.continuuity.data.operation.Read(name, new byte[]{'a'}, new byte[]{'b'}));
    }
    return !result.isEmpty();
  }

}
