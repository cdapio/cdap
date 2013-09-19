package com.continuuity.gateway.v2.handlers.v2.dataset;

import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.TestUtil;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Callable;

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
    Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/all").getStatusLine().getStatusCode());
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
    Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/datasets").getStatusLine().getStatusCode());
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
    Assert.assertEquals(200, GatewayFastTestsSuite.doDelete("/v2/queues").getStatusLine().getStatusCode());
    // verify all are gone
    Assert.assertTrue(verifyTable(tableName));
    // NOTE: actually streams data gone too since we store it in same place where we store queues TODO: fix it
    Assert.assertTrue(verifyStream(streamName));
    Assert.assertFalse(verifyQueue(queueName));
  }

  static final QueueEntry STREAM_ENTRY = new QueueEntry("x".getBytes());

  static void createStream(String name) throws Exception {
    // create stream
    Stream stream = new Stream(name);
    stream.setName(name);

    MetadataService mds = GatewayFastTestsSuite.getInjector().getInstance(MetadataService.class);
    mds.assertStream(new Account(context.getAccount()), stream);

    // write smth to a stream
    QueueName queueName = QueueName.fromStream(context.getAccount(), name);
    enqueue(queueName, STREAM_ENTRY);
  }

  static void createQueue(String name) throws Exception {
    // write smth to a queue
    QueueName queueName = getQueueName(name);
    enqueue(queueName, STREAM_ENTRY);
  }

  static boolean dequeueOne(QueueName queueName) throws Exception {
    QueueClientFactory queueClientFactory = GatewayFastTestsSuite.getInjector().getInstance(QueueClientFactory.class);
    final Queue2Consumer consumer = queueClientFactory.createConsumer(queueName,
                                                                      new ConsumerConfig(1L, 0, 1,
                                                                                         DequeueStrategy.ROUND_ROBIN,
                                                                                         null),
                                                                      1);
    // doing inside tx
    TransactionExecutorFactory txExecutorFactory =
      GatewayFastTestsSuite.getInjector().getInstance(TransactionExecutorFactory.class);
    return txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) consumer))
      .execute(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return !consumer.dequeue(1).isEmpty();
        }
      });
  }

  boolean verifyStream(String name) throws Exception {
    MetadataService mds = GatewayFastTestsSuite.getInjector().getInstance(MetadataService.class);
    Stream stream = mds.getStream(new Account(context.getAccount()), new Stream(name));
    boolean streamExists = stream.isExists();
    boolean dataExists = dequeueOne(QueueName.fromStream(context.getAccount(), name));
    return streamExists || dataExists;
  }

  boolean verifyQueue(String name) throws Exception {
    return dequeueOne(getQueueName(name));
  }

  boolean verifyTable(String name) throws Exception {
    DataSetInstantiatorFromMetaData instantiator =
      GatewayFastTestsSuite.getInjector().getInstance(DataSetInstantiatorFromMetaData.class);
    TransactionSystemClient txClient = GatewayFastTestsSuite.getInjector().getInstance(TransactionSystemClient.class);

    OperationResult<Map<byte[], byte[]>> result;
    Table table = instantiator.getDataSet(name, context);
    TransactionContext txContext =
      new TransactionContext(txClient, instantiator.getInstantiator().getTransactionAware());
    txContext.start();
    result = table.read(new Read(new byte[]{'a'}, new byte[]{'b'}));
    txContext.finish();
    return !result.isEmpty();
  }

  private static void enqueue(QueueName queueName, final QueueEntry queueEntry) throws Exception {
    QueueClientFactory queueClientFactory = GatewayFastTestsSuite.getInjector().getInstance(QueueClientFactory.class);
    final Queue2Producer producer = queueClientFactory.createProducer(queueName);
    // doing inside tx
    TransactionExecutorFactory txExecutorFactory =
      GatewayFastTestsSuite.getInjector().getInstance(TransactionExecutorFactory.class);
    txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) producer))
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // write more than one so that we can dequeue multiple times for multiple checks
          producer.enqueue(queueEntry);
          producer.enqueue(queueEntry);
        }
      });
  }

  private static QueueName getQueueName(String name) {
    // i.e. flow and flowlet are constants: should be good enough
    return QueueName.fromFlowlet("flow1", "flowlet1", name);
  }
}
