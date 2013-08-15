package com.continuuity.internal.app.runtime;

import com.continuuity.app.program.Program;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetContext;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SmartTransactionAgent;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.internal.app.queue.QueueConsumerFactory;
import com.continuuity.internal.app.queue.QueueConsumerFactory.QueueInfo;
import com.continuuity.internal.app.queue.QueueConsumerFactoryImpl;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@link DataFabricFacade} that will create a new {@link SmartTransactionAgent} every time
 * when the {@link #createAndUpdateTransactionAgentProxy} method is called.
 * Also the newly created {@link TransactionAgent} would be set into the given {@link TransactionProxy} instance.
 */
public final class SmartDataFabricFacade implements DataFabricFacade {

  private static final Logger LOG = LoggerFactory.getLogger(SmartDataFabricFacade.class);

  private final OperationExecutor opex;
  private final Program program;
  private final TransactionProxy transactionProxy;
  private final DataSetInstantiator dataSetContext;
  private final QueueClientFactory queueClientFactory;

  private final TransactionSystemClient txSystemClient;

  @Inject
  public SmartDataFabricFacade(OperationExecutor opex, LocationFactory locationFactory,
                               TransactionSystemClient txSystemClient, DataSetAccessor dataSetAccessor,
                               QueueClientFactory queueClientFactory, @Assisted Program program) {
    this.opex = opex;
    this.program = program;
    this.transactionProxy = new TransactionProxy();
    this.txSystemClient = txSystemClient;
    this.queueClientFactory = queueClientFactory;
    this.dataSetContext = createDataSetContext(program, opex, locationFactory, dataSetAccessor, transactionProxy);
  }

  @Override
  public DataSetContext getDataSetContext() {
    return dataSetContext;
  }

  @Override
  public TransactionAgent createAndUpdateTransactionAgentProxy() {
    OperationContext ctx = new OperationContext(program.getAccountId(), program.getApplicationId());
    TransactionAgent agent = new SmartTransactionAgent(opex, ctx, dataSetContext.getTransactionAware(), txSystemClient);
    transactionProxy.setTransactionAgent(agent);
    return agent;
  }

  @Override
  public TransactionAgent createTransactionAgent() {
    OperationContext ctx = new OperationContext(program.getAccountId(), program.getApplicationId());
    return new SmartTransactionAgent(opex, ctx, dataSetContext.getTransactionAware(), txSystemClient);
  }

  @Override
  public QueueConsumerFactory createQueueConsumerFactory(int instanceId, long groupId, String groupName,
                                                         QueueName queueName, QueueInfo queueInfo,
                                                         boolean singleEntry) {

    final QueueConsumerFactory factory = new QueueConsumerFactoryImpl(opex, program, instanceId, groupId,
                                                                      groupName, queueName, queueInfo,
                                                                      singleEntry, queueClientFactory);
    // TODO: This is a hacky way to inject queue consumer into transaction aware set.
    return new QueueConsumerFactory() {
      private volatile Queue2Consumer consumer;

      @Override
      public QueueConsumer create(int groupSize) {
        return factory.create(groupSize);
      }

      @Override
      public Queue2Consumer createConsumer(int groupSize) {
        // Cleanup old consumer
        Queue2Consumer oldConsumer = consumer;
        if (oldConsumer != null) {
          if (oldConsumer instanceof TransactionAware) {
            dataSetContext.removeTransactionAware((TransactionAware) oldConsumer);
          }
          if (oldConsumer instanceof Closeable) {
            try {
              ((Closeable) oldConsumer).close();
            } catch (IOException e) {
              LOG.warn("Exception when closing queue consumer: {}", e.getMessage(), e);
            }
          }
        }

        // Create new consumer
        Queue2Consumer newConsumer = factory.createConsumer(groupSize);
        if (newConsumer instanceof TransactionAware) {
          dataSetContext.addTransactionAware((TransactionAware) newConsumer);
        }
        consumer = newConsumer;

        return newConsumer;
      }
    };
  }

  private DataSetInstantiator createDataSetContext(Program program,
                                              OperationExecutor opex,
                                              LocationFactory locationFactory,
                                              DataSetAccessor dataSetAccessor,
                                              TransactionProxy proxy) {
    try {
      OperationContext ctx = new OperationContext(program.getAccountId(), program.getApplicationId());
      DataFabric dataFabric = new DataFabricImpl(opex, locationFactory, dataSetAccessor, ctx);
      DataSetInstantiator dataSetInstantiator = new DataSetInstantiator(dataFabric, proxy,
                                                                        program.getMainClass().getClassLoader());
      dataSetInstantiator.setDataSets(ImmutableList.copyOf(program.getSpecification().getDataSets().values()));
      return dataSetInstantiator;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
