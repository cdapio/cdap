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
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.IOException;

/**
 * A {@link DataFabricFacade} that will create a new {@link SmartTransactionAgent} every time
 * when the {@link #createAndUpdateTransactionAgentProxy} method is called.
 * Also the newly created {@link TransactionAgent} would be set into the given {@link TransactionProxy} instance.
 */
public final class SmartDataFabricFacade implements DataFabricFacade {

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
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName,
                                       ConsumerConfig consumerConfig, int numGroups) throws IOException {
    Queue2Consumer consumer = queueClientFactory.createConsumer(queueName, consumerConfig, numGroups);
    if (consumer instanceof TransactionAware) {
      consumer = new CloseableQueue2Consumer(dataSetContext, consumer);
      dataSetContext.addTransactionAware((TransactionAware) consumer);
    }
    return consumer;
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    Queue2Producer producer = queueClientFactory.createProducer(queueName, queueMetrics);
    if (producer instanceof TransactionAware) {
      dataSetContext.addTransactionAware((TransactionAware) producer);
    }
    return producer;
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
