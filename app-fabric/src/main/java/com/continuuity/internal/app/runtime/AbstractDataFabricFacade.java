package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.app.program.Program;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Throwables;

import java.io.IOException;

/**
 * Abstract base class for implementing DataFabricFacade.
 */
public abstract class AbstractDataFabricFacade implements DataFabricFacade {

  private final DataSetInstantiator dataSetContext;
  private final QueueClientFactory queueClientFactory;
  private final TransactionExecutorFactory txExecutorFactory;

  private final TransactionSystemClient txSystemClient;

  public AbstractDataFabricFacade(TransactionSystemClient txSystemClient, TransactionExecutorFactory txExecutorFactory,
                                  DataSetAccessor dataSetAccessor, QueueClientFactory queueClientFactory,
                                  LocationFactory locationFactory, Program program) {
    this.txSystemClient = txSystemClient;
    this.queueClientFactory = queueClientFactory;
    this.txExecutorFactory = txExecutorFactory;
    this.dataSetContext = createDataSetContext(program, locationFactory, dataSetAccessor);
  }

  @Override
  public DataSetContext getDataSetContext() {
    return dataSetContext;
  }

  @Override
  public TransactionContext createTransactionManager() {
    return new TransactionContext(txSystemClient, dataSetContext.getTransactionAware());
  }

  @Override
  public TransactionExecutor createTransactionExecutor() {
    return txExecutorFactory.createExecutor(dataSetContext.getTransactionAware());
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
                                                   LocationFactory locationFactory,
                                                   DataSetAccessor dataSetAccessor) {
    try {
      DataFabric dataFabric = new DataFabric2Impl(locationFactory, dataSetAccessor);
      DataSetInstantiator dataSetInstantiator = new DataSetInstantiator(dataFabric,
                                                                        program.getMainClass().getClassLoader());
      dataSetInstantiator.setDataSets(program.getSpecification().getDataSets().values());
      return dataSetInstantiator;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
