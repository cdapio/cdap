/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.app.program.Program;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.queue.QueueConsumer;
import com.continuuity.data2.queue.QueueProducer;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.continuuity.data2.transaction.stream.ForwardingStreamConsumer;
import com.continuuity.data2.transaction.stream.StreamConsumer;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.proto.Id;
import com.continuuity.tephra.TransactionAware;
import com.continuuity.tephra.TransactionContext;
import com.continuuity.tephra.TransactionExecutor;
import com.continuuity.tephra.TransactionExecutorFactory;
import com.continuuity.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 * Abstract base class for implementing DataFabricFacade.
 */
public abstract class AbstractDataFabricFacade implements DataFabricFacade {

  private final DataSetInstantiator dataSetContext;
  private final QueueClientFactory queueClientFactory;
  private final StreamConsumerFactory streamConsumerFactory;
  private final TransactionExecutorFactory txExecutorFactory;
  private final TransactionSystemClient txSystemClient;
  private final Id.Program programId;

  public AbstractDataFabricFacade(TransactionSystemClient txSystemClient, TransactionExecutorFactory txExecutorFactory,
                                  DataSetAccessor dataSetAccessor, DatasetFramework datasetFramework,
                                  QueueClientFactory queueClientFactory, StreamConsumerFactory streamConsumerFactory,
                                  LocationFactory locationFactory, Program program,
                                  CConfiguration configuration) {
    this.txSystemClient = txSystemClient;
    this.queueClientFactory = queueClientFactory;
    this.streamConsumerFactory = streamConsumerFactory;
    this.txExecutorFactory = txExecutorFactory;
    this.dataSetContext = createDataSetContext(program, locationFactory, dataSetAccessor,
                                               datasetFramework, configuration);
    this.programId = program.getId();
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
  public QueueProducer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public QueueConsumer createConsumer(QueueName queueName,
                                       ConsumerConfig consumerConfig, int numGroups) throws IOException {
    QueueConsumer consumer = queueClientFactory.createConsumer(queueName, consumerConfig, numGroups);
    if (consumer instanceof TransactionAware) {
      consumer = new CloseableQueueConsumer(dataSetContext, consumer);
      dataSetContext.addTransactionAware((TransactionAware) consumer);
    }
    return consumer;
  }

  @Override
  public QueueProducer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    QueueProducer producer = queueClientFactory.createProducer(queueName, queueMetrics);
    if (producer instanceof TransactionAware) {
      dataSetContext.addTransactionAware((TransactionAware) producer);
    }
    return producer;
  }

  @Override
  public StreamConsumer createStreamConsumer(QueueName streamName, ConsumerConfig consumerConfig) throws IOException {
    String namespace = String.format("%s.%s", programId.getApplicationId(), programId.getId());
    final StreamConsumer consumer = streamConsumerFactory.create(streamName, namespace, consumerConfig);

    dataSetContext.addTransactionAware(consumer);
    return new ForwardingStreamConsumer(consumer) {
      @Override
      public void close() throws IOException {
        super.close();
        dataSetContext.removeTransactionAware(consumer);
      }
    };
  }

  private DataSetInstantiator createDataSetContext(Program program,
                                                   LocationFactory locationFactory,
                                                   DataSetAccessor dataSetAccessor,
                                                   DatasetFramework datasetFramework,
                                                   CConfiguration configuration) {
    try {
      DataFabric dataFabric = new DataFabric2Impl(locationFactory, dataSetAccessor);
      DataSetInstantiator dataSetInstantiator = new DataSetInstantiator(dataFabric, datasetFramework, configuration,
                                                                        program.getClassLoader());
      dataSetInstantiator.setDataSets(program.getSpecification().getDataSets().values(),
                                      program.getSpecification().getDatasets().values());
      return dataSetInstantiator;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
