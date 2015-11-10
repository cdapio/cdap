/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.app.guice;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.ProgramContext;
import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import co.cask.cdap.data2.transaction.stream.StreamConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.internal.app.runtime.AbstractDataFabricFacade;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import co.cask.cdap.internal.app.runtime.DataFabricFacadeFactory;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionExecutor;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;

import java.io.IOException;

/**
 * A private module for creating bindings for DataFabricFacadeFactory
 */
public final class DataFabricFacadeModule extends PrivateModule {

  @Override
  protected void configure() {

    // Creates a DataFabricFacadeFactory injection for creating DataFabricFacade of different types.
    install(
      new FactoryModuleBuilder()
        .implement(DataFabricFacade.class, LineageWriterDataFabricFacade.class)
        .build(DataFabricFacadeFactory.class)
    );
    expose(DataFabricFacadeFactory.class);
  }

  /**
   * A {@link DataFabricFacade} with transaction supports.
   */
  private static final class TransactionDataFabricFacade extends AbstractDataFabricFacade {

    @Inject
    TransactionDataFabricFacade(TransactionExecutorFactory txExecutorFactory,
                                QueueClientFactory queueClientFactory,
                                StreamConsumerFactory streamConsumerFactory,
                                @Assisted Program program,
                                @Assisted DynamicDatasetCache datasetCache) {
      super(txExecutorFactory, queueClientFactory, streamConsumerFactory, program, datasetCache);
    }
  }

  /**
   * A {@link DataFabricFacade} that records data access from programs.
   */
  private static final class LineageWriterDataFabricFacade implements DataFabricFacade, ProgramContextAware {
    private final DataFabricFacade delegate;
    private final LineageWriter lineageWriter;
    private final ProgramContext programContext = new ProgramContext();

    @Inject
    LineageWriterDataFabricFacade(TransactionExecutorFactory txExecutorFactory,
                                  QueueClientFactory queueClientFactory,
                                  StreamConsumerFactory streamConsumerFactory,
                                  LineageWriter lineageWriter,
                                  @Assisted Program program,
                                  @Assisted DynamicDatasetCache datasetCache) {
      this.delegate = new TransactionDataFabricFacade(txExecutorFactory, queueClientFactory,
                                                      streamConsumerFactory, program, datasetCache);
      this.lineageWriter = lineageWriter;
    }

    @Override
    public void initContext(Id.Run run) {
      programContext.initContext(run);
    }

    @Override
    public void initContext(Id.Run run, Id.NamespacedId componentId) {
      programContext.initContext(run, componentId);
    }

    @Override
    public DatasetContext getDatasetContext() {
      return delegate.getDatasetContext();
    }

    @Override
    public TransactionContext createTransactionContext() {
      return delegate.createTransactionContext();
    }

    @Override
    public TransactionExecutor createTransactionExecutor() {
      return delegate.createTransactionExecutor();
    }

    @Override
    public StreamConsumer createStreamConsumer(Id.Stream streamName, ConsumerConfig consumerConfig) throws IOException {
      StreamConsumer streamConsumer = delegate.createStreamConsumer(streamName, consumerConfig);
      if (streamConsumer != null && programContext.getRun() != null) {
        lineageWriter.addAccess(programContext.getRun(), streamName, AccessType.READ, programContext.getComponentId());
      }
      return streamConsumer;
    }

    @Override
    public QueueProducer createProducer(QueueName queueName) throws IOException {
      return delegate.createProducer(queueName);
    }

    @Override
    public QueueProducer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
      return delegate.createProducer(queueName, queueMetrics);
    }

    @Override
    public QueueConsumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig, int numGroups)
      throws IOException {
      return delegate.createConsumer(queueName, consumerConfig, numGroups);
    }
  }
}
