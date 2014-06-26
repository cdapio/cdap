/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.app.program.Program;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.DetachedTxSystemClient;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.internal.app.runtime.AbstractDataFabricFacade;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataFabricFacadeFactory;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.twill.filesystem.LocationFactory;

/**
 * A private module for creating bindings for DataFabricFacadeFactory
 */
public final class DataFabricFacadeModule extends PrivateModule {

  @Override
  protected void configure() {
    // When transaction is off, use a detached transaction system client.
    bind(TransactionSystemClient.class)
      .annotatedWith(Names.named("transaction.off"))
      .to(DetachedTxSystemClient.class).in(Scopes.SINGLETON);

    // When transaction is off, use a TransactionExecutorFactory that uses DetachedTxSystemClient
    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DetachedTransactionExecutor.class)
              .build(Key.get(TransactionExecutorFactory.class, Names.named("transaction.off"))));

    // Creates a DataFabricFacadeFactory injection for creating DataFabricFacade of different types.
    install(
      new FactoryModuleBuilder()
        .implement(DataFabricFacade.class, TransactionDataFabricFacade.class)
        .implement(DataFabricFacade.class, Names.named("transaction.off"), DetachedDataFabricFacade.class)
        .build(DataFabricFacadeFactory.class)
    );

    expose(DataFabricFacadeFactory.class);
  }

  /**
   * A {@link TransactionExecutor} without transaction supports.
   */
  private static final class DetachedTransactionExecutor extends DefaultTransactionExecutor {

    @Inject
    public DetachedTransactionExecutor(@Named("transaction.off") TransactionSystemClient txClient,
                                       @Assisted Iterable<TransactionAware>  txAwares) {
      super(txClient, txAwares);
    }
  }

  /**
   * A {@link DataFabricFacade} with transaction supports.
   */
  private static final class TransactionDataFabricFacade extends AbstractDataFabricFacade {

    @Inject
    public TransactionDataFabricFacade(TransactionSystemClient txSystemClient,
                                       TransactionExecutorFactory txExecutorFactory,
                                       DataSetAccessor dataSetAccessor,
                                       DatasetFramework datasetFramework,
                                       QueueClientFactory queueClientFactory,
                                       StreamConsumerFactory streamConsumerFactory,
                                       LocationFactory locationFactory,
                                       CConfiguration configuration,
                                       @Assisted Program program) {
      super(txSystemClient, txExecutorFactory, dataSetAccessor, datasetFramework,
            queueClientFactory, streamConsumerFactory, locationFactory, program, configuration);
    }
  }

  /**
   * A {@link DataFabricFacade} without transaction supports.
   */
  private static final class DetachedDataFabricFacade extends AbstractDataFabricFacade {

    @Inject
    public DetachedDataFabricFacade(@Named("transaction.off") TransactionSystemClient txSystemClient,
                                    @Named("transaction.off") TransactionExecutorFactory txExecutorFactory,
                                    DataSetAccessor dataSetAccessor,
                                    DatasetFramework datasetFramework,
                                    QueueClientFactory queueClientFactory,
                                    StreamConsumerFactory streamConsumerFactory,
                                    LocationFactory locationFactory,
                                    CConfiguration configuration,
                                    @Assisted Program program) {
      super(txSystemClient, txExecutorFactory, dataSetAccessor, datasetFramework,
            queueClientFactory, streamConsumerFactory, locationFactory, program, configuration);
    }
  }
}
