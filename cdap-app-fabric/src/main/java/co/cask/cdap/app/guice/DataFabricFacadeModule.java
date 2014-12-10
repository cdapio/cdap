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

import co.cask.cdap.app.program.Program;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.internal.app.runtime.AbstractDataFabricFacade;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import co.cask.cdap.internal.app.runtime.DataFabricFacadeFactory;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.inmemory.DetachedTxSystemClient;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

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
                                       QueueClientFactory queueClientFactory,
                                       StreamConsumerFactory streamConsumerFactory,
                                       @Assisted Program program,
                                       @Assisted DatasetInstantiator instantiator) {
      super(txSystemClient, txExecutorFactory, queueClientFactory, streamConsumerFactory, program, instantiator);
    }
  }

  /**
   * A {@link DataFabricFacade} without transaction supports.
   */
  private static final class DetachedDataFabricFacade extends AbstractDataFabricFacade {

    @Inject
    public DetachedDataFabricFacade(@Named("transaction.off") TransactionSystemClient txSystemClient,
                                    @Named("transaction.off") TransactionExecutorFactory txExecutorFactory,
                                    QueueClientFactory queueClientFactory,
                                    StreamConsumerFactory streamConsumerFactory,
                                    @Assisted Program program,
                                    @Assisted DatasetInstantiator instantiator) {
      super(txSystemClient, txExecutorFactory, queueClientFactory, streamConsumerFactory, program, instantiator);
    }
  }
}
