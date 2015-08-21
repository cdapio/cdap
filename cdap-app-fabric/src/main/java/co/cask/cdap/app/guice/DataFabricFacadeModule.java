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
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;

/**
 * A private module for creating bindings for DataFabricFacadeFactory
 */
public final class DataFabricFacadeModule extends PrivateModule {

  @Override
  protected void configure() {

    // Creates a DataFabricFacadeFactory injection for creating DataFabricFacade of different types.
    install(
      new FactoryModuleBuilder()
        .implement(DataFabricFacade.class, TransactionDataFabricFacade.class)
        .build(DataFabricFacadeFactory.class)
    );
    expose(DataFabricFacadeFactory.class);
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
}
