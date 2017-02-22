/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.runtime;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.metrics.MetricsCollector;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.NoOpTransactionStateStorage;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.snapshot.SnapshotCodecProvider;

/**
 * Guice bindings for running completely in-memory (no persistence).  This should only be used for
 * test classes, as the transaction state cannot be recovered in the case of a failure.
 */
public class TransactionInMemoryModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SnapshotCodecProvider.class).in(Scopes.SINGLETON);
    bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class).in(Scopes.SINGLETON);
    bind(TransactionManager.class).in(Scopes.SINGLETON);
    bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class).in(Scopes.SINGLETON);
    // no metrics output for in-memory
    bind(MetricsCollector.class).to(TxMetricsCollector.class);

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));
  }
}
