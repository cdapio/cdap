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

package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.metrics.TxMetricsCollector;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Google Guice Provider for {@link InMemoryTransactionManager} instances.  Each call to {@link #get()} will
 * return a new {@link InMemoryTransactionManager} instance.
 */
public class InMemoryTransactionManagerProvider implements Provider<InMemoryTransactionManager> {
  private final CConfiguration conf;
  private final Provider<TransactionStateStorage> storageProvider;
  private final TxMetricsCollector txMetricsCollector;

  @Inject
  public InMemoryTransactionManagerProvider(CConfiguration config, Provider<TransactionStateStorage> storageProvider,
                                            TxMetricsCollector txMetricsCollector) {
    this.conf = config;
    this.storageProvider = storageProvider;
    this.txMetricsCollector = txMetricsCollector;
  }

  @Override
  public InMemoryTransactionManager get() {
    return new InMemoryTransactionManager(conf, storageProvider.get(), txMetricsCollector);
  }
}
