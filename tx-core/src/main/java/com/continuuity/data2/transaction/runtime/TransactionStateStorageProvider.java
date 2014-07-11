/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.persist.NoOpTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

/**
 * A provider for {@link TransactionStateStorage} that provides different
 * {@link TransactionStateStorage} implementation based on configuration.
 */
@Singleton
public final class TransactionStateStorageProvider implements Provider<TransactionStateStorage> {

  private final CConfiguration cConf;
  private final Injector injector;

  @Inject
  TransactionStateStorageProvider(CConfiguration cConf, Injector injector) {
    this.cConf = cConf;
    this.injector = injector;
  }

  @Override
  public TransactionStateStorage get() {
    if (cConf.getBoolean(TxConstants.Manager.CFG_DO_PERSIST, true)) {
      return injector.getInstance(Key.get(TransactionStateStorage.class, Names.named("persist")));
    } else {
      return injector.getInstance(NoOpTransactionStateStorage.class);
    }
  }
}
