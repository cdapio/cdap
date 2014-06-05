/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.persist.LocalFileTransactionStateStorage;
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
final class TransactionStateStorageProvider implements Provider<TransactionStateStorage> {

  private final CConfiguration cConf;
  private final Injector injector;

  @Inject
  TransactionStateStorageProvider(CConfiguration cConf, Injector injector) {
    this.cConf = cConf;
    this.injector = injector;
  }

  @Override
  public TransactionStateStorage get() {
    if (cConf.getBoolean(Constants.Transaction.Manager.CFG_DO_PERSIST, true)) {
      return injector.getInstance(Key.get(LocalFileTransactionStateStorage.class, Names.named("persist")));
    } else {
      return injector.getInstance(NoOpTransactionStateStorage.class);
    }
  }
}
