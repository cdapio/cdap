/*
 * Copyright © 2019 Cask Data, Inc.
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


package io.cdap.cdap.data.runtime;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.StorageProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.common.DefaultStorageProvider;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;

/**
 * Module to provide the binding for the new storage spi
 */
public class StorageModule extends PrivateModule {
  public static final String STORAGE_IMPL_NAME_KEY = "storageImplNameKey";

  private final String storageImplNameKey;

  /**
   * Returns a storage module with the default name key.
   */
  public StorageModule() {
    this.storageImplNameKey = Constants.Dataset.DATA_STORAGE_IMPLEMENTATION;
  }

  /**
   * Returns a storage module with a specific name key.
   * @param storageImplNameKey The specific name key to use as an override config
   */
  public StorageModule(String storageImplNameKey) {
    this.storageImplNameKey = storageImplNameKey;
  }

  @Override
  protected void configure() {
    bind(StorageProvider.class).to(DefaultStorageProvider.class).in(Scopes.SINGLETON);
    bind(String.class).annotatedWith(Names.named(STORAGE_IMPL_NAME_KEY))
      .toInstance(storageImplNameKey);

    expose(StorageProvider.class);
    expose(TransactionRunner.class);
    expose(StructuredTableAdmin.class);
  }

  @Provides
  @Singleton
  private TransactionRunner provideTransactionRunner(StorageProvider storageProvider) {
    try {
      return storageProvider.getTransactionRunner();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create TransactionRunner", e);
    }
  }

  @Provides
  @Singleton
  private StructuredTableAdmin provideStructuredTableAdmin(StorageProvider storageProvider) {
    try {
      return storageProvider.getStructuredTableAdmin();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create StructuredTableAdmin", e);
    }
  }
}
