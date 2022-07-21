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
import io.cdap.cdap.spi.data.StorageProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.common.DefaultStorageProvider;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;

/**
 * Module to provide the binding for the new storage spi
 */
public class StorageModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(StorageProvider.class).to(DefaultStorageProvider.class).in(Scopes.SINGLETON);

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
