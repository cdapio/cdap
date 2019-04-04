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

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.common.CachedStructuredTableRegistry;
import io.cdap.cdap.spi.data.nosql.NoSqlStructuredTableAdmin;
import io.cdap.cdap.spi.data.nosql.NoSqlStructuredTableRegistry;
import io.cdap.cdap.spi.data.nosql.NoSqlTransactionRunner;
import io.cdap.cdap.spi.data.sql.PostgresSqlStructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.RetryingSqlTransactionRunner;
import io.cdap.cdap.spi.data.sql.SqlStructuredTableRegistry;
import io.cdap.cdap.spi.data.sql.jdbc.DataSourceProvider;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;

import javax.sql.DataSource;

/**
 * Module to provide the binding for the new storage spi
 */
public class StorageModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(TransactionRunner.class).toProvider(TransactionRunnerProvider.class).in(Scopes.SINGLETON);
    bind(StructuredTableAdmin.class).toProvider(StructuredTableAdminProvider.class).in(Scopes.SINGLETON);
    bind(StructuredTableRegistry.class).toProvider(StructuredTableRegistryProvider.class).in(Scopes.SINGLETON);
    bind(DataSourceProvider.class).in(Scopes.SINGLETON);
    bind(DataSource.class).toProvider(DataSourceProvider.class).in(Scopes.SINGLETON);

    expose(TransactionRunner.class);
    expose(StructuredTableAdmin.class);
    expose(StructuredTableRegistry.class);
    expose(DataSource.class);
  }

  /**
   * Transaction runner provider to provide the {@link TransactionRunner} class, the actual implementation will be
   * based on the configuration file.
   */
  private static final class TransactionRunnerProvider implements Provider<TransactionRunner> {

    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    TransactionRunnerProvider(CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    @Override
    public TransactionRunner get() {
      String storageImpl = cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION);
      if (storageImpl == null) {
        throw new IllegalStateException("No storage implementation is specified in the configuration file");
      }

      storageImpl = storageImpl.toLowerCase();
      if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_NOSQL)) {
        return injector.getInstance(NoSqlTransactionRunner.class);
      }

      if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_SQL)) {
        return injector.getInstance(RetryingSqlTransactionRunner.class);
      }

      throw new UnsupportedOperationException(
        String.format("%s is not a supported storage implementation, the supported implementations are %s and %s",
                      storageImpl, Constants.Dataset.DATA_STORAGE_NOSQL, Constants.Dataset.DATA_STORAGE_SQL));
    }
  }

  /**
   * Structure table admin provider to provide the {@link StructuredTableAdmin} class, the actual implementation
   * will be based on the configuration file.
   */
  private static final class StructuredTableAdminProvider implements Provider<StructuredTableAdmin> {

    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    StructuredTableAdminProvider(CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    @Override
    public StructuredTableAdmin get() {
      String storageImpl = cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION);
      if (storageImpl == null) {
        throw new IllegalStateException("No storage implementation is specified in the configuration file");
      }

      storageImpl = storageImpl.toLowerCase();
      if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_NOSQL)) {
        return injector.getInstance(NoSqlStructuredTableAdmin.class);
      }
      if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_SQL)) {
        return injector.getInstance(PostgresSqlStructuredTableAdmin.class);
      }
      throw new UnsupportedOperationException(
        String.format("%s is not a supported storage implementation, the supported implementations are %s and %s",
                      storageImpl, Constants.Dataset.DATA_STORAGE_NOSQL, Constants.Dataset.DATA_STORAGE_SQL));
    }
  }

  /**
   * Structure table registry provider to provide the {@link StructuredTableRegistry} class, the actual implementation
   * will be based on the configuration file.
   */
  private static final class StructuredTableRegistryProvider implements Provider<StructuredTableRegistry> {
    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    StructuredTableRegistryProvider(CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    @Override
    public StructuredTableRegistry get() {
      String storageImpl = cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION);
      if (storageImpl == null) {
        throw new IllegalStateException("No storage implementation is specified in the configuration file");
      }

      storageImpl = storageImpl.toLowerCase();
      if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_NOSQL)) {
        NoSqlStructuredTableRegistry registry = injector.getInstance(NoSqlStructuredTableRegistry.class);
        return new CachedStructuredTableRegistry(registry);
      }
      if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_SQL)) {
        SqlStructuredTableRegistry registry = injector.getInstance(SqlStructuredTableRegistry.class);
        return new CachedStructuredTableRegistry(registry);
      }
      throw new UnsupportedOperationException(
        String.format("%s is not a supported storage implementation, the supported implementations are %s and %s",
                      storageImpl, Constants.Dataset.DATA_STORAGE_NOSQL, Constants.Dataset.DATA_STORAGE_SQL));
    }
  }
}
