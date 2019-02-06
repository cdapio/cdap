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


package co.cask.cdap.data.runtime;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.data2.nosql.NoSqlStructuredTableAdmin;
import co.cask.cdap.data2.nosql.NoSqlStructuredTableRegistry;
import co.cask.cdap.data2.nosql.NoSqlTransactionRunner;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.apache.tephra.TransactionSystemClient;

/**
 * Module to provide the binding for the new storage spi
 */
public class StorageModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(TransactionRunner.class).toProvider(TransactionRunnerProvider.class);
    bind(StructuredTableAdmin.class).toProvider(StructuredTableAdminProvider.class);
    bind(StructuredTableRegistry.class).toProvider(StructuredTableRegistryProvider.class);
    expose(TransactionRunner.class);
    expose(StructuredTableAdmin.class);
    expose(StructuredTableRegistry.class);
  }

  /**
   * Transaction runner provider to provide the {@link TransactionRunner} class, the actual implementation will be
   * based on the configuration file.
   */
  @Singleton
  private static final class TransactionRunnerProvider implements Provider<TransactionRunner> {
    private final CConfiguration cConf;
    private final SConfiguration sConf;
    private final Injector injector;

    @Inject
    TransactionRunnerProvider(CConfiguration cConf, SConfiguration sConf, Injector injector) {
      this.cConf = cConf;
      this.sConf = sConf;
      this.injector = injector;
    }

    @Override
    public TransactionRunner get() {
      String storageImpl = cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION);
      if (storageImpl == null) {
        throw new IllegalStateException("No storage implentation is specified in the configuration file");
      }

      storageImpl = storageImpl.toLowerCase();
      if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_NOSQL)) {
        return new NoSqlTransactionRunner(injector.getInstance(NoSqlStructuredTableAdmin.class),
                                          injector.getInstance(TransactionSystemClient.class));
      }

      if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_SQL)) {
        // TODO: CDAP-14780, connect to the sql using the connection, user name and password from the sConf
        return null;
      }

      throw new UnsupportedOperationException(String.format("%s is not a supported storage implementation, the " +
                                                              "supported implementations are NoSql and PostgresSql.",
                                                            storageImpl));
    }
  }

  /**
   * Structure table admin provider to provide the {@link StructuredTableAdmin} class, the actual implementation
   * will be based on the configuration file.
   */
  @Singleton
  private static final class StructuredTableAdminProvider implements Provider<StructuredTableAdmin> {
    private final CConfiguration cConf;
    private final SConfiguration sConf;
    private final Injector injector;

    @Inject
    StructuredTableAdminProvider(CConfiguration cConf, SConfiguration sConf, Injector injector) {
      this.cConf = cConf;
      this.sConf = sConf;
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
        // TODO: CDAP-14780, connect to the sql using the connection, user name and password from the sConf
        return null;
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
  @Singleton
  private static final class StructuredTableRegistryProvider implements Provider<StructuredTableRegistry> {
    private final CConfiguration cConf;
    private final SConfiguration sConf;
    private final Injector injector;

    @Inject
    StructuredTableRegistryProvider(CConfiguration cConf, SConfiguration sConf, Injector injector) {
      this.cConf = cConf;
      this.sConf = sConf;
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
        return injector.getInstance(NoSqlStructuredTableRegistry.class);
      }
      if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_SQL)) {
        // TODO: CDAP-14780, connect to the sql using the connection, user name and password from the sConf
        return null;
      }
      throw new UnsupportedOperationException(
        String.format("%s is not a supported storage implementation, the supported implementations are %s and %s",
                      storageImpl, Constants.Dataset.DATA_STORAGE_NOSQL, Constants.Dataset.DATA_STORAGE_SQL));
    }
  }
}
