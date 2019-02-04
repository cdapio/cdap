/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.cdap.store;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocalLocationModule;
import co.cask.cdap.common.guice.NamespaceAdminTestModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.StorageModule;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.nosql.NoSqlStructuredTableAdmin;
import co.cask.cdap.data2.nosql.NoSqlTransactionRunner;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.OwnerStore;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests for {@link DefaultOwnerStore}.
 */
public class DefaultOwnerStoreTest extends OwnerStoreTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static OwnerStore ownerStore;

  @BeforeClass
  public static void setup() throws IOException, TableAlreadyExistsException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new DataSetsModules().getInMemoryModules(),
      new LocalLocationModule(),
      new TransactionInMemoryModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new NamespaceAdminTestModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new StorageModule()
    );

    injector.getInstance(TransactionManager.class).startAndWait();

    StructuredTableAdmin structuredTableAdmin = injector.getInstance(StructuredTableAdmin.class);
    TransactionRunner transactionRunner =
      new NoSqlTransactionRunner(injector.getInstance(NoSqlStructuredTableAdmin.class),
                                 injector.getInstance(TransactionSystemClient.class));
    StoreDefinition.OwnerStore.createTables(structuredTableAdmin);
    ownerStore = new DefaultOwnerStore(transactionRunner);
  }

  @Override
  public OwnerStore getOwnerStore() {
    return ownerStore;
  }
}
