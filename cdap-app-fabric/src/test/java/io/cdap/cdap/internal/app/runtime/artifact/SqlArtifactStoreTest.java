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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.base.Joiner;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class SqlArtifactStoreTest extends ArtifactStoreTest {

  private static EmbeddedPostgres pg;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    // any plugin which requires transaction will be excluded
    cConf.set(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, Joiner.on(",").join(Table.TYPE, KeyValueTable.TYPE));
    pg = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());

    Injector injector = AppFabricTestHelper.getInjector(cConf);

    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    artifactStore = new ArtifactStore(cConf,
                                      injector.getInstance(NamespacePathLocator.class),
                                      injector.getInstance(LocationFactory.class),
                                      injector.getInstance(Impersonator.class),
                                      transactionRunner);
    StoreDefinition.ArtifactStore.create(injector.getInstance(StructuredTableAdmin.class));
  }

  @AfterClass
  public static void afterClass() throws IOException {
    pg.close();
    AppFabricTestHelper.shutdown();
  }
}
