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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.nosql.NoSqlStructuredTableAdmin;
import co.cask.cdap.data2.nosql.NoSqlStructuredTableRegistry;
import co.cask.cdap.data2.nosql.NoSqlTransactionRunner;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.store.StoreDefinition;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;

public class NoSqlLineageTableTest extends LineageTableTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @BeforeClass
  public static void beforeClass() throws IOException, TableAlreadyExistsException {
    StructuredTableAdmin structuredTableAdmin =
      dsFrameworkUtil.getInjector().getInstance(NoSqlStructuredTableAdmin.class);
    transactionRunner = dsFrameworkUtil.getInjector().getInstance(NoSqlTransactionRunner.class);
    NoSqlStructuredTableRegistry registry =
      dsFrameworkUtil.getInjector().getInstance(NoSqlStructuredTableRegistry.class);
    registry.initialize();
    StoreDefinition.createAllTables(structuredTableAdmin, registry);
  }

}
