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

package io.cdap.cdap.data2.metadata.lineage;

import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.time.Instant;
import java.util.Set;
import org.apache.twill.api.RunId;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class NoSqlLineageTableTest extends LineageTableTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @BeforeClass
  public static void beforeClass() throws IOException, TableAlreadyExistsException {
    CConfiguration cConf = dsFrameworkUtil.getConfiguration();
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);

    StructuredTableAdmin structuredTableAdmin = dsFrameworkUtil.getInjector().getInstance(StructuredTableAdmin.class);
    transactionRunner = dsFrameworkUtil.getInjector().getInstance(TransactionRunner.class);
    StoreDefinition.createAllTables(structuredTableAdmin);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeleteCompletedLineageRecordsThrowsException() {
    final Instant currentTime = Instant.now();
    final RunId runId = RunIds.generate(10000);
    final DatasetId datasetInstance = new DatasetId("default", "dataset1");
    final ProgramId program = new ProgramId("default", "app1", ProgramType.SERVICE, "service1");
    final ProgramRunId run = program.run(runId.getId());

    final long accessTimeMillis = System.currentTimeMillis();
    TransactionRunners.run(transactionRunner, context -> {
      LineageTable lineageTable = LineageTable.create(context);
      lineageTable.addAccess(run, datasetInstance, AccessType.READ, accessTimeMillis);
      Set<NamespacedEntityId> entitiesForRun = lineageTable.getEntitiesForRun(run);
      // Since no-SQL DBs are not supported for time based deletes, this should throw an
      // UnsupportedOperationException.
      lineageTable.deleteCompletedLineageRecordsStartedBefore(currentTime);
    });
  }
}
