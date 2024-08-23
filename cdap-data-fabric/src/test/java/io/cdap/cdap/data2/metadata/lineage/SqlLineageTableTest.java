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

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import org.apache.twill.api.RunId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqlLineageTableTest extends LineageTableTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static EmbeddedPostgres pg;

  @BeforeClass
  public static void beforeClass() throws IOException, TableAlreadyExistsException {
    CConfiguration cConf = CConfiguration.create();
    pg = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocalLocationModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new StorageModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      }
    );

    transactionRunner = injector.getInstance(TransactionRunner.class);
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
  }


  @Test
  public void testDeleteOutOfRangeCompletedRuns() {
    final Instant currentTime = Instant.now();
    final RunId runId = RunIds.generate(10000);
    final DatasetId datasetInstance = new DatasetId("default", "dataset1");
    final ProgramId program = new ProgramId("default", "app1", ProgramType.SERVICE, "service1");
    final ProgramRunId run = program.run(runId.getId());
    final long accessTimeMillis = System.currentTimeMillis();
    TransactionRunners.run(transactionRunner, context -> {
      LineageTable lineageTable = LineageTable.create(context);
      lineageTable.addAccess(run, datasetInstance, AccessType.READ, accessTimeMillis);

      lineageTable.deleteCompletedLineageRecordsStartedBefore(currentTime);

      Set<NamespacedEntityId> entitiesForRun = lineageTable.getEntitiesForRun(run);
      // Asserts that the records are deleted.
      Assert.assertTrue(entitiesForRun.isEmpty());
    });
  }

  @Test
  public void testDeleteOutOfRangeCompletedRunsDoesNotDeleteLatestRuns() {
    final Instant currentTime = Instant.now();
    final RunId runId = RunIds.generate(currentTime.toEpochMilli());
    final DatasetId datasetInstance = new DatasetId("default", "dataset1");
    final ProgramId program = new ProgramId("default", "app1", ProgramType.SERVICE, "service1");
    final ProgramRunId run = program.run(runId.getId());


    final long accessTimeMillis = System.currentTimeMillis();
    TransactionRunners.run(transactionRunner, context -> {
      LineageTable lineageTable = LineageTable.create(context);
      lineageTable.addAccess(run, datasetInstance, AccessType.READ, accessTimeMillis);

      Instant deleteStartTime = currentTime.minus(10, ChronoUnit.HOURS);
      lineageTable.deleteCompletedLineageRecordsStartedBefore(deleteStartTime);

      Relation expected = new Relation(datasetInstance, program, AccessType.READ, runId);
      Set<Relation> relations = lineageTable.getRelations(datasetInstance, 0, currentTime.toEpochMilli(), x -> true);
      // Asserts that the records are not deleted.
      Assert.assertEquals(1, relations.size());
      Assert.assertEquals(expected, relations.iterator().next());
      Assert.assertEquals(toSet(program, datasetInstance), lineageTable.getEntitiesForRun(run));
      Assert.assertEquals(ImmutableList.of(accessTimeMillis), lineageTable.getAccessTimesForRun(run));
    });
  }


  @AfterClass
  public static void teardown() throws IOException {
    pg.close();
  }
}
