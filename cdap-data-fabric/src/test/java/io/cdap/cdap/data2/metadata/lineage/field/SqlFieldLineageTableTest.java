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

package io.cdap.cdap.data2.metadata.lineage.field;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.proto.ProgramType;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.twill.api.RunId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqlFieldLineageTableTest extends FieldLineageTableTest {
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
  public void testDeleteFieldRecordsBefore() {
    final Instant currentTime = Instant.now();

    RunId runId = RunIds.generate(10000);
    ProgramId program = new ProgramId("default", "app1", ProgramType.WORKFLOW, "workflow1");
    final ProgramRunId programRun1 = program.run(runId.getId());
    final FieldLineageInfo info1 = new FieldLineageInfo(generateOperations(false));

    TransactionRunners.run(transactionRunner, context -> {
      FieldLineageTable fieldLineageTable = FieldLineageTable.create(context);
      fieldLineageTable.addFieldLineageInfo(programRun1, info1);
      EndPoint source = EndPoint.of("ns1", "endpoint1");
      EndPoint destination = EndPoint.of("myns", "another_file");

      fieldLineageTable.deleteFieldRecordsBefore(currentTime);
      List<EndPoint> actual = fieldLineageTable.getEndpoints("ns1",
          program.getProgramReference(), runId);

      // Asserts that only the endpoint records are deleted.
      Assert.assertTrue(actual.isEmpty());
      // Since the base table is deleted, get fields also won't return anything.
      Assert.assertTrue(fieldLineageTable.getFields(destination, 0, 10001).isEmpty());
      Assert.assertTrue(fieldLineageTable.getFields(source, 0, 10001).isEmpty());

    });
  }

  @Test
  public void testDeleteFieldRecordsBeforeDoesNotDelete() {
    final Instant currentTime = Instant.now();

    RunId runId = RunIds.generate(
        currentTime.minus(1, ChronoUnit.MINUTES)
            .toEpochMilli());
    ProgramId program = new ProgramId("default", "app1", ProgramType.WORKFLOW, "workflow1");
    final ProgramRunId programRun1 = program.run(runId.getId());
    final FieldLineageInfo info1 = new FieldLineageInfo(generateOperations(false));

    TransactionRunners.run(transactionRunner, context -> {
      FieldLineageTable fieldLineageTable = FieldLineageTable.create(context);
      fieldLineageTable.addFieldLineageInfo(programRun1, info1);
      EndPoint source = EndPoint.of("ns1", "endpoint1");
      EndPoint destination = EndPoint.of("myns", "another_file");
      Instant deleteTime = currentTime.minus(2, ChronoUnit.HOURS);
      fieldLineageTable.deleteFieldRecordsBefore(deleteTime);
      List<EndPoint> actual = fieldLineageTable.getEndpoints("ns1",
          program.getProgramReference(), runId);
      List<EndPoint> expected = ImmutableList.of(EndPoint.of("ns1", "endpoint1"));
      // Asserts that only the endpoint records are deleted.
      Assert.assertEquals(expected, actual);

      Set<String> expectedDestinationFields = new HashSet<>(Arrays.asList("offset", "name"));
      Set<String> expectedSourceFields = new HashSet<>(Arrays.asList("offset", "body"));
      // End time of currentTime should return the data for the run which was added at time
      // currentTime - 1 minute.
      Assert.assertEquals(expectedDestinationFields,
          fieldLineageTable.getFields(destination, 0, currentTime.toEpochMilli()));
      Assert.assertEquals(expectedSourceFields, fieldLineageTable.getFields(source, 0, currentTime.toEpochMilli()));

    });
  }

  @AfterClass
  public static void teardown() throws IOException {
    pg.close();
  }
}
