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

package io.cdap.cdap.reporting;

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
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqlProgramHeartBeatTableTest extends ProgramHeartBeatTableTest {
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

  @AfterClass
  public static void teardown() throws IOException {
    pg.close();
  }

  @Test
  public void testDeleteRecordsBefore() {
    RunRecordDetail runRecord = RunRecordDetail.builder()
      .setProgramRunId(NamespaceId.DEFAULT.app("app").spark("spark").run(RunIds.generate()))
      .setStartTime(System.currentTimeMillis())
      .setSourceId(new byte[MessageId.RAW_ID_SIZE])
      .build();
    final Instant cutOffTime = Instant.now();
    final Instant timeBefore5min = cutOffTime.minus(Duration.ofMinutes(5));
    final Instant timeAfterCutOff = Instant.now().plus(Duration.ofMinutes(5));

    TransactionRunners.run(transactionRunner, context -> {
      ProgramHeartbeatTable programHeartbeatTable = new ProgramHeartbeatTable(context);
      //Insert 4 Records with different times
      programHeartbeatTable.writeRunRecordMeta(runRecord, timeBefore5min.getEpochSecond());
      programHeartbeatTable.writeRunRecordMeta(runRecord, timeBefore5min.getEpochSecond());
      programHeartbeatTable.writeRunRecordMeta(runRecord, cutOffTime.getEpochSecond());
      programHeartbeatTable.writeRunRecordMeta(runRecord, timeAfterCutOff.getEpochSecond());

      //Now delete all records <= cutOffTime
      programHeartbeatTable.deleteRecordsBefore(cutOffTime);
      Collection<RunRecordDetail> result =
        programHeartbeatTable.scan(0, Long.MAX_VALUE, new HashSet<>(Arrays.asList("default")));

      //This should contain only 1 record i.e. the last with timeAfterCutOff
      Assert.assertEquals(1, result.size());
    });
  }
}
