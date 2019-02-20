/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed.remote;

import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.master.spi.program.ProgramOptions;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the {@link RemoteRuntimeTable}.
 */
public class RemoteRuntimeTableTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static TransactionManager txManager;
  private static DatasetService datasetService;
  private static MessagingService messagingService;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, false);

    // TODO CDAP-14780 pass in cconf for sql test
    Injector injector = Guice.createInjector(new AppFabricTestModule(cConf));
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class), structuredTableRegistry);
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    transactionRunner = injector.getInstance(TransactionRunner.class);
  }

  @AfterClass
  public static void finish() {
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
    datasetService.stopAndWait();
    txManager.stopAndWait();
  }

  @After
  public void resetTest() {
    TransactionRunners.run(transactionRunner, context -> {
      RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
      dataset.deleteAll();
    });
  }

  @Test
  public void testReadWrite() {
    ProgramRunId programRunId =
      NamespaceId.DEFAULT.app("app").program(ProgramType.SPARK, "spark").run(RunIds.generate().getId());
    ProgramOptions programOpts = new SimpleProgramOptions(programRunId.getParent(),
                                                          new BasicArguments(Collections.singletonMap("a", "b")),
                                                          new BasicArguments(Collections.singletonMap("x", "y")));

    // Write the state
    TransactionRunners.run(transactionRunner, context -> {
      RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
      dataset.write(programRunId, programOpts);
    });

    // Use scan to read it back
    Optional<Map.Entry<ProgramRunId, ProgramOptions>> result = TransactionRunners.run(transactionRunner, context -> {
      RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
      return dataset.scan(1, null).stream().findFirst();
    });

    Map.Entry<ProgramRunId, ProgramOptions> entry = result.orElseThrow(IllegalStateException::new);
    Assert.assertEquals(programRunId, entry.getKey());
    Assert.assertTrue(programOptionsEquals(programOpts, entry.getValue()));
  }

  @Test
  public void testScan() {
    // Write 5 task info
    Map<ProgramRunId, ProgramOptions> expected = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      ProgramRunId programRunId =
        NamespaceId.DEFAULT.app("app").program(ProgramType.SPARK, "spark").run(RunIds.generate().getId());
      ProgramOptions programOpts = new SimpleProgramOptions(programRunId.getParent(),
                                                            new BasicArguments(Collections.singletonMap("a", "b" + i)),
                                                            new BasicArguments(Collections.singletonMap("x", "y" + i)));
      expected.put(programRunId, programOpts);

      TransactionRunners.run(transactionRunner, context -> {
        RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
        dataset.write(programRunId, programOpts);
      });
    }

    // Scan with limit = 2
    AtomicReference<ProgramRunId> lastProgramRunId = new AtomicReference<>();
    Map<ProgramRunId, ProgramOptions> result = new HashMap<>();
    boolean scanCompleted = false;

    while (!scanCompleted) {
      scanCompleted = TransactionRunners.run(transactionRunner, context -> {
        RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
        List<Map.Entry<ProgramRunId, ProgramOptions>> scanResult = dataset.scan(2, lastProgramRunId.get());
        for (Map.Entry<ProgramRunId, ProgramOptions> entry : scanResult) {
          result.put(entry.getKey(), entry.getValue());
          lastProgramRunId.set(entry.getKey());
        }
        return scanResult.isEmpty();
      });
    }

    // Compare results
    Assert.assertEquals(expected.size(), result.size());
    Assert.assertTrue(expected.entrySet().stream()
                        .allMatch(entry -> programOptionsEquals(entry.getValue(), result.get(entry.getKey()))));
  }

  @Test
  public void testDelete() {
    ProgramRunId programRunId =
      NamespaceId.DEFAULT.app("app").program(ProgramType.SPARK, "spark").run(RunIds.generate().getId());
    ProgramOptions programOpts = new SimpleProgramOptions(programRunId.getParent(),
                                                          new BasicArguments(Collections.singletonMap("a", "b")),
                                                          new BasicArguments(Collections.singletonMap("x", "y")));

    // Write a task info
    TransactionRunners.run(transactionRunner, context -> {
      RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
      dataset.write(programRunId, programOpts);
    });

    // Delete the task info
    TransactionRunners.run(transactionRunner, context -> {
      RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
      dataset.delete(programRunId);
    });

    // Scan should be empty
    boolean result = TransactionRunners.run(transactionRunner, context -> {
      RemoteRuntimeTable dataset = RemoteRuntimeTable.create(context);
      return dataset.scan(1, null).isEmpty();
    });

    Assert.assertTrue(result);
  }

  private boolean programOptionsEquals(ProgramOptions opt1, ProgramOptions opt2) {
    return Objects.equals(opt1.getProgramId(), opt2.getProgramId())
      && opt1.isDebug() == opt2.isDebug()
      && Objects.equals(opt1.getArguments().asMap(), opt2.getArguments().asMap())
      && Objects.equals(opt1.getUserArguments().asMap(), opt2.getUserArguments().asMap());
  }
}
