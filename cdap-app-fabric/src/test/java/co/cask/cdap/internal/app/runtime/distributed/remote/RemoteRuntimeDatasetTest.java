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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the {@link RemoteRuntimeDataset}.
 */
public class RemoteRuntimeDatasetTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static TransactionManager txManager;
  private static DatasetService datasetService;
  private static MessagingService messagingService;
  private static DatasetFramework datasetFramework;
  private static Transactional transactional;
  private static DynamicDatasetCache datasetCache;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, false);

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
    datasetFramework = injector.getInstance(DatasetFramework.class);
    StructuredTableAdmin tableAdmin = injector.getInstance(StructuredTableAdmin.class);
    StructuredTableRegistry registry = injector.getInstance(StructuredTableRegistry.class);
    try {
      StoreDefinition.createAllTables(tableAdmin, registry);
    } catch (IOException | TableAlreadyExistsException e) {
      throw new RuntimeException("Failed to create the system tables", e);
    }
    TransactionSystemClient txClient = injector.getInstance(TransactionSystemClient.class);
    datasetCache = new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
      NamespaceId.SYSTEM, ImmutableMap.of(), null, null);
    transactional = Transactions.createTransactionalWithRetry(Transactions.createTransactional(datasetCache),
                                                              RetryStrategies.retryOnConflict(20, 100));
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
  public void resetTest() throws IOException, DatasetManagementException {
    datasetCache.invalidate();
    datasetFramework.deleteInstance(AppMetadataStore.APP_META_INSTANCE_ID);
  }

  @Test
  public void testReadWrite() {
    ProgramRunId programRunId =
      NamespaceId.DEFAULT.app("app").program(ProgramType.SPARK, "spark").run(RunIds.generate().getId());
    ProgramOptions programOpts = new SimpleProgramOptions(programRunId.getParent(),
                                                          new BasicArguments(Collections.singletonMap("a", "b")),
                                                          new BasicArguments(Collections.singletonMap("x", "y")));

    // Write the state
    Transactionals.execute(transactional, context -> {
      RemoteRuntimeDataset dataset = RemoteRuntimeDataset.create(context, datasetFramework);
      dataset.write(programRunId, programOpts);
    });

    // Use scan to read it back
    Optional<Map.Entry<ProgramRunId, ProgramOptions>> result = Transactionals.execute(transactional, context -> {
      RemoteRuntimeDataset dataset = RemoteRuntimeDataset.create(context, datasetFramework);
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

      Transactionals.execute(transactional, context -> {
        RemoteRuntimeDataset dataset = RemoteRuntimeDataset.create(context, datasetFramework);
        dataset.write(programRunId, programOpts);
      });
    }

    // Scan with limit = 2
    AtomicReference<ProgramRunId> lastProgramRunId = new AtomicReference<>();
    Map<ProgramRunId, ProgramOptions> result = new HashMap<>();
    boolean scanCompleted = false;

    while (!scanCompleted) {
      scanCompleted = Transactionals.execute(transactional, context -> {
        RemoteRuntimeDataset dataset = RemoteRuntimeDataset.create(context, datasetFramework);
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
    Transactionals.execute(transactional, context -> {
      RemoteRuntimeDataset dataset = RemoteRuntimeDataset.create(context, datasetFramework);
      dataset.write(programRunId, programOpts);
    });

    // Delete the task info
    Transactionals.execute(transactional, context -> {
      RemoteRuntimeDataset dataset = RemoteRuntimeDataset.create(context, datasetFramework);
      dataset.delete(programRunId);
    });

    // Scan should be empty
    boolean result = Transactionals.execute(transactional, context -> {
      RemoteRuntimeDataset dataset = RemoteRuntimeDataset.create(context, datasetFramework);
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
