/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.internal.provision.ProvisionerNotifier;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import com.google.inject.Injector;
import org.apache.tephra.TransactionExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests program run state persistence.
 */
public class ProgramNotificationSubscriberServiceTest {
  private static Table appMetaTable;
  private static AppMetadataStore metadataStoreDataset;
  private static TransactionExecutor txnl;
  private static ProgramStateWriter programStateWriter;
  private static ProvisionerNotifier provisionerNotifier;

  @BeforeClass
  public static void setupClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    CConfiguration cConf = injector.getInstance(CConfiguration.class);

    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    TransactionExecutorFactory txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);

    DatasetId storeTable = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
    appMetaTable = DatasetsUtil.getOrCreateDataset(datasetFramework, storeTable, Table.class.getName(),
                                                   DatasetProperties.EMPTY, Collections.emptyMap());
    metadataStoreDataset = new AppMetadataStore(appMetaTable, cConf);
    txnl = txExecutorFactory.createExecutor(Collections.singleton(metadataStoreDataset));

    programStateWriter = injector.getInstance(ProgramStateWriter.class);
    provisionerNotifier = injector.getInstance(ProvisionerNotifier.class);
  }

  @After
  public void cleanupTest() throws Exception {
    txnl.execute(() -> {
      Scanner scanner = appMetaTable.scan(null, null);
      Row row;
      while ((row = scanner.next()) != null) {
        appMetaTable.delete(row.getRow());
      }
    });
  }

  @Test
  public void testAppSpecNotRequiredToWriteState() throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("someapp").program(ProgramType.SERVICE, "s");
    Map<String, String> systemArguments = new HashMap<>();
    systemArguments.put(ProgramOptionConstants.SKIP_PROVISIONING, Boolean.TRUE.toString());
    ProgramOptions programOptions = new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                                             new BasicArguments());
    ProgramRunId runId = programId.run(RunIds.generate());
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();

    Cluster cluster = new Cluster("name", ClusterStatus.RUNNING, Collections.emptyList(), Collections.emptyMap());
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
      "name", "1.0.0", "desc", null, artifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);
    provisionerNotifier.provisioning(runId, programOptions, programDescriptor, "Bob");
    provisionerNotifier.provisioned(runId, programOptions, programDescriptor, "Bob", cluster);

    Tasks.waitFor(ProgramRunStatus.PENDING, () -> txnl.execute(() -> {
                    RunRecordMeta meta = metadataStoreDataset.getRun(runId);
                    if (meta == null) {
                      return null;
                    }
                    Assert.assertEquals(artifactId, meta.getArtifactId());
                    return meta.getStatus();
                  }),
                  10, TimeUnit.SECONDS);
  }
}
