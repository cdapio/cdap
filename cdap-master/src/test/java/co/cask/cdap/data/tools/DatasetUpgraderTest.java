/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.InMemoryOwnerStore;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerStore;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;

/**
 * Test DatasetUpgrader class.
 */
public class DatasetUpgraderTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCreateTableDatasetMap() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File dataDir = new File(TMP_FOLDER.newFolder(), "data");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, dataDir.getAbsolutePath());

    final Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new NonCustomLocationUnitTestModule().getModule(),
      new TransactionInMemoryModule(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuditModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(OwnerStore.class).to(InMemoryOwnerStore.class).in(Scopes.SINGLETON);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      }
    );
    DatasetDefinitionRegistryFactory registryFactory = new DatasetDefinitionRegistryFactory() {
      @Override
      public DatasetDefinitionRegistry create() {
        DefaultDatasetDefinitionRegistry registry = new DefaultDatasetDefinitionRegistry();
        injector.injectMembers(registry);
        return registry;
      }
    };

    DatasetFramework dsFramework =
      new InMemoryDatasetFramework(registryFactory,
                                   ImmutableMap.of("in-memory", new InMemoryTableModule(),
                                                   "core", new CoreDatasetsModule(),
                                                   "file", new FileSetModule(),
                                                   "pfs", new PartitionedFileSetModule()));
    NamespaceId ns = new NamespaceId("ns");
    DatasetModuleId multiDatasetType = ns.datasetModule("multi");
    dsFramework.addModule(multiDatasetType, new SingleTypeModule(MultiLevelDataset.class));

    // Create some system tables
    // TODO

    // Create some user tables in default namespace
    DatasetId table1 = NamespaceId.DEFAULT.dataset("table1");
    dsFramework.addInstance("table", table1, DatasetProperties.EMPTY);
    DatasetId idxTable1 = NamespaceId.DEFAULT.dataset("idx-table1");
    dsFramework.addInstance("indexedTable", idxTable1,
                            DatasetProperties.builder().add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "col1").build());

    // Create some user tables in ns namespace
    DatasetId nsTable1 = ns.dataset("nsTable1");
    dsFramework.addInstance("table", nsTable1, DatasetProperties.EMPTY);
    // Create multi-dataset
    DatasetId nsMulti1 = ns.dataset("nsMulti1");
    DatasetProperties pfsProps = PartitionedFileSetProperties.builder().setPartitioning(Partitioning.builder()
                                                                                       .addStringField("s")
                                                                                       .build()).build();
    dsFramework.addInstance(MultiLevelDataset.class.getName(), nsMulti1,
                            DatasetProperties.builder()
                              .addAll(pfsProps.getProperties())
                              .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "col1")
                              .build());

    Map<String, DatasetId> expected =
      ImmutableMap.of("table1", table1, "idx-table1.d", idxTable1, "idx-table1.i", idxTable1);
    Map<String, DatasetId> actual = DatasetUpgrader.createTableDatasetMap(dsFramework, NamespaceId.DEFAULT);
    Assert.assertEquals(expected, actual);

    expected =
      ImmutableMap.of("nsTable1", nsTable1,
                      "nsMulti1.index-table.d", nsMulti1, "nsMulti1.index-table.i", nsMulti1,
                      "nsMulti1.files.partitions.d", nsMulti1, "nsMulti1.files.partitions.i", nsMulti1);
    actual = DatasetUpgrader.createTableDatasetMap(dsFramework, ns);
    Assert.assertEquals(expected, actual);
  }
}
