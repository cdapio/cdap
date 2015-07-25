/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.IndexedTableDefinition;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDefinition;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetTableMigrator;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class PFSUpgraderTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static DatasetFramework framework;
  private static PFSUpgrader pfsUpgrader;

  private static DatasetProperties dsProps = PartitionedFileSetProperties.builder()
    .setPartitioning(Partitioning.builder().addStringField("league").addIntField("season").build())
    .setBasePath("/temp/fs2")
    .build();

  @BeforeClass
  public static void setup() throws Exception {
    framework = dsFrameworkUtil.getFramework();
    pfsUpgrader = new PFSUpgrader(
      // we can pass null for HBaseAdmin in test case, since we do not test it
      null,
      new PartitionedFileSetTableMigrator(null, null, framework),
      new TransactionExecutorFactory() {
        @Override
        public TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
          return dsFrameworkUtil.newTransactionExecutor(Iterables.toArray(txAwares, TransactionAware.class));
        }
      },
      framework
    );
  }


  @Test
  public void testSimplePFSUpgrade() throws Exception {
    Id.DatasetInstance ds1 = Id.DatasetInstance.from("test", "fs1");
    framework.addInstance(PartitionedFileSet.class.getName(), ds1,
                          FileSetProperties.builder().setBasePath("/temp/fs1").build());
    // PFS specs created by the current DSFramework already use an IndexedTable for the partitions dataset
    Assert.assertTrue(pfsUpgrader.alreadyUpgraded(framework.getDatasetSpec(ds1)));


    // test conversion from old PartitionedFileSet spec to current spec
    DatasetSpecification oldResultsSpec = constructOldPfsSpec("results", dsProps.getProperties(),
                                                              PartitionedFileSet.class.getName());
    testPFSUpgrade(oldResultsSpec);
  }

  @Test
  public void testSimpleTPFSUpgrade() throws Exception {
    Id.DatasetInstance ds1 = Id.DatasetInstance.from("test", "tfs1");
    framework.addInstance(TimePartitionedFileSet.class.getName(), ds1,
                          FileSetProperties.builder().setBasePath("/temp/fs1").build());
    // TPFS specs created by the current DSFramework already use an IndexedTable for the partitions dataset
    Assert.assertTrue(pfsUpgrader.alreadyUpgraded(framework.getDatasetSpec(ds1)));


    // test conversion from old TimePartitionedFileSet spec to current spec
    DatasetSpecification oldResultsSpec = constructOldPfsSpec("results", Collections.<String, String>emptyMap(),
                                                              TimePartitionedFileSet.class.getName());
    testPFSUpgrade(oldResultsSpec);

  }

  // does the conversion and assertions for the PFS and TPFS specifications
  private void testPFSUpgrade(DatasetSpecification oldPFSSpec) throws Exception {
    Assert.assertTrue(pfsUpgrader.needsConverting(oldPFSSpec));
    DatasetSpecification newResultsSpec = pfsUpgrader.convertSpec(oldPFSSpec.getName(), oldPFSSpec);

    // the files Dataset shouldn't be changed
    Assert.assertEquals(oldPFSSpec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME),
                        newResultsSpec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME));

    DatasetSpecification newPartitionsSpec =
      newResultsSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME);

    assertIsNewPartitionsTable(newPartitionsSpec);
  }

  @Test
  public void testEmbeddedPfsUpgrade() throws Exception {
    DatasetSpecification pfsSpec = constructOldPfsSpec("results", dsProps.getProperties(),
                                                       PartitionedFileSet.class.getName());

    DatasetSpecification embeddingDsSpec = DatasetSpecification.builder("outerDataset", "customDs")
      .datasets(pfsSpec)
      .build();

    Assert.assertTrue(pfsUpgrader.needsConverting(embeddingDsSpec));

    Multimap<Id.Namespace, DatasetSpecification> datasetInstances = HashMultimap.create();
    DatasetSpecification convertedSpec =
      pfsUpgrader.recursivelyMigrateSpec(Constants.DEFAULT_NAMESPACE_ID, embeddingDsSpec.getName(),
                                         embeddingDsSpec, datasetInstances);
    DatasetSpecification migratedEmbeddedPfsSpec = convertedSpec.getSpecification("results");
    DatasetSpecification newPartitionsSpec =
      migratedEmbeddedPfsSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME);
    assertIsNewPartitionsTable(newPartitionsSpec);
  }

  @Test
  public void testModuleUpgrade() throws Exception {
    DatasetModuleMeta oldModuleMetaNotNeedingUpgrade =
      new DatasetModuleMeta("myModule", "co.cask.cdap.examples.helloworld.MyModule",
                            null, ImmutableList.of("myType"), ImmutableList.of("cube"));

    Assert.assertFalse(pfsUpgrader.needsConverting(oldModuleMetaNotNeedingUpgrade));


    DatasetModuleMeta oldModuleMeta =
      new DatasetModuleMeta("myOtherModule", "co.cask.cdap.examples.helloworld.MyOtherModule",
                            null, ImmutableList.of("myType"), ImmutableList.of("partitionedFileSet"));
    Assert.assertTrue(pfsUpgrader.needsConverting(oldModuleMeta));
    DatasetModuleMeta newModuleMeta = pfsUpgrader.migrateDatasetModuleMeta(oldModuleMeta);

    List<String> usesModules = newModuleMeta.getUsesModules();
    Assert.assertTrue(usesModules.contains("orderedTable-hbase"));
    Assert.assertTrue(usesModules.contains("core"));
    Assert.assertTrue(usesModules.indexOf("orderedTable-hbase") < usesModules.indexOf("core"));
  }

  private void assertIsNewPartitionsTable(DatasetSpecification dsSpec) {
    // the new partitions should be an IndexedTable and should be set to index appropriately
    Assert.assertEquals(IndexedTable.class.getName(), dsSpec.getType());
    Assert.assertEquals(PartitionedFileSetDefinition.INDEXED_COLS,
                        dsSpec.getProperty(IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY));

    // the embedded tables of Indexed Table should be named 'i' and 'd', and be of type Table
    SortedMap<String, DatasetSpecification> indexedTableEmbeddedTables = dsSpec.getSpecifications();
    Assert.assertEquals(ImmutableSet.of("i", "d"), indexedTableEmbeddedTables.keySet());
    for (DatasetSpecification datasetSpecification : indexedTableEmbeddedTables.values()) {
      Assert.assertEquals(Table.class.getName(), datasetSpecification.getType());
    }
  }

  private DatasetSpecification constructOldPfsSpec(String name, Map<String, String> properties, String typeName) {
    DatasetSpecification.Builder pfsBuilder = DatasetSpecification.builder(name, typeName);
    pfsBuilder.properties(properties);

    DatasetSpecification filesSpec =
      DatasetSpecification.builder(PartitionedFileSetDefinition.FILESET_NAME, FileSet.class.getName())
        .properties(properties)
        .build();

    DatasetSpecification partitionsSpec =
      DatasetSpecification.builder(PartitionedFileSetDefinition.PARTITION_TABLE_NAME, Table.class.getName())
        .properties(properties)
        .build();

    pfsBuilder.datasets(filesSpec, partitionsSpec);
    return pfsBuilder.build();
  }


  @Test
  public void testSpecRename() {
    DatasetSpecification outerSpec = constructOldPfsSpec("myDs", ImmutableMap.<String, String>of(),
                                                         "partitionedFileSet");
    DatasetSpecification embeddedSpec1 = outerSpec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME);
    DatasetSpecification embeddedSpec2 = outerSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME);
    Assert.assertEquals("myDs.files", embeddedSpec1.getName());
    Assert.assertEquals("myDs.partitions", embeddedSpec2.getName());

    DatasetSpecification renamedSpec = pfsUpgrader.changeName(outerSpec, "yourDs");
    embeddedSpec1 = renamedSpec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME);
    embeddedSpec2 = renamedSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME);
    Assert.assertEquals("yourDs.files", embeddedSpec1.getName());
    Assert.assertEquals("yourDs.partitions", embeddedSpec2.getName());
  }
}
