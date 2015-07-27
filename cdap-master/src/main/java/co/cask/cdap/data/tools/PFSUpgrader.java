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

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.IndexedTableDefinition;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceMDS;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetTypeMDS;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDefinition;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetTableMigrator;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Migrates all dataset specifications that are for PartitionedFileSet or have an embedded PartitionedFileSet,
 * from the pre-3.1.0 format to the 3.1.0 format. The main change is that the embedded partitions table is now an
 * IndexedTable.
 */
public class PFSUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(PFSUpgrader.class);
  protected final HBaseTableUtil tableUtil;
  private final PartitionedFileSetTableMigrator pfsTableMigrator;
  private final TransactionExecutorFactory executorFactory;
  private final DatasetFramework dsFramework;

  @Inject
  protected PFSUpgrader(HBaseTableUtil tableUtil, PartitionedFileSetTableMigrator pfsTableMigrator,
                        final TransactionExecutorFactory executorFactory, final DatasetFramework dsFramework) {
    this.tableUtil = tableUtil;
    this.pfsTableMigrator = pfsTableMigrator;
    this.executorFactory = executorFactory;
    this.dsFramework = dsFramework;
  }

  public void upgrade() throws Exception {
    LOG.info("Begin upgrade of PartitionedFileSets.");
    upgradePartitionedFileSets();
    upgradeModulesDependingOnPfs();
    LOG.info("Completed upgrade of PartitionedFileSets.");
  }

  @VisibleForTesting
  DatasetSpecification convertSpec(String dsName, DatasetSpecification dsSpec) {
    Preconditions.checkArgument(isPartitionedFileSet(dsSpec));

    DatasetSpecification oldPartitionsSpec = dsSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME);
    DatasetSpecification oldFileSetSpec = dsSpec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME);

    Map<String, String> partitionsTableProperties = oldPartitionsSpec.getProperties();
    Map<String, String> newPartitionsTableProperties = Maps.newHashMap(partitionsTableProperties);
    newPartitionsTableProperties.put(IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY,
                                     PartitionedFileSetDefinition.INDEXED_COLS);

    DatasetSpecification dataTable = DatasetSpecification.builder("d", Table.class.getName())
      .properties(newPartitionsTableProperties)
      .build();

    DatasetSpecification indexTable = DatasetSpecification.builder("i", Table.class.getName())
      .properties(newPartitionsTableProperties)
      .build();

    DatasetSpecification newPartitionsSpec =
      DatasetSpecification.builder(PartitionedFileSetDefinition.PARTITION_TABLE_NAME, IndexedTable.class.getName())
        .properties(newPartitionsTableProperties)
        .datasets(dataTable, indexTable)
        .build();

    // we need to do this to effectively "un-namespace" the name of the embedded fileset.
    DatasetSpecification newFileSetSpec =
      DatasetSpecification.builder(PartitionedFileSetDefinition.FILESET_NAME, FileSet.class.getName())
        .properties(oldFileSetSpec.getProperties())
        .build();


    return DatasetSpecification.builder(dsName, dsSpec.getType())
      .properties(dsSpec.getProperties())
      .datasets(newPartitionsSpec, newFileSetSpec)
      .build();
  }

  private void upgradePartitionedFileSets() throws Exception {
    final DatasetInstanceMDS dsInstancesMDS;
    try {
      dsInstancesMDS = new DatasetMetaTableUtil(dsFramework).getInstanceMetaTable();
    } catch (Exception e) {
      LOG.error("Failed to access Datasets instances meta table.");
      throw e;
    }

    TransactionExecutor executor = executorFactory.createExecutor(ImmutableList.of((TransactionAware) dsInstancesMDS));
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        MDSKey key = new MDSKey.Builder().add(DatasetInstanceMDS.INSTANCE_PREFIX).build();
        Map<MDSKey, DatasetSpecification> dsSpecs = dsInstancesMDS.listKV(key, DatasetSpecification.class);

        // first, upgrade all the specifications, while keeping track of the tables that need migrating
        Multimap<Id.Namespace, DatasetSpecification> partitionDatasetsToMigrate = HashMultimap.create();
        for (Map.Entry<MDSKey, DatasetSpecification> entry : dsSpecs.entrySet()) {
          DatasetSpecification dsSpec = entry.getValue();
          if (!needsConverting(dsSpec)) {
            continue;
          }
          DatasetSpecification migratedSpec = recursivelyMigrateSpec(extractNamespace(entry.getKey()), dsSpec.getName(),
                                                                     dsSpec, partitionDatasetsToMigrate);
          dsInstancesMDS.write(entry.getKey(), migratedSpec);
        }

        // migrate the necessary tables
        LOG.info("Tables to migrate: {}", partitionDatasetsToMigrate);
        for (Map.Entry<Id.Namespace, DatasetSpecification> entry : partitionDatasetsToMigrate.entries()) {
          pfsTableMigrator.upgrade(entry.getKey(), entry.getValue());
        }
      }
    });
  }

  // need to add 'orderedTable-hbase' and 'core' as module dependencies for any modules that currently depend on
  // partitionedFileSet or timePartitionedFileSet as a resolution to: https://issues.cask.co/browse/CDAP-3030
  private void upgradeModulesDependingOnPfs() throws Exception {
    final DatasetTypeMDS dsTypeMDS;
    try {
      dsTypeMDS = new DatasetMetaTableUtil(dsFramework).getTypeMetaTable();
    } catch (Exception e) {
      LOG.error("Failed to access Datasets types meta table.");
      throw e;
    }

    TransactionExecutor executor = executorFactory.createExecutor(ImmutableList.of((TransactionAware) dsTypeMDS));
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        MDSKey key = new MDSKey.Builder().add(DatasetTypeMDS.MODULES_PREFIX).build();
        Map<MDSKey, DatasetModuleMeta> dsSpecs = dsTypeMDS.listKV(key, DatasetModuleMeta.class);

        for (Map.Entry<MDSKey, DatasetModuleMeta> entry : dsSpecs.entrySet()) {
          DatasetModuleMeta moduleMeta = entry.getValue();
          if (!needsConverting(moduleMeta)) {
            continue;
          }
          LOG.info("Migrating dataset module meta: {}", moduleMeta);
          DatasetModuleMeta migratedModuleMeta = migrateDatasetModuleMeta(moduleMeta);
          dsTypeMDS.write(entry.getKey(), migratedModuleMeta);
        }
      }
    });
  }

  @VisibleForTesting
  boolean needsConverting(DatasetModuleMeta moduleMeta) {
    // if 'orderedTable-hbase' and 'core' are already declared dependencies, no need to upgrade the module meta
    List<String> usesModules = moduleMeta.getUsesModules();
    if (usesModules.contains("orderedTable-hbase") && usesModules.contains("core")) {
      return false;
    }
    return usesModules.contains("partitionedFileSet") || usesModules.contains("timePartitionedFileSet");
  }

  @VisibleForTesting
  DatasetModuleMeta migrateDatasetModuleMeta(DatasetModuleMeta moduleMeta) {
    List<String> newUsesModules = new ArrayList<>();
    // ordering of the usesModules is important. They are loaded in order of the elements' indices
    // add the following two modules as the first two dependencies, and then simply exclude them from the existing
    // declared dependencies to avoid duplicate declared dependencies.
    newUsesModules.add("orderedTable-hbase");
    newUsesModules.add("core");
    for (String usesModule : moduleMeta.getUsesModules()) {
      if (!"orderedTable-hbase".equals(usesModule) && !"core".equals(usesModule)) {
        newUsesModules.add(usesModule);
      }
    }
    DatasetModuleMeta migratedModuleMeta = new DatasetModuleMeta(moduleMeta.getName(),
                                                                 moduleMeta.getClassName(),
                                                                 moduleMeta.getJarLocation(),
                                                                 moduleMeta.getTypes(),
                                                                 newUsesModules);
    for (String usedByModule : moduleMeta.getUsedByModules()) {
      migratedModuleMeta.addUsedByModule(usedByModule);
    }
    return migratedModuleMeta;
  }

  @VisibleForTesting
  boolean needsConverting(DatasetSpecification dsSpec) {
    if (isPartitionedFileSet(dsSpec) && !alreadyUpgraded(dsSpec)) {
      return true;
    }
    for (DatasetSpecification datasetSpecification : dsSpec.getSpecifications().values()) {
      if (needsConverting(datasetSpecification)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Recursively search the datasets of a Dataset for {@link PartitionedFileSet}s. Return the migrated dataset spec,
   * and add the DatasetInstances of the partitions table that require data migration to the given list.
   *
   * @return recursively migrated dsSpec
   */
  DatasetSpecification recursivelyMigrateSpec(Id.Namespace namespaceId, String dsName, DatasetSpecification dsSpec,
                                              Multimap<Id.Namespace, DatasetSpecification> addTo) throws Exception {
    if (isPartitionedFileSet(dsSpec)) {
      if (alreadyUpgraded(dsSpec)) {
        LOG.info("The partitions table of Dataset '{}' has already been upgraded to an IndexedTable.",
                 dsSpec.getName());
        return dsSpec;
      }
      DatasetSpecification convertedSpec = convertSpec(dsName, dsSpec);
      // the pfs spec that is passed to the PartitionedFileSetTableMigrator must have a name that is namespaced by its
      // embedding dsSpec (if any) in order to accurately match the hbase table. However, the convertedSpec that is
      // returned simply has the short name (not namespaced by embedding spec) because it is yet to be used in a
      // DatasetSpecification#Builder
      addTo.put(namespaceId, changeName(convertedSpec, dsSpec.getName()));
      return convertedSpec;
    }

    List<DatasetSpecification> newSpecs = Lists.newArrayList();
    for (Map.Entry<String, DatasetSpecification> entry : dsSpec.getSpecifications().entrySet()) {
      newSpecs.add(recursivelyMigrateSpec(namespaceId, entry.getKey(), entry.getValue(), addTo));
    }

    DatasetSpecification.Builder builder = DatasetSpecification.builder(dsName, dsSpec.getType());
    builder.properties(dsSpec.getProperties());
    builder.datasets(newSpecs);
    return builder.build();
  }

  // modifies the name of a dataset, while propogating this name change to the embedded datasets.
  @VisibleForTesting
  DatasetSpecification changeName(DatasetSpecification dsSpec, String newName) {
    DatasetSpecification.Builder builder = DatasetSpecification.builder(newName, dsSpec.getType());
    builder.properties(dsSpec.getProperties());
    SortedMap<String, DatasetSpecification> specifications = dsSpec.getSpecifications();
    for (Map.Entry<String, DatasetSpecification> entry : specifications.entrySet()) {
      builder.datasets(changeName(entry.getValue(), entry.getKey()));
    }
    return builder.build();
  }

  private static Id.Namespace extractNamespace(MDSKey key) {
    MDSKey.Splitter splitter = key.split();
    splitter.skipString();
    return Id.Namespace.from(splitter.getString());
  }

  boolean alreadyUpgraded(DatasetSpecification dsSpec) {
    DatasetSpecification partitionsSpec = dsSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME);
    // the partitions table is now an IndexedTable (was Table in < v3.1.0 CDAP)
    return isIndexedTable(partitionsSpec);
  }

  boolean isPartitionedFileSet(DatasetSpecification dsSpec) {
    String dsType = dsSpec.getType();
    return PartitionedFileSet.class.getName().equals(dsType) || "partitionedFileSet".equals(dsType)
      || TimePartitionedFileSet.class.getName().equals(dsType) || "timePartitionedFileSet".equals(dsType);
  }

  boolean isIndexedTable(DatasetSpecification dsSpec) {
    String dsType = dsSpec.getType();
    return (IndexedTable.class.getName().equals(dsType) || "indexedTable".equals(dsType));
  }
}
