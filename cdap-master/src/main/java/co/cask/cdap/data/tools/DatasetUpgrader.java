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

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceMDS;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.data2.util.hbase.HTableNameConverterFactory;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Handles upgrade for System and User Datasets
 */
public class DatasetUpgrader extends AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetUpgrader.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final HBaseTableUtil hBaseTableUtil;
  private final DatasetFramework dsFramework;
  private final TransactionExecutorFactory transactionExecutorFactory;
  private final Pattern userTablePrefix;

  @Inject
  @VisibleForTesting
  DatasetUpgrader(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory,
                  NamespacedLocationFactory namespacedLocationFactory,
                  HBaseTableUtil hBaseTableUtil, DatasetFramework dsFramework,
                  TransactionExecutorFactory transactionExecutorFactory) {

    super(locationFactory, namespacedLocationFactory);
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.hBaseTableUtil = hBaseTableUtil;
    this.dsFramework = dsFramework;
    this.transactionExecutorFactory = transactionExecutorFactory;
    this.userTablePrefix = Pattern.compile(String.format("^%s\\.user\\..*", cConf.get(Constants.Dataset.TABLE_PREFIX)));
  }

  @Override
  public void upgrade() throws Exception {
    // Upgrade system dataset
    upgradeSystemDatasets();

    // Upgrade all user hbase tables
    upgradeUserTables();

    // Upgrade all file sets
    upgradeFileSets();
  }

  private void upgradeSystemDatasets() throws Exception {

    // Upgrade all datasets in system namespace
    for (DatasetSpecificationSummary spec : dsFramework.getInstances(Constants.DEFAULT_NAMESPACE_ID)) {
      LOG.info("Upgrading dataset: {}, spec: {}", spec.getName(), spec.toString());
      DatasetAdmin admin = dsFramework.getAdmin(Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE_ID, spec.getName()),
                                                null);
      // we know admin is not null, since we are looping over existing datasets
      admin.upgrade();
      LOG.info("Upgraded dataset: {}", spec.getName());
    }
  }

  private void upgradeUserTables() throws Exception {
    HBaseAdmin hAdmin = new HBaseAdmin(hConf);

    for (HTableDescriptor desc : hAdmin.listTables(userTablePrefix)) {
      HTableNameConverter hTableNameConverter = new HTableNameConverterFactory().get();
      TableId tableId = hTableNameConverter.from(desc);
      LOG.info("Upgrading hbase table: {}, desc: {}", tableId, desc);

      final boolean supportsIncrement = HBaseTableAdmin.supportsReadlessIncrements(desc);
      final boolean transactional = HBaseTableAdmin.isTransactional(desc);
      DatasetAdmin admin = new AbstractHBaseDataSetAdmin(tableId, hConf, hBaseTableUtil) {
        @Override
        protected CoprocessorJar createCoprocessorJar() throws IOException {
          return HBaseTableAdmin.createCoprocessorJarInternal(cConf,
                                                              locationFactory,
                                                              hBaseTableUtil,
                                                              transactional,
                                                              supportsIncrement);
        }

        @Override
        protected boolean upgradeTable(HTableDescriptor tableDescriptor) {
          // we don't do any other changes apart from coprocessors upgrade
          return false;
        }

        @Override
        public void create() throws IOException {
          // no-op
          throw new UnsupportedOperationException("This DatasetAdmin is only used for upgrade() operation");
        }
      };
      admin.upgrade();
      LOG.info("Upgraded hbase table: {}", tableId);
    }
  }

  /**
   * Upgrades all file sets and all datasets that contain an embedded file set: If the properties contain
   * an absolute base path, convert that into a relative base path. This is because since 2.8, with the
   * introduction of namespaces, absolute paths have incorrectly been treated the same as realtive paths,
   * that is, relative to the namespace's data path. This was fixed in 3.1, but that means that existing
   * file set with absolute paths now do not point to their data anymore. Therefore the upgrade turns all
   * absolute paths into relative paths by removing the leading "/", which is equivalent under the new
   * semantics.
   */
  private void upgradeFileSets() throws Exception {
    final DatasetInstanceMDS mds;
    try {
      mds = new DatasetMetaTableUtil(dsFramework).getInstanceMetaTable();
    } catch (Exception e) {
      LOG.error("Failed to access Datasets instances meta table.");
      throw e;
    }
    TransactionExecutor executor = transactionExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) mds));
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        MDSKey key = new MDSKey.Builder().add(DatasetInstanceMDS.INSTANCE_PREFIX).build();
        Map<MDSKey, DatasetSpecification> specs = mds.listKV(key, DatasetSpecification.class);
        for (Map.Entry<MDSKey, DatasetSpecification> entry : specs.entrySet()) {
          DatasetSpecification spec = entry.getValue();
          String absolutePath = containsFileSetWithAbsolutePath(spec);
          if (absolutePath != null) {
            Id.DatasetInstance datasetId = toDatasetId(entry.getKey());
            LOG.info("Migrating spec for dataset {} because it contains a FileSet with absolute path: {}",
                     datasetId, absolutePath);
            DatasetSpecification newSpec = migrateDatasetSpec(spec.getName(), spec);
            mds.write(entry.getKey(), newSpec);
          }
        }
      }
    });
  }

  @VisibleForTesting
  DatasetSpecification migrateDatasetSpec(DatasetSpecification spec) {
    return migrateDatasetSpec(spec.getName(), spec);
  }

  /**
   * Recursively scans the spec for file sets with an absolute base path, and if so, strips the leading / char.
   * @param datasetName The name to use when building the new spec. This is needed for recursive calls: The specs
   *                    of embedded datasets have the name (parent name).(embedded name), whereas when creating a
   *                    new spec with embedded data sets, their specs must have just the (embedded name) - the
   *                    DatasetSpecification.Builder.build() prepends (parent name). to the name of each sub-spec.
   *                    This is a bit confusing but that is how the DatasetSpecification API is defined.
   */
  private DatasetSpecification migrateDatasetSpec(String datasetName, DatasetSpecification spec) {
    Map<String, String> oldProperties = spec.getProperties();
    Map<String, String> newProperties = oldProperties; // delay copying of properties until we know we have to

    // This could be a FileSet, or it could be a dataset that embeds a FileSet. In the second case, it may or may
    // not have a base path property. If it does, we assume that this was propagated to the embedded file set,
    // and we remove the leading / from it (even though most likely, it is not needed here, we want it to be
    // consistent with the base path property of the embedded file set). But if this is not a FileSet and does not
    // embed one either, then we don't want to change its properties.
    if (containsFileSetWithAbsolutePath(spec) != null) {
      String basePath = FileSetProperties.getBasePath(spec.getProperties());
      if (basePath != null && basePath.startsWith("/")) {
        int idx = 1;
        while (basePath.length() > idx && basePath.charAt(idx) == '/') {
          idx++;
        }
        basePath = basePath.substring(idx);
        newProperties = new HashMap<>(oldProperties); // now we have to copy the properties to a new map
        newProperties.put(FileSetProperties.BASE_PATH, basePath);
      }
    }
    List<DatasetSpecification> subSpecs = new ArrayList<>(spec.getSpecifications().size());
    for (Map.Entry<String, DatasetSpecification> entry : spec.getSpecifications().entrySet()) {
      DatasetSpecification oldSpec = entry.getValue();
      // note that the key is the unqualified name of the embedded dataset, whereas its spec will
      // have the name prefixed with the name of the parent dataset. Therefore pass in the key
      // as the name to use for the returned spec. See comment above.
      DatasetSpecification newSpec = migrateDatasetSpec(entry.getKey(), oldSpec);
      subSpecs.add(newSpec);
    }
    return DatasetSpecification
      .builder(datasetName, spec.getType())
      .datasets(subSpecs)
      .properties(newProperties)
      .build();
  }

  private boolean isFileSet(DatasetSpecification spec) {
    return spec.getType().equals(FileSet.class.getName()) || spec.getType().equals("fileSet");
  }

  @VisibleForTesting
  String containsFileSetWithAbsolutePath(DatasetSpecification spec) {
    if (isFileSet(spec)) {
      String basePath = FileSetProperties.getBasePath(spec.getProperties());
      return (basePath != null && basePath.startsWith("/")) ? basePath : null;
    }
    for (DatasetSpecification subSpec : spec.getSpecifications().values()) {
      String absolutePath = containsFileSetWithAbsolutePath(subSpec);
      if (absolutePath != null) {
        return absolutePath;
      }
    }
    return null;
  }

  private static Id.DatasetInstance toDatasetId(MDSKey key) {
    MDSKey.Splitter splitter = key.split();
    splitter.skipString();
    return Id.DatasetInstance.from(splitter.getString(), splitter.getString());
  }
}
