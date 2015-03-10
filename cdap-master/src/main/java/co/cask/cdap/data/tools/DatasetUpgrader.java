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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceMDS;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.data2.util.hbase.HTableNameConverterFactory;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Handles upgrade for System and User Datasets
 */
public class DatasetUpgrader extends AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetUpgrader.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final QueueAdmin queueAdmin;
  private final HBaseTableUtil hBaseTableUtil;
  private final DatasetFramework dsFramework;
  private static final Pattern USER_TABLE_PREFIX = Pattern.compile("^cdap\\.user\\..*");

  private final Transactional<UpgradeMdsStores<DatasetInstanceMDS>, DatasetInstanceMDS> datasetInstanceMds;

  @Inject
  private DatasetUpgrader(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory,
                          QueueAdmin queueAdmin, HBaseTableUtil hBaseTableUtil,
                          final TransactionExecutorFactory executorFactory,
                          @Named("dsFramework") final  DatasetFramework dsFramework) {

    super(locationFactory);
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.queueAdmin = queueAdmin;
    this.hBaseTableUtil = hBaseTableUtil;
    this.dsFramework = dsFramework;


    this.datasetInstanceMds = Transactional.of(executorFactory,
                                                  new Supplier<UpgradeMdsStores<DatasetInstanceMDS>>() {
      @Override
      public UpgradeMdsStores<DatasetInstanceMDS> get() {
        String dsName = Joiner.on(".").join(Constants.SYSTEM_NAMESPACE, DatasetMetaTableUtil.INSTANCE_TABLE_NAME);
        Id.DatasetInstance datasetId = Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE_ID, dsName);
        try {
          DatasetInstanceMDS oldMds =
            DatasetsUtil.getOrCreateDataset(dsFramework, datasetId, DatasetInstanceMDS.class.getName(),
                                            DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
          DatasetInstanceMDS newMds = new DatasetMetaTableUtil(dsFramework).getInstanceMetaTable();
          return new UpgradeMdsStores<DatasetInstanceMDS>(oldMds, newMds);
        } catch (Exception e) {
          LOG.error("Failed to access table: {}", datasetId, e);
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @Override
  public void upgrade() throws Exception {
    // Upgrade system dataset
    upgradeSystemDatasets();

    // Upgrade all user hbase tables
    upgradeUserTables();

    // Upgrade all queue and stream tables.
    queueAdmin.upgrade();

    // Upgrade the datasets instace meta table
    upgradeDatasetInstanceMDS();
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

    for (HTableDescriptor desc : hAdmin.listTables(USER_TABLE_PREFIX)) {
      String tableName = desc.getNameAsString();
      HTableNameConverter hTableNameConverter = new HTableNameConverterFactory().get();
      TableId tableId = hTableNameConverter.from(tableName);
      LOG.info("Upgrading hbase table: {}, desc: {}", tableName, desc);

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
      LOG.info("Upgraded hbase table: {}", tableName);
    }
  }


  // Moves dataset instance meta entries into new table (in system namespace)
  // Also updates the spec's name ('cdap.user.foo' -> 'foo')
  private void upgradeDatasetInstanceMDS() throws IOException, DatasetManagementException, InterruptedException,
    TransactionFailureException {
    LOG.info("Upgrading dataset instance mds.");
    datasetInstanceMds.execute(new TransactionExecutor.Function<UpgradeMdsStores<DatasetInstanceMDS>, Void>() {
      @Override
      public Void apply(UpgradeMdsStores<DatasetInstanceMDS> ctx) throws Exception {
        MDSKey key = new MDSKey(Bytes.toBytes(DatasetInstanceMDS.INSTANCE_PREFIX));
        DatasetInstanceMDS newMds = ctx.getNewMds();
        List<DatasetSpecification> dsSpecs = ctx.getOldMds().list(key, DatasetSpecification.class);
        for (DatasetSpecification dsSpec: dsSpecs) {
          LOG.info("Migrating dataset Spec: {}", dsSpec);
          Id.Namespace namespace = namespaceFromDatasetName(dsSpec.getName());
          DatasetSpecification migratedDsSpec = migrateDatasetSpec(dsSpec);
          LOG.info("Writing new dataset Spec: {}", migratedDsSpec);
          newMds.write(namespace, migratedDsSpec);
        }
        return null;
      }
    });
  }

  /**
   * Construct a {@link Id.DatasetInstance} from a pre-2.8.0 CDAP Dataset name
   *
   * @param datasetName the dataset/table name to construct the {@link Id.DatasetInstance} from
   * @return the {@link Id.DatasetInstance} object for the specified dataset/table name
   */
  private static Id.DatasetInstance from(String datasetName) {
    Preconditions.checkArgument(datasetName != null, "Dataset name should not be null");
    // Dataset/Table name is expected to be in the format <table-prefix>.<namespace>.<name>
    String invalidFormatError = String.format("Invalid format for dataset '%s'. " +
                                                "Expected - <table-prefix>.<namespace>.<dataset-name>", datasetName);
    String [] parts = datasetName.split("\\.", 3);
    Preconditions.checkArgument(parts.length == 3, invalidFormatError);
    // Ignore the prefix in the input name.
    return Id.DatasetInstance.from(parts[1], parts[2]);
  }

  private DatasetSpecification migrateDatasetSpec(DatasetSpecification oldSpec) {
    Id.DatasetInstance dsId = from(oldSpec.getName());
    String newDatasetName = dsId.getId();
    return DatasetSpecification.changeName(oldSpec, newDatasetName);
  }

  private Id.Namespace namespaceFromDatasetName(String dsName) {
    // input of the form: 'cdap.user.foo', or 'cdap.system.app.meta'
    Id.DatasetInstance dsId = from(dsName);
    String namespace = dsId.getNamespaceId();
    if (Constants.SYSTEM_NAMESPACE.equals(namespace)) {
      return Constants.SYSTEM_NAMESPACE_ID;
    } else if ("user".equals(namespace)) {
      return Constants.DEFAULT_NAMESPACE_ID;
    } else {
      throw new IllegalArgumentException(String.format("Expected dataset namespace to be either 'system' or 'user': %s",
                                                       dsId));
    }
  }

  private static final class UpgradeMdsStores<T> implements Iterable<T> {
    private final List<T> stores;

    private UpgradeMdsStores(T oldMds, T newMds) {
      this.stores = ImmutableList.of(oldMds, newMds);
    }

    private T getOldMds() {
      return stores.get(0);
    }

    private T getNewMds() {
      return stores.get(1);
    }

    @Override
    public Iterator<T> iterator() {
      return stores.iterator();
    }
  }
}
