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

package co.cask.cdap.data2.datafabric.dataset.service.mds;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Upgrades Dataset instances MDS
 */
public final class DatasetInstanceMDSUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceMDSUpgrader.class);
  private final Transactional<UpgradeMdsStores<DatasetInstanceMDS>, DatasetInstanceMDS> datasetInstanceMds;

  @Inject
  private DatasetInstanceMDSUpgrader(final TransactionExecutorFactory executorFactory,
                                     @Named("dsFramework") final DatasetFramework dsFramework) {

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

  public void upgrade() throws Exception {
    // Moves dataset instance meta entries into new table (in system namespace)
    // Also updates the spec's name ('cdap.user.foo' -> 'foo')
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
