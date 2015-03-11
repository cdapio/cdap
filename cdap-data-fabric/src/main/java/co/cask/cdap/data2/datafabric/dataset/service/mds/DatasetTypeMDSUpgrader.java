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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Upgrades {@link DatasetTypeMDS}
 */
public final class DatasetTypeMDSUpgrader extends AbstractMDSUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeMDSUpgrader.class);
  // lists of datasets type modules which existed earlier but does not anymore existed in 2.6 and removed in 2.8
  private static final Set<String> REMOVED_DATASET_MODULES = Sets.newHashSet(
    "co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseOrderedTableModule",
    "co.cask.cdap.data2.dataset2.lib.table.ACLTableModule");
  private final TransactionExecutorFactory executorFactory;
  private final DatasetFramework dsFramework;
  private Transactional<UpgradeMDSStores<DatasetTypeMDS>, DatasetTypeMDS> datasetTypeMDS;
  private final LocationFactory locationFactory;
  private final Configuration hConf;
  private final HBaseTableUtil tableUtil;
  private Id.DatasetInstance oldDatasetId;


  @Inject
  private DatasetTypeMDSUpgrader(final TransactionExecutorFactory executorFactory,
                                 @Named("dsFramework") final DatasetFramework dsFramework,
                                 LocationFactory locationFactory, Configuration hConf,
                                 HBaseTableUtil tableUtil) {
    this.executorFactory = executorFactory;
    this.dsFramework = dsFramework;
    this.locationFactory = locationFactory;
    this.hConf = hConf;
    this.tableUtil = tableUtil;
  }

  private void setupDatasetTypeMDS(final DatasetTypeMDS oldMds) {
    datasetTypeMDS = Transactional.of(executorFactory, new Supplier<UpgradeMDSStores<DatasetTypeMDS>>() {
      @Override
      public UpgradeMDSStores<DatasetTypeMDS> get() {
        DatasetTypeMDS newMds;
        try {
          newMds = new DatasetMetaTableUtil(dsFramework).getTypeMetaTable();
        } catch (Exception e) {
          LOG.error("Failed to access Datasets type meta table.");
          throw Throwables.propagate(e);
        }
        return new UpgradeMDSStores<DatasetTypeMDS>(oldMds, newMds);
      }
    });
  }

  /**
   * Gets the old {@link DatasetTypeMDS} table
   *
   * @return {@link DatasetTypeMDS} the old meta table
   */
  private DatasetTypeMDS getOldDatasetTypeMDS() {
    String oldMDSName = Joiner.on(".").join(Constants.SYSTEM_NAMESPACE, DatasetMetaTableUtil.META_TABLE_NAME);
    oldDatasetId = Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE_ID, oldMDSName);
    DatasetTypeMDS oldMds;
    try {
      oldMds = dsFramework.getDataset(oldDatasetId, DatasetDefinition.NO_ARGUMENTS, null);
    } catch (Exception e) {
      LOG.error("Failed to access table of Dataset: {}", oldDatasetId, e);
      throw Throwables.propagate(e);
    }
    return oldMds;
  }

  /**
   * Upgrades the {@link DatasetTypeMDS} table for namespaces
   * Note: We don't write to new TypeMDS table through {@link DatasetTypeManager} because if the user's custom Datasets
   * has api/classes changes {@link DatasetTypeManager#addModule} will fail. So, we directly move the meta type
   * information. User's custom datasets which don't use any such changed api/classes will work out of the box but
   * the one which does will need to be re-deployed.
   *
   * @throws TransactionFailureException
   * @throws InterruptedException
   * @throws IOException
   */
  @Override
  public void upgrade() throws Exception {
    DatasetTypeMDS oldMds = getOldDatasetTypeMDS();
    if (oldMds != null) {
      setupDatasetTypeMDS(oldMds);
      final MDSKey dsModulePrefix = new MDSKey(Bytes.toBytes(DatasetTypeMDS.MODULES_PREFIX));
      try {
        datasetTypeMDS.execute(new TransactionExecutor.Function<UpgradeMDSStores<DatasetTypeMDS>, Void>() {
          @Override
          public Void apply(UpgradeMDSStores<DatasetTypeMDS> ctx) throws Exception {
            List<DatasetModuleMeta> mdsKeyDatasetModuleMetaMap = ctx.getOldMds().list(dsModulePrefix,
                                                                                      DatasetModuleMeta.class);
            for (DatasetModuleMeta datasetModuleMeta : mdsKeyDatasetModuleMetaMap) {
              if (!REMOVED_DATASET_MODULES.contains(datasetModuleMeta.getClassName())) {
                upgradeDatasetModuleMeta(datasetModuleMeta, ctx.getNewMds());
              }
            }
            return null;
          }
        });
      } catch (Exception e) {
        throw e;
      }

      // delete the old meta table
      tableUtil.dropTable(new HBaseAdmin(hConf),
                          TableId.from(oldDatasetId.getNamespaceId(), oldDatasetId.getId()));
    } else {
      LOG.info("Unable to find old meta table {}. It might have already been upgraded.", oldDatasetId.getId());
    }
  }

  /**
   * Upgrades the {@link DatasetModuleMeta} for namespace
   * The system modules are written as it is and the user module meta is written with new jarLocation which is
   * under namespace
   *
   * @param olddatasetModuleMeta the old {@link DatasetModuleMeta}
   * @param newDatasetTypeMDS the new {@link DatasetTypeMDS} where the new moduleMeta will be written
   * @throws IOException
   */
  private void upgradeDatasetModuleMeta(DatasetModuleMeta olddatasetModuleMeta, DatasetTypeMDS newDatasetTypeMDS)
    throws IOException {
    DatasetModuleMeta newDatasetModuleMeta;
    LOG.info("Upgrading dataset module {} meta", olddatasetModuleMeta.getName());
    if (olddatasetModuleMeta.getJarLocation() == null) {
      newDatasetModuleMeta = olddatasetModuleMeta;
    } else {
      Location oldJarLocation = locationFactory.create(olddatasetModuleMeta.getJarLocation());
      Location newJarLocation = updateUserDatasetModuleJarLocation(oldJarLocation, olddatasetModuleMeta.getClassName(),
                                                                   Constants.DEFAULT_NAMESPACE);

      newDatasetModuleMeta = new DatasetModuleMeta(olddatasetModuleMeta.getName(), olddatasetModuleMeta.getClassName(),
                                                   newJarLocation.toURI(), olddatasetModuleMeta.getTypes(),
                                                   olddatasetModuleMeta.getUsesModules());
      // add usedByModules to the newdatasetModuleMeta
      Collection<String> usedByModules = olddatasetModuleMeta.getUsedByModules();
      for (String moduleName : usedByModules) {
        newDatasetModuleMeta.addUsedByModule(moduleName);
      }
      newDatasetModuleMeta = olddatasetModuleMeta;
      renameLocation(oldJarLocation, newJarLocation);
    }
    newDatasetTypeMDS.writeModule(Constants.DEFAULT_NAMESPACE_ID, newDatasetModuleMeta);
  }

  /**
   * Strips different parts from the old jar location and creates a new one
   *
   * @param location the old log {@link Location}
   * @param datasetClassname the dataset class name
   * @param namespace the namespace which will be added to the new jar location
   * @return the log {@link Location}
   * @throws IOException
   */
  private Location updateUserDatasetModuleJarLocation(Location location, String datasetClassname,
                                                      String namespace) throws IOException {
    String jarFilename = location.getName();
    Location parentLocation = Locations.getParent(location);  // strip jarFilename
    parentLocation = Locations.getParent(parentLocation); // strip account_placeholder
    Preconditions.checkNotNull(parentLocation, "failed to get parent on {}", location);
    String archive = parentLocation.getName();
    parentLocation = Locations.getParent(parentLocation); // strip archive
    Preconditions.checkNotNull(parentLocation, "failed to get parent on {}", location);
    String datasets = parentLocation.getName();

    return locationFactory.create(namespace).append(datasets).append(datasetClassname).append(archive)
      .append(jarFilename);
  }

  /**
   * Renames the old location to new location if old location exists and the new one does not
   *
   * @param oldLocation the old {@link Location}
   * @param newLocation the new {@link Location}
   * @return new location if and only if the file or directory is successfully moved; null otherwise.
   * @throws IOException
   */
  @Nullable
  protected Location renameLocation(Location oldLocation, Location newLocation) throws IOException {
    // if the newLocation does not exists or the oldLocation does we try to rename. If either one of them is false then
    // the underlying call to renameTo will throw IOException which we re-throw.
    if (!newLocation.exists() && oldLocation.exists()) {
      Locations.getParent(newLocation).mkdirs();
      try {
        return oldLocation.renameTo(newLocation);
      } catch (IOException ioe) {
        newLocation.delete();
        LOG.warn("Failed to rename {} to {}", oldLocation, newLocation);
        throw ioe;
      }
    } else {
      LOG.debug("New location {} already exists and old location {} does not exists. The location might already be " +
                  "updated.", newLocation, oldLocation);
      return null;
    }
  }
}
