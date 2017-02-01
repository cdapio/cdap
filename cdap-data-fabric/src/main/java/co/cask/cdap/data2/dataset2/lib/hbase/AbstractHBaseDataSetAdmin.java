/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.data2.dataset2.lib.hbase;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.CoprocessorManager;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.hbase.CoprocessorDescriptor;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for writing HBase EntityAdmin.
 */
public abstract class AbstractHBaseDataSetAdmin implements DatasetAdmin {

  /**
   * This will only be set by the upgrade tool to force upgrade of all HBase coprocessors after an HBase upgrade.
   */
  public static final String SYSTEM_PROPERTY_FORCE_HBASE_UPGRADE = "cdap.force.hbase.upgrade";

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseDataSetAdmin.class);

  // Function to convert Class into class Name
  private static final Function<Class<?>, String> CLASS_TO_NAME = new Function<Class<?>, String>() {
    @Override
    public String apply(Class<?> input) {
      return input.getName();
    }
  };

  protected final TableId tableId;
  protected final Configuration hConf;
  protected final CConfiguration cConf;
  protected final HBaseTableUtil tableUtil;
  protected final HBaseDDLExecutorFactory ddlExecutorFactory;
  protected final String tablePrefix;
  protected final CoprocessorManager coprocessorManager;

  protected AbstractHBaseDataSetAdmin(TableId tableId, Configuration hConf, CConfiguration cConf,
                                      HBaseTableUtil tableUtil, LocationFactory locationFactory) {
    this.tableId = tableId;
    this.hConf = hConf;
    this.cConf = cConf;
    this.tableUtil = tableUtil;
    this.tablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    this.ddlExecutorFactory = new HBaseDDLExecutorFactory(cConf, hConf);
    this.coprocessorManager = new CoprocessorManager(cConf, locationFactory, tableUtil);
  }

  @Override
  public void upgrade() throws IOException {
    updateTable(Boolean.valueOf(System.getProperty(SYSTEM_PROPERTY_FORCE_HBASE_UPGRADE)));
  }

  @Override
  public boolean exists() throws IOException {
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      return tableUtil.tableExists(admin, tableId);
    }
  }

  @Override
  public void truncate() throws IOException {
    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
      tableUtil.truncateTable(ddlExecutor, tableId);
    }
  }

  @Override
  public void drop() throws IOException {
    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
      tableUtil.dropTable(ddlExecutor, tableId);
    }
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  /**
   * Performs update on a given HBase table. It will be updated if either its spec has
   * changed since the HBase table was created or updated, or if the CDAP version recorded
   * in the HTable descriptor is less than the current CDAP version.
   *
   * @param force forces update regardless of whether the table needs it.
   * @throws IOException If update failed.
   */
  public void updateTable(boolean force) throws IOException {

    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
      HTableDescriptor tableDescriptor;

      try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
        tableDescriptor = tableUtil.getHTableDescriptor(admin, tableId);
      }

      // update any table properties if necessary
      boolean needUpdate = needsUpdate(tableDescriptor) || force;

      // Get the cdap version from the table
      ProjectInfo.Version version = HBaseTableUtil.getVersion(tableDescriptor);

      if (!needUpdate && version.compareTo(ProjectInfo.getVersion()) >= 0) {
        // If neither the table spec nor the cdap version have changed, no need to update
        LOG.info("Table '{}' has not changed and its version '{}' is same or greater " +
                   "than current CDAP version '{}'", tableId, version, ProjectInfo.getVersion());
        return;
      }

      // create a new descriptor for the table update
      HTableDescriptorBuilder newDescriptor = tableUtil.buildHTableDescriptor(tableDescriptor);

      // Generate the coprocessor jar
      CoprocessorJar coprocessorJar = createCoprocessorJar();
      Location jarLocation = coprocessorJar.getJarLocation();

      // Check if coprocessor upgrade is needed
      Map<String, HBaseTableUtil.CoprocessorInfo> coprocessorInfo = HBaseTableUtil.getCoprocessorInfo(tableDescriptor);
      // For all required coprocessors, check if they've need to be upgraded.
      for (Class<? extends Coprocessor> coprocessor : coprocessorJar.getCoprocessors()) {
        HBaseTableUtil.CoprocessorInfo info = coprocessorInfo.get(coprocessor.getName());
        if (info != null) {
          // The same coprocessor has been configured, check by the file name to see if they are the same.
          if (!jarLocation.getName().equals(info.getPath().getName())) {
            // Remove old one and add the new one.
            newDescriptor.removeCoprocessor(info.getClassName());
            addCoprocessor(newDescriptor, coprocessor, coprocessorJar.getPriority(coprocessor));
          }
        } else {
          // The coprocessor is missing from the table, add it.
          addCoprocessor(newDescriptor, coprocessor, coprocessorJar.getPriority(coprocessor));
        }
      }

      // Removes all old coprocessors
      Set<String> coprocessorNames = ImmutableSet.copyOf(Iterables.transform(coprocessorJar.coprocessors,
                                                                             CLASS_TO_NAME));
      for (String remove : Sets.difference(coprocessorInfo.keySet(), coprocessorNames)) {
        newDescriptor.removeCoprocessor(remove);
      }

      HBaseTableUtil.setVersion(newDescriptor);
      HBaseTableUtil.setTablePrefix(newDescriptor, cConf);

      LOG.info("Updating table '{}'...", tableId);
      TableName tableName = HTableNameConverter.toTableName(cConf.get(Constants.Dataset.TABLE_PREFIX), tableId);
      boolean enableTable = false;
      try {
        ddlExecutor.disableTableIfEnabled(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
        enableTable = true;
      } catch (TableNotEnabledException e) {
        // TODO (CDAP-7324) This is a workaround and should be removed once we have pure hbase coprocessor upgrade
        // (CDAP-7095)
        // If the table is in cdap_system namespace enable it regardless so that they can be used later. See CDAP-7324
        if (isSystemTable()) {
          enableTable = true;
        } else {
          LOG.debug("Table '{}' was not enabled before update and will not be enabled after update.", tableId);
        }
      }

      tableUtil.modifyTable(ddlExecutor, newDescriptor.build());
      if (enableTable) {
        LOG.debug("Enabling table '{}'...", tableId);
        ddlExecutor.enableTableIfDisabled(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
      }
    }

    LOG.info("Table '{}' update completed.", tableId);
  }

  private boolean isSystemTable() {
    return tableId.getNamespace().equalsIgnoreCase(String.format("%s_%s", cConf.get(Constants.Dataset.TABLE_PREFIX),
                                                                 NamespaceId.SYSTEM.getNamespace()));
  }

  protected void addCoprocessor(HTableDescriptorBuilder tableDescriptor, Class<? extends Coprocessor> coprocessor,
                                Integer priority) throws IOException {
    CoprocessorDescriptor descriptor = coprocessorManager.getCoprocessorDescriptor(coprocessor, priority);
    Path path = descriptor.getPath() == null ? null : new Path(descriptor.getPath());
    tableDescriptor.addCoprocessor(descriptor.getClassName(), path, descriptor.getPriority(),
                                   descriptor.getProperties());
  }

  protected abstract CoprocessorJar createCoprocessorJar() throws IOException;

  /**
   * Modifies the table descriptor for update, if an update is needed due to a dataset properties change,
   * that is, if the current table descriptor does not reflect the current dataset specification.
   *
   * @return true if the table descriptor is modified, that is, whether update is needed.
   */
  protected abstract boolean needsUpdate(HTableDescriptor tableDescriptor);

  /**
   * Holder for coprocessor information.
   */
  // todo: make protected, after CDAP-1193 is fixed
  public static final class CoprocessorJar {
    public static final CoprocessorJar EMPTY = new CoprocessorJar(ImmutableList.<Class<? extends Coprocessor>>of(),
                                                                  null);

    private final List<Class<? extends Coprocessor>> coprocessors;
    private final Location jarLocation;
    private final Map<Class<? extends Coprocessor>, Integer> priorities = Maps.newHashMap();

    public CoprocessorJar(Iterable<? extends Class<? extends Coprocessor>> coprocessors, Location jarLocation) {
      this.coprocessors = ImmutableList.copyOf(coprocessors);
      // set coprocessor loading order to match iteration order
      int priority = Coprocessor.PRIORITY_USER;
      for (Class<? extends Coprocessor> cpClass : coprocessors) {
        priorities.put(cpClass, priority++);
      }
      this.jarLocation = jarLocation;
    }

    public void setPriority(Class<? extends Coprocessor> cpClass, int priority) {
      priorities.put(cpClass, priority);
    }

    public Integer getPriority(Class<? extends Coprocessor> cpClass) {
      return priorities.get(cpClass);
    }

    public Iterable<? extends Class<? extends Coprocessor>> getCoprocessors() {
      return coprocessors;
    }

    public Location getJarLocation() {
      return jarLocation;
    }

    public boolean isEmpty() {
      return coprocessors.isEmpty();
    }

    public int size() {
      return coprocessors.size();
    }
  }
}
