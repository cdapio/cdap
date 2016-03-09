/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
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
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.Location;
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

  // Property key in the coprocessor for storing version of the coprocessor.
  private static final String CDAP_VERSION = "cdap.version";

  // Function to convert Class into class Name
  private static final Function<Class<?>, String> CLASS_TO_NAME = new Function<Class<?>, String>() {
    @Override
    public String apply(Class<?> input) {
      return input.getName();
    }
  };

  protected final TableId tableId;
  protected final Configuration hConf;
  protected final HBaseTableUtil tableUtil;

  private HBaseAdmin admin;

  protected AbstractHBaseDataSetAdmin(TableId tableId, Configuration hConf, HBaseTableUtil tableUtil) {
    this.tableId = tableId;
    this.hConf = hConf;
    this.tableUtil = tableUtil;
  }

  @Override
  public void upgrade() throws IOException {
    upgradeTable(Boolean.valueOf(System.getProperty(SYSTEM_PROPERTY_FORCE_HBASE_UPGRADE)));
  }

  @Override
  public boolean exists() throws IOException {
    return tableUtil.tableExists(getAdmin(), tableId);
  }

  @Override
  public void truncate() throws IOException {
    tableUtil.truncateTable(getAdmin(), tableId);
  }

  @Override
  public void drop() throws IOException {
    tableUtil.dropTable(getAdmin(), tableId);
  }

  @Override
  public void close() throws IOException {
    if (admin != null) {
      admin.close();
    }
  }

  /**
   * Performs upgrade on a given HBase table. It will be upgraded if either its spec has
   * changed since the HBase table was created or upgraded, or if the CDAP version recorded
   * in the HTable descriptor
   *
   * @param force forces upgrade regardless of whether the table needs upgrade.
   * @throws IOException If upgrade failed.
   */
  public void upgradeTable(boolean force) throws IOException {

    HTableDescriptor tableDescriptor = tableUtil.getHTableDescriptor(getAdmin(), tableId);

    // Upgrade any table properties if necessary
    boolean needUpgrade = upgradeTable(tableDescriptor) || force;

    // Get the cdap version from the table
    ProjectInfo.Version version = getVersion(tableDescriptor);

    if (!needUpgrade && version.compareTo(ProjectInfo.getVersion()) >= 0) {
      // If neither the table spec nor the cdap version have changed, no need to upgrade
      LOG.info("Table '{}' has not changed and its version '{}' is same or greater " +
                 "than current CDAP version '{}'", tableId, version, ProjectInfo.getVersion());
      return;
    }

    // create a new descriptor for the table upgrade
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
        // The same coprocessor has been configured, check by the file name hash to see if they are the same.
        if (!jarLocation.getName().equals(info.getPath().getName())) {
          // Remove old one and add the new one.
          newDescriptor.removeCoprocessor(info.getClassName());
          addCoprocessor(newDescriptor, coprocessor, jarLocation, coprocessorJar.getPriority(coprocessor));
        }
      } else {
        // The coprocessor is missing from the table, add it.
        addCoprocessor(newDescriptor, coprocessor, jarLocation, coprocessorJar.getPriority(coprocessor));
      }
    }

    // Removes all old coprocessors
    Set<String> coprocessorNames = ImmutableSet.copyOf(Iterables.transform(coprocessorJar.coprocessors, CLASS_TO_NAME));
    for (String remove : Sets.difference(coprocessorInfo.keySet(), coprocessorNames)) {
      newDescriptor.removeCoprocessor(remove);
    }

    setVersion(newDescriptor);

    LOG.info("Upgrading table '{}'...", tableId);
    boolean enableTable = false;
    try {
      tableUtil.disableTable(getAdmin(), tableId);
      enableTable = true;
    } catch (TableNotEnabledException e) {
      LOG.debug("Table '{}' was not enabled before upgrade and will not be enabled after upgrade.", tableId);
    }

    tableUtil.modifyTable(getAdmin(), newDescriptor.build());
    if (enableTable) {
      tableUtil.enableTable(getAdmin(), tableId);
    }

    LOG.info("Table '{}' upgrade completed.", tableId);
  }

  public static void setVersion(HTableDescriptorBuilder tableDescriptor) {
    tableDescriptor.setValue(CDAP_VERSION, ProjectInfo.getVersion().toString());
  }

  public static ProjectInfo.Version getVersion(HTableDescriptor tableDescriptor) {
    String value = tableDescriptor.getValue(CDAP_VERSION);
    return new ProjectInfo.Version(value);
  }

  protected void addCoprocessor(HTableDescriptorBuilder tableDescriptor, Class<? extends Coprocessor> coprocessor,
                                Location jarFile, Integer priority) throws IOException {
    if (priority == null) {
      priority = Coprocessor.PRIORITY_USER;
    }
    tableDescriptor.addCoprocessor(coprocessor.getName(), new Path(Locations.toURI(jarFile).getPath()), priority, null);
  }

  protected abstract CoprocessorJar createCoprocessorJar() throws IOException;

  /**
   * Modifies the table descriptor for upgrade.
   *
   * @return true if the table descriptor is modified.
   */
  protected abstract boolean upgradeTable(HTableDescriptor tableDescriptor);

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

  protected HBaseAdmin getAdmin() throws IOException {
    if (admin == null) {
      admin = new HBaseAdmin(hConf);
    }
    return admin;
  }
}
