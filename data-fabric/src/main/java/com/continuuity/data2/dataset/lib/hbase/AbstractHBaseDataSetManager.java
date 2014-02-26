/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.dataset.lib.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ProjectInfo;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for writing HBase DataSetManager.
 */
public abstract class AbstractHBaseDataSetManager implements DataSetManager {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseDataSetManager.class);

  // Property key in the coprocessor for storing version of the coprocessor.
  private static final String CONTINUUITY_VERSION = "continuuity.version";

  // Function to convert Class into class Name
  private static final Function<Class<?>, String> CLASS_TO_NAME = new Function<Class<?>, String>() {
    @Override
    public String apply(Class<?> input) {
      return input.getName();
    }
  };

  protected final HBaseAdmin admin;
  protected final HBaseTableUtil tableUtil;

  protected AbstractHBaseDataSetManager(HBaseAdmin admin, HBaseTableUtil tableUtil) {
    this.admin = admin;
    this.tableUtil = tableUtil;
  }

  @Override
  public void upgrade(String name) throws Exception {
    upgradeTable(getHBaseTableName(name));
  }

  /**
   * Performs upgrade on a given HBase table.
   *
   * @param tableNameStr The HBase table name that upgrade will be performed on.
   * @throws Exception If upgrade failed.
   */
  protected void upgradeTable(String tableNameStr) throws Exception {
    byte[] tableName = Bytes.toBytes(tableNameStr);

    HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);

    // Generate the coprocessor jar
    CoprocessorJar coprocessorJar = createCoprocessorJar();
    Location jarLocation = coprocessorJar.getJarLocation();

    // Check if upgrade is needed
    boolean needUpgrade = false;
    Map<String, HBaseTableUtil.CoprocessorInfo> coprocessorInfo = HBaseTableUtil.getCoprocessorInfo(tableDescriptor);

    // For all required coprocessors, check if they've need to be upgraded.
    for (Class<? extends Coprocessor> coprocessor : coprocessorJar.getCoprocessors()) {
      HBaseTableUtil.CoprocessorInfo info = coprocessorInfo.get(coprocessor.getName());
      if (info != null) {
        // The same coprocessor has been configured for the table, check the version to see if it is upgrade.
        ProjectInfo.Version version = new ProjectInfo.Version(info.getProperties().get(CONTINUUITY_VERSION));
        if (version.compareTo(ProjectInfo.getVersion()) < 0) {
          needUpgrade = true;
          // Remove old one and add the new one.
          tableDescriptor.removeCoprocessor(info.getClassName());
          addCoprocessor(tableDescriptor, coprocessor, jarLocation, null);
        }
      } else {
        // The coprocessor is missing from the table, add it.
        needUpgrade = true;
        addCoprocessor(tableDescriptor, coprocessor, jarLocation, null);
      }
    }

    // Removes all old coprocessors
    Set<String> coprocessorNames = ImmutableSet.copyOf(Iterables.transform(coprocessorJar.coprocessors, CLASS_TO_NAME));
    for (String remove : Sets.difference(coprocessorInfo.keySet(), coprocessorNames)) {
      needUpgrade = true;
      tableDescriptor.removeCoprocessor(remove);
    }

    if (!needUpgrade) {
      LOG.info("No upgrade needed for table '{}'", tableNameStr);
      return;
    }

    try {
      admin.disableTable(tableName);
    } catch (TableNotEnabledException e) {
      LOG.debug("Table '{}' not enabled when try to disable it.", tableNameStr);
    }

    admin.modifyTable(tableName, tableDescriptor);
    admin.enableTable(tableName);

    LOG.info("Upgrade for table '{}' completed.", tableNameStr);
  }

  protected void addCoprocessor(HTableDescriptor tableDescriptor, Class<? extends Coprocessor> coprocessor,
                                Location jarFile, Map<String, String> kvs) throws IOException {
    Map<String, String> properties = Maps.newHashMap();
    if (kvs != null) {
      properties.putAll(kvs);
    }
    properties.put(CONTINUUITY_VERSION, ProjectInfo.getVersion().toString());

    tableDescriptor.addCoprocessor(coprocessor.getName(), new Path(jarFile.toURI()),
                                   Coprocessor.PRIORITY_USER, properties);
  }

  protected abstract String getHBaseTableName(String name);

  protected abstract CoprocessorJar createCoprocessorJar() throws IOException;

  /**
   * Holder for coprocessor information.
   */
  protected static final class CoprocessorJar {
    public static final CoprocessorJar EMPTY = new CoprocessorJar(ImmutableList.<Class<? extends Coprocessor>>of(),
                                                                  null);

    private final List<Class<? extends Coprocessor>> coprocessors;
    private final Location jarLocation;

    public CoprocessorJar(Iterable<? extends Class<? extends Coprocessor>> coprocessors, Location jarLocation) {
      this.coprocessors = ImmutableList.copyOf(coprocessors);
      this.jarLocation = jarLocation;
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
