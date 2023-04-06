/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.util.hbase;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.spi.hbase.CoprocessorDescriptor;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages HBase coprocessors for Tables and Queues.
 */
public class CoprocessorManager {

  private static final Logger LOG = LoggerFactory.getLogger(CoprocessorManager.class);
  private static final String INCLUDE_BUILD_IN_PATH = "master.coprocessors.include.build.in.path";
  private final boolean manageCoprocessors;
  private final boolean includeBuildInPath;
  private final Location jarDir;
  private final Set<Class<? extends Coprocessor>> coprocessors;

  public CoprocessorManager(CConfiguration cConf, LocationFactory locationFactory,
      HBaseTableUtil tableUtil) {
    this.manageCoprocessors = cConf.getBoolean(Constants.HBase.MANAGE_COPROCESSORS);
    // this is really only useful in a development setting, so not putting the default in cdap-default.xml
    this.includeBuildInPath = cConf.getBoolean(INCLUDE_BUILD_IN_PATH, true);
    this.jarDir = locationFactory.create(cConf.get(Constants.CFG_HDFS_LIB_DIR));
    //noinspection unchecked
    this.coprocessors = ImmutableSet.of(
        tableUtil.getTransactionDataJanitorClassForVersion(),
        tableUtil.getIncrementHandlerClassForVersion(),
        tableUtil.getMessageTableRegionObserverClassForVersion(),
        tableUtil.getPayloadTableRegionObserverClassForVersion());
  }


  /**
   * Get the descriptor for a single coprocessor that uses the pre-built coprocessor jar.
   */
  public CoprocessorDescriptor getCoprocessorDescriptor(Class<? extends Coprocessor> coprocessor,
      @Nullable Integer priority) throws IOException {
    if (priority == null) {
      priority = Coprocessor.PRIORITY_USER;
    }

    Location jarFile = ensureCoprocessorExists();
    String jarPath = manageCoprocessors ? jarFile.toURI().getPath() : null;
    return new CoprocessorDescriptor(coprocessor.getName(), jarPath, priority, null);
  }

  /**
   * Get the location of the coprocessor and ensure it exists. In distributed mode, the coprocessor
   * jar is loaded onto hdfs by the CoprocessorBuildTool, but in other modes it is still useful to
   * create the jar on demand.
   *
   * @return the location of the coprocessor
   * @throws IOException if there was an issue accessing the location
   */
  public synchronized Location ensureCoprocessorExists() throws IOException {
    return ensureCoprocessorExists(false);
  }

  /**
   * Get the location of the coprocessor and ensure it exists, optionally overwriting it if it
   * exists. In distributed mode, the coprocessor jar is loaded onto hdfs by the
   * CoprocessorBuildTool, but in other modes it is still useful to create the jar on demand.
   *
   * @param overwrite whether to overwrite the coprocessor if it already exists
   * @return the location of the coprocessor
   * @throws IOException if there was an issue accessing the location
   */
  public synchronized Location ensureCoprocessorExists(boolean overwrite) throws IOException {

    Location targetPath = jarDir.append(getCoprocessorName());
    if (!overwrite && targetPath.exists()) {
      return targetPath;
    }

    // ensure the jar directory exists
    Locations.mkdirsIfNotExists(jarDir);

    StringBuilder buf = new StringBuilder();
    for (Class<? extends Coprocessor> c : coprocessors) {
      buf.append(c.getName()).append(", ");
    }

    LOG.debug("Creating jar file for coprocessor classes: {}", buf);

    Location tmpLocation = jarDir.getTempFile(".jar");
    ApplicationBundler appBundler = new ApplicationBundler(new ClassAcceptor() {
      @Override
      public boolean accept(String className, final URL classUrl, URL classPathUrl) {
        // Assuming the endpoint and protocol class doesn't have dependencies
        // other than those comes with HBase, Java, fastutil, and gson
        // if (!dependentClasses.containsKey(className)) {
        //   dependentClasses.put(className, classPathUrl);
        // }
        return className.startsWith("io.cdap") || className.startsWith("it.unimi.dsi.fastutil")
            || className.startsWith("org.apache.tephra") || className.startsWith(
            "com.google.gson");
      }
    });
    appBundler.setClassesDir("");
    appBundler.setLibDir("");
    appBundler.createBundle(tmpLocation, new ArrayList<>(coprocessors));

    tmpLocation.renameTo(targetPath);
    return targetPath;
  }

  private String getCoprocessorName() {
    ProjectInfo.Version cdapVersion = ProjectInfo.getVersion();
    StringBuilder name = new StringBuilder()
        .append("coprocessor-")
        .append(cdapVersion.getMajor()).append('.')
        .append(cdapVersion.getMinor()).append('.')
        .append(cdapVersion.getFix());
    if (cdapVersion.isSnapshot()) {
      name.append("-SNAPSHOT");
    }
    if (includeBuildInPath) {
      name.append("-").append(cdapVersion.getBuildTime());
    }

    name.append("-").append(HBaseVersion.get()).append(".jar");
    return name.toString();
  }
}
