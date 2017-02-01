/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.spi.hbase.CoprocessorDescriptor;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.annotation.Nullable;

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

  public CoprocessorManager(CConfiguration cConf, LocationFactory locationFactory, HBaseTableUtil tableUtil) {
    this.manageCoprocessors = cConf.getBoolean(Constants.HBase.MANAGE_COPROCESSORS);
    // this is really only useful in a development setting, so not putting the default in cdap-default.xml
    this.includeBuildInPath = cConf.getBoolean(INCLUDE_BUILD_IN_PATH, true);
    this.jarDir = locationFactory.create(cConf.get(Constants.CFG_HDFS_LIB_DIR));
    //noinspection unchecked
    this.coprocessors = ImmutableSet.of(
      tableUtil.getTransactionDataJanitorClassForVersion(),
      tableUtil.getIncrementHandlerClassForVersion(),
      tableUtil.getQueueRegionObserverClassForVersion(),
      tableUtil.getDequeueScanObserverClassForVersion(),
      tableUtil.getMessageTableRegionObserverClassForVersion(),
      tableUtil.getPayloadTableRegionObserverClassForVersion());
  }


  /**
   * Get the descriptor for a single coprocessor that uses the pre-built coprocessor jar.
   */
  public CoprocessorDescriptor getCoprocessorDescriptor(Class<? extends  Coprocessor> coprocessor,
                                                        @Nullable Integer priority) throws IOException {
    if (priority == null) {
      priority = Coprocessor.PRIORITY_USER;
    }

    Location jarFile = ensureCoprocessorExists();
    String jarPath = manageCoprocessors ? jarFile.toURI().getPath() : null;
    return new CoprocessorDescriptor(coprocessor.getName(), jarPath, priority, null);
  }

  /**
   * Get the location of the coprocessor and ensure it exists.
   * In distributed mode, the coprocessor jar is loaded onto hdfs by the CoprocessorBuildTool,
   * but in other modes it is still useful to create the jar on demand.
   *
   * @return the location of the coprocessor
   * @throws IOException if there was an issue accessing the location
   */
  public synchronized Location ensureCoprocessorExists() throws IOException {
    return ensureCoprocessorExists(false);
  }

  /**
   * Get the location of the coprocessor and ensure it exists, optionally overwriting it if it exists.
   * In distributed mode, the coprocessor jar is loaded onto hdfs by the CoprocessorBuildTool,
   * but in other modes it is still useful to create the jar on demand.
   *
   * @param overwrite whether to overwrite the coprocessor if it already exists
   * @return the location of the coprocessor
   * @throws IOException if there was an issue accessing the location
   */
  public synchronized Location ensureCoprocessorExists(boolean overwrite) throws IOException {

    final Location targetPath = jarDir.append(getCoprocessorName());
    if (!overwrite && targetPath.exists()) {
      return targetPath;
    }

    // ensure the jar directory exists
    Locations.mkdirsIfNotExists(jarDir);

    StringBuilder buf = new StringBuilder();
    for (Class<? extends Coprocessor> c : coprocessors) {
      buf.append(c.getName()).append(", ");
    }

    LOG.debug("Creating jar file for coprocessor classes: {}", buf.toString());

    final Map<String, URL> dependentClasses = new HashMap<>();
    for (Class<? extends Coprocessor> clz : coprocessors) {
      Dependencies.findClassDependencies(clz.getClassLoader(), new ClassAcceptor() {
        @Override
        public boolean accept(String className, final URL classUrl, URL classPathUrl) {
          // Assuming the endpoint and protocol class doesn't have dependencies
          // other than those comes with HBase, Java, fastutil, and gson
          if (className.startsWith("co.cask") || className.startsWith("it.unimi.dsi.fastutil")
            || className.startsWith("org.apache.tephra") || className.startsWith("com.google.gson")) {
            if (!dependentClasses.containsKey(className)) {
              dependentClasses.put(className, classUrl);
            }
            return true;
          }
          return false;
        }
      }, clz.getName());
    }

    if (dependentClasses.isEmpty()) {
      return null;
    }

    // create the coprocessor jar on local filesystem
    LOG.debug("Adding " + dependentClasses.size() + " classes to jar");
    File jarFile = File.createTempFile("coprocessor", ".jar");
    byte[] buffer = new byte[4 * 1024];
    try (JarOutputStream jarOutput = new JarOutputStream(new FileOutputStream(jarFile))) {
      for (Map.Entry<String, URL> entry : dependentClasses.entrySet()) {
        jarOutput.putNextEntry(new JarEntry(entry.getKey().replace('.', File.separatorChar) + ".class"));

        try (InputStream inputStream = entry.getValue().openStream()) {
          int len = inputStream.read(buffer);
          while (len >= 0) {
            jarOutput.write(buffer, 0, len);
            len = inputStream.read(buffer);
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Unable to create temporary local coprocessor jar {}.", jarFile.getAbsolutePath(), e);
      if (!jarFile.delete()) {
        LOG.warn("Unable to clean up temporary local coprocessor jar {}.", jarFile.getAbsolutePath());
      }
      throw e;
    }

    // copy the local jar file to the filesystem (HDFS)
    // copies to a tmp location then renames the tmp location to the target location in case
    // multiple CoprocessorManagers we called at the same time. This should never be the case in distributed
    // mode, as coprocessors should all be loaded beforehand using the CoprocessorBuildTool.
    final Location tmpLocation = jarDir.getTempFile(".jar");
    try {
      // Copy jar file into filesystem (HDFS)
      Files.copy(jarFile, new OutputSupplier<OutputStream>() {
        @Override
        public OutputStream getOutput() throws IOException {
          return tmpLocation.getOutputStream();
        }
      });
    } catch (IOException e) {
      LOG.error("Unable to copy local coprocessor jar to filesystem at {}.", tmpLocation, e);
      if (tmpLocation.exists()) {
        LOG.info("Deleting partially copied coprocessor jar at {}.", tmpLocation);
        try {
          if (!tmpLocation.delete()) {
            LOG.error("Unable to delete partially copied coprocessor jar at {}.", tmpLocation, e);
          }
        } catch (IOException e1) {
          LOG.error("Unable to delete partially copied coprocessor jar at {}.", tmpLocation, e1);
          e.addSuppressed(e1);
        }
      }
      throw e;
    } finally {
      if (!jarFile.delete()) {
        LOG.warn("Unable to clean up temporary local coprocessor jar {}.", jarFile.getAbsolutePath());
      }
    }

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
