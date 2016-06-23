/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.master.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Checks that the configured FileSystem is available and the root path has the correct permissions.
 */
// class is picked up through classpath examination
@SuppressWarnings("unused")
public class FileSystemCheck extends AbstractMasterCheck {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemCheck.class);
  private final LocationFactory locationFactory;
  private final Configuration hConf;

  @Inject
  private FileSystemCheck(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory) {
    super(cConf);
    this.hConf = hConf;
    this.locationFactory = locationFactory;
  }

  @Override
  public void run() {
    String user = cConf.get(Constants.CFG_HDFS_USER);
    String rootPath = cConf.get(Constants.CFG_HDFS_NAMESPACE);

    LOG.info("Checking FileSystem availability.");
    Location rootLocation = locationFactory.create(rootPath);
    boolean rootExists;
    try {
      rootExists = rootLocation.exists();
      LOG.info("  FileSystem availability successfully verified.");
      if (rootExists) {
        if (!rootLocation.isDirectory()) {
          throw new RuntimeException(String.format(
            "%s is not a directory. Change it to a directory, or update %s to point to a different location.",
            rootPath, Constants.CFG_HDFS_NAMESPACE));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format(
        "Unable to connect to the FileSystem with %s set to %s. " +
          "Please check that the FileSystem is running and that the correct " +
          "Hadoop configuration (e.g. core-site.xml, hdfs-site.xml) " +
          "and Hadoop libraries are included in the CDAP Master classpath.",
        CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        hConf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY)), e);
    }

    LOG.info("Checking that user {} has permission to write to {} on the FileSystem.", user, rootPath);
    if (rootExists) {
      // try creating a tmp file to check permissions
      try {
        Location tmpFile = rootLocation.append("newTempFile").getTempFile("tmp");
        if (!tmpFile.createNew()) {
          throw new RuntimeException(String.format(
            "Could not make a temp file %s in directory %s on the FileSystem. " +
              "Please check that user %s has permission to write to %s, " +
              "or create the directory manually with write permissions.",
            tmpFile, rootPath, user, rootPath));
        } else {
          tmpFile.delete();
        }
      } catch (IOException e) {
        throw new RuntimeException(String.format(
          "Could not make/delete a temp file in directory %s on the FileSystem. " +
            "Please check that user %s has permission to write to %s, " +
            "or create the directory manually with write permissions.",
          rootPath, user, rootPath), e);
      }
    } else {
      // try creating the directory to check permissions
      try {
        if (!rootLocation.mkdirs()) {
          throw new RuntimeException(String.format(
            "Could not make directory %s on the FileSystem. " +
              "Please check that user %s has permission to write to %s, " +
              "or create the directory manually with write permissions.",
            rootPath, user, rootPath));
        }
      } catch (IOException e) {
        throw new RuntimeException(String.format(
          "Could not make directory %s on the FileSystem. " +
            "Please check that user %s has permission to write to %s, " +
            "or create the directory manually with write permissions.",
          rootPath, user, rootPath), e);
      }
    }

    LOG.info("  FileSystem permissions successfully verified.");
  }
}
