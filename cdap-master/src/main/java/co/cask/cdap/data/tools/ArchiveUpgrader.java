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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.logging.LoggingConfiguration;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Class to Upgrade the archive directories
 */
public class ArchiveUpgrader extends AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveUpgrader.class);
  private final CConfiguration cConf;

  @Inject
  private ArchiveUpgrader(LocationFactory locationFactory, NamespacedLocationFactory namespacedLocationFactory,
                          CConfiguration cConf) {
    super(locationFactory, namespacedLocationFactory);
    this.cConf = cConf;
  }

  @Override
  public void upgrade() throws Exception {
    upgradeStream();
    upgradeLogs();
  }

  /**
   * Upgrades the streams archive path
   */
  private void upgradeStream() throws IOException {
    LOG.info("Upgrading stream files ...");
    Location oldLocation = locationFactory.create(cConf.get(Constants.Stream.BASE_DIR));
    Location newLocation = namespacedLocationFactory.get(Constants.DEFAULT_NAMESPACE_ID)
      .append(cConf.get(Constants.Stream.BASE_DIR));
    if (renameLocation(oldLocation, newLocation) != null) {
      LOG.info("Upgraded streams archives from {} to {}", oldLocation, newLocation);
    }
  }

  /**
   * Upgraded the logs path
   */
  private void upgradeLogs() throws IOException {
    LOG.info("Upgrading log files ...");
    String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    Location defaultNamespacedLogDir = namespacedLocationFactory.get(Constants.SYSTEM_NAMESPACE_ID)
        .append(cConf.get(LoggingConfiguration.LOG_BASE_DIR));
    renameLocation(locationFactory.create(logBaseDir).append(Constants.CDAP_NAMESPACE), defaultNamespacedLogDir);
    //TODO: This developer string in 2.7 is default so we need to handle that here when we improvise on this tool for
    //2.7 version
    Location systemNamespacedLogDir = namespacedLocationFactory.get(Constants.DEFAULT_NAMESPACE_ID)
        .append(cConf.get(LoggingConfiguration.LOG_BASE_DIR));
    renameLocation(locationFactory.create(logBaseDir).append(Constants.DEVELOPER_ACCOUNT), systemNamespacedLogDir);
  }
}
