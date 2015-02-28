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
  public ArchiveUpgrader(LocationFactory locationFactory, CConfiguration cConf) {
    super(locationFactory);
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
    Location oldLocation = locationFactory.create(cConf.get(Constants.Stream.BASE_DIR));
    Location newLocation = Locations.getParent(oldLocation).append(Constants.DEFAULT_NAMESPACE)
      .append(cConf.get(Constants.Stream.BASE_DIR));
    if (renameLocation(oldLocation, newLocation) != null) {
      LOG.info("Upgraded streams archives from {} to {}", oldLocation, newLocation);
    }
  }

  /**
   * Upgraded the logs path
   */
  private void upgradeLogs() throws IOException {
    // move log files under cdap and developer dir
    Location oldLogLocation1 = locationFactory.create("logs").append("avro").append(Constants.Logging.SYSTEM_NAME);
    Location oldLogLocation2 = locationFactory.create("logs").append("avro").append(DEVELOPER_STRING);

    //temporary location
    Location tempLocation1 = locationFactory.create("temp1");
    Location tempLocation2 = locationFactory.create("temp2");

    // create the new namespace locations
    Location newLocation1 = locationFactory.create(Constants.SYSTEM_NAMESPACE).append("logs").append("avro");
    Location newLocation2 = locationFactory.create(Constants.DEFAULT_NAMESPACE).append("logs").append("avro");

    // move to temp location
    renameLocation(oldLogLocation1, tempLocation1);
    renameLocation(tempLocation1, newLocation1);

    // move to new namespaced location
    renameLocation(oldLogLocation2, tempLocation2);
    renameLocation(tempLocation2, newLocation2);
  }
}
