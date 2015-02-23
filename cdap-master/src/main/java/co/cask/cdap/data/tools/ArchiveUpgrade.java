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

import co.cask.cdap.common.conf.Constants;
import com.google.inject.Injector;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


/**
 * Class to Upgrade the archive directories
 */
public class ArchiveUpgrade extends AbstractUpgrade implements Upgrade {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveUpgrade.class);

  @Override
  public void upgrade(Injector injector) throws Exception {
    upgradeStreamFilepath();
  }

  /**
   * Upgrades the streams archive path
   */
  private void upgradeStreamFilepath() throws IOException {
    List<Location> streamLocations = locationFactory.create(EMPTY_STRING).append(FORWARD_SLASH +
                                                                                   Constants.Service.STREAMS +
                                                                                   FORWARD_SLASH).list();
    for (Location oldLocation : streamLocations) {
      Location newLocation = locationFactory.create(FORWARD_SLASH + Constants.DEFAULT_NAMESPACE + FORWARD_SLASH +
                                                      Constants.Service.STREAMS + FORWARD_SLASH +
                                                      oldLocation.getName());
      LOG.info("Upgrading Stream archive for {}", oldLocation);
      renameLocation(oldLocation, newLocation);
    }
  }
}
