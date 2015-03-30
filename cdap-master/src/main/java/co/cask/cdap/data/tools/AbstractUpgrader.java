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

import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Abstract class for Upgrade
 */
public abstract class AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractUpgrader.class);
  protected final LocationFactory locationFactory;
  protected final NamespacedLocationFactory namespacedLocationFactory;

  public AbstractUpgrader(LocationFactory locationFactory, NamespacedLocationFactory namespacedLocationFactory) {
    this.locationFactory = locationFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
  }

  /**
   * Modules which want to upgrade should provide a definition for this
   *
   * @throws Exception if the upgrade failed
   */
  abstract void upgrade() throws Exception;

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
      LOG.debug("Failed to perform rename. Either the new location {} already exists or old location {} " +
                  "does not exist.", newLocation, oldLocation);
      return null;
    }
  }
}
