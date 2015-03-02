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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Abstract class for Upgrade
 */
public abstract class AbstractUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractUpgrader.class);
  protected static final byte[] COLUMN = Bytes.toBytes("c");
  protected static final String DEVELOPER_STRING = "developer";
  protected static final Gson GSON;

  static {
    GsonBuilder builder = new GsonBuilder();
    ApplicationSpecificationAdapter.addTypeAdapters(builder);
    GSON = builder.create();
  }

  protected final LocationFactory locationFactory;

  public AbstractUpgrader(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
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
  protected Location renameLocation(Location oldLocation, Location newLocation) throws IOException {
    if (!newLocation.exists() && oldLocation.exists()) {
      Locations.getParent(newLocation).mkdirs();
      try {
        return oldLocation.renameTo(newLocation);
      } catch (Exception e) {
        newLocation.delete();
        LOG.warn("Failed to rename {} with {}", oldLocation, newLocation);
        throw Throwables.propagate(e);
      }
    }
    return null;
  }

  /**
   * Checks if they given key to be valid from the supplied key prefixes
   *
   * @param key           the key to be validated
   * @param validPrefixes the valid prefixes
   * @return boolean which is true if the key start with validPrefixes else false
   */
  protected boolean isKeyValid(String key, String[] validPrefixes) {
    for (String validPrefix : validPrefixes) {
      if (key.startsWith(validPrefix)) {
        return true;
      }
    }
    return false;
  }
}
