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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
  protected static final String DEVELOPER_ACCOUNT = "developer";
  protected static final Gson GSON;
  protected static final byte[] COLUMN = Bytes.toBytes("c");

  protected final LocationFactory locationFactory;

  static {
    GsonBuilder builder = new GsonBuilder();
    ApplicationSpecificationAdapter.addTypeAdapters(builder);
    GSON = builder.create();
  }

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
  @Nullable
  protected Location renameLocation(Location oldLocation, Location newLocation) throws IOException {
    // if the newLocation does not exists or the oldLocation does we try to rename. If either one of them is false then
    // the underlying call to renameTo will throw IOException which we propagate.
    if (!newLocation.exists() || oldLocation.exists()) {
      Locations.getParent(newLocation).mkdirs();
      try {
        return oldLocation.renameTo(newLocation);
      } catch (IOException ioe) {
        newLocation.delete();
        LOG.warn("Failed to rename {} to {}", oldLocation, newLocation);
        throw ioe;
      }
    } else {
      LOG.debug("New location {} already exists and old location {} does not exists. The location might already be " +
                  "updated.", newLocation, oldLocation);
      return null;
    }
  }
}
