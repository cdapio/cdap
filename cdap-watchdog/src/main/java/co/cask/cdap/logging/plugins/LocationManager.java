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

package co.cask.cdap.logging.plugins;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Manage locations for {@link RollingLocationLogAppender}
 */
public class LocationManager {
  protected static final String TAG_NAMESPACE_ID = ".namespaceId";
  protected static final String TAG_APPLICATION_ID = ".applicationId";

  private Location logDirLocation;

  private Map<LocationIdentifier, OutputStream> activeFiles;
  private Map<LocationIdentifier, Location> activeFilesToLocation;

  public LocationManager(LocationFactory locationFactory) {
    this.logDirLocation = locationFactory.create("plugins/logs");
    this.activeFiles = new HashMap<>();
    this.activeFilesToLocation = new HashMap<>();
  }


  protected LocationIdentifier getLocationIdentifier(Map<String, String> propertyMap) throws IllegalArgumentException,
    IOException {

    String namespaceId = propertyMap.get(TAG_NAMESPACE_ID);
    Preconditions.checkArgument(propertyMap.containsKey(TAG_APPLICATION_ID),
                                String.format("%s is expected but not found in the context %s",
                                              TAG_APPLICATION_ID, propertyMap));
    String application = propertyMap.get(TAG_APPLICATION_ID);

    return new LocationIdentifier(namespaceId, application);
  }

  protected OutputStream getLocationOutputStream(LocationIdentifier locationIdentifier, String fileName)
    throws IOException {
    if (activeFiles.containsKey(locationIdentifier)) {
      return activeFiles.get(locationIdentifier);
    }

    ensureDirectoryCheck(logDirLocation);
    Location contextLocation = getLogLocation(locationIdentifier);
    ensureDirectoryCheck(contextLocation);

    Location location = contextLocation.append(fileName);
    activeFiles.put(locationIdentifier, location.getOutputStream());
    activeFilesToLocation.put(locationIdentifier, location);
    return activeFiles.get(locationIdentifier);
  }

  protected Location getLogLocation(LocationIdentifier locationIdentifier) throws IOException {
    return logDirLocation.append(locationIdentifier.getNamespaceId()).append(locationIdentifier.getApplicationId());
  }

  protected void ensureDirectoryCheck(Location location) throws IOException {
    if (!location.exists()) {
      location.mkdirs();
    } else {
      if (!location.isDirectory()) {
        throw new IOException(
          String.format("File Exists at the logging location %s, Expected to be a directory", location));
      }
    }
  }

  public void close() {
    Collection<OutputStream> locations = new ArrayList<>(activeFiles.values());
    activeFiles.clear();
    activeFilesToLocation.clear();

    for (OutputStream outputStream : locations) {
      closeOutputStream(outputStream);
    }
  }

  protected void closeOutputStream(OutputStream outputStream) {
    Closeables.closeQuietly(outputStream);
  }

  public Location getLogDirLocation() {
    return logDirLocation;
  }

  public Map<LocationIdentifier, OutputStream> getActiveFiles() {
    return activeFiles;
  }

  public Map<LocationIdentifier, Location> getActiveFilesLocations() {
    return activeFilesToLocation;
  }
}
