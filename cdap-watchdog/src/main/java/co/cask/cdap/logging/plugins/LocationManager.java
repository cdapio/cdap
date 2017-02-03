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

import co.cask.cdap.common.io.Locations;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Manage locations for {@link RollingLocationLogAppender}
 */
public class LocationManager implements Flushable, Closeable {
  protected static final String TAG_NAMESPACE_ID = ".namespaceId";
  protected static final String TAG_APPLICATION_ID = ".applicationId";

  private Location logBaseDir;
  private Map<LocationIdentifier, LocationOutputStream> activeLocations;
  private String filePermissions;

  public LocationManager(LocationFactory locationFactory, String basePath, String filePermissions)
    throws IOException {
    this.logBaseDir = locationFactory.create(basePath);
    this.activeLocations = new HashMap<>();
    this.filePermissions = filePermissions;
  }

  LocationIdentifier getLocationIdentifier(Map<String, String> propertyMap) throws IllegalArgumentException,
    IOException {

    String namespaceId = propertyMap.get(TAG_NAMESPACE_ID);
    Preconditions.checkArgument(propertyMap.containsKey(TAG_APPLICATION_ID),
                                String.format("%s is expected but not found in the context %s",
                                              TAG_APPLICATION_ID, propertyMap));
    String application = propertyMap.get(TAG_APPLICATION_ID);

    return new LocationIdentifier(namespaceId, application);
  }

  OutputStream getLocationOutputStream(final LocationIdentifier locationIdentifier, String fileName)
    throws IOException {
    if (activeLocations.containsKey(locationIdentifier)) {
      return activeLocations.get(locationIdentifier).getOutputStream();
    }

    Location logFile = getLogLocation(locationIdentifier).append(fileName);
    Location logDir = Locations.getParent(logFile);
    // check if parent directories exist
    Locations.mkdirsIfNotExists(logDir);

    if (logFile.exists()) {
      // The file name for a given application exists if the appender was stopped and then started again but file was
      // not rolled over. In this case, since the roll over size is typically small, we can rename the old file and
      // copy its contents to new file and delete old file
      long now = System.currentTimeMillis();
      // rename existing file to temp file
      if (logDir == null) {
        // this should never happen
        throw new IOException(String.format("Parent Directory for %s is null", logFile.toURI().toString()));
      }
      Location tempLocation = logFile.renameTo(logDir.append(Long.toString(now)));
      logFile.createNew(filePermissions);

      if (tempLocation == null) {
        throw new IOException(String.format("Can not rename file %s", logFile.toURI().toString()));
      }

      OutputStream outputStream = logFile.getOutputStream();
      try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(tempLocation.getInputStream()))) {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          outputStream.write(line.getBytes());
        }
      }
      // delete temporary file
      tempLocation.delete();
      activeLocations.put(locationIdentifier, new LocationOutputStream(logFile, outputStream));
    } else {
      // create file with correct permissions
      logFile.createNew(filePermissions);
      activeLocations.put(locationIdentifier, new LocationOutputStream(logFile, logFile.getOutputStream()));
    }

    return activeLocations.get(locationIdentifier).getOutputStream();
  }

  /**
   * Closes all open output streams and clears cache
   */
  public void close() {
    Collection<LocationOutputStream> locations = new ArrayList<>(activeLocations.values());
    activeLocations.clear();

    for (LocationOutputStream locationOutputStream : locations) {
      // we do not want to throw any exception rather close all the open output streams. so close quietly
      Closeables.closeQuietly(locationOutputStream);
    }
  }

  /**
   * Flushes all the open output streams
   */
  @Override
  public void flush() throws IOException {
    Collection<LocationOutputStream> locations = new ArrayList<>(activeLocations.values());
    for (LocationOutputStream locationOutputStream : locations) {
      locationOutputStream.flush();
    }
  }

  /**
   * Appends information from location identifier to logBaseDir
   */
  Location getLogLocation(LocationIdentifier locationIdentifier) throws IOException {
    return logBaseDir.append(locationIdentifier.getNamespaceId()).append(locationIdentifier.getApplicationId());
  }

  @VisibleForTesting
  Map<LocationIdentifier, LocationOutputStream> getActiveLocations() {
    return activeLocations;
  }
}
