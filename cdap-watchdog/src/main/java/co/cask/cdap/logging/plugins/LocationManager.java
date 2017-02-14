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

import co.cask.cdap.common.conf.Constants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Manage locations for {@link RollingLocationLogAppender}
 */
public class LocationManager implements Flushable, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LocationManager.class);
  protected static final String TAG_NAMESPACE_ID = Constants.Logging.TAG_NAMESPACE_ID;
  protected static final String TAG_APPLICATION_ID = Constants.Logging.TAG_APPLICATION_ID;

  private final Location logBaseDir;
  private Map<LocationIdentifier, LocationOutputStream> activeLocations;
  private String filePermissions;
  private String dirPermissions;

  public LocationManager(LocationFactory locationFactory, String basePath, String dirPermissions,
                         String filePermissions) {
    this.logBaseDir = locationFactory.create(basePath);
    this.activeLocations = new HashMap<>();
    this.dirPermissions = dirPermissions;
    this.filePermissions = filePermissions;
  }

  /**
   * Creates {@link LocationIdentifier} from propertymap
   *
   * @param propertyMap MDC property map which contains namespace id and application id
   * @return returns {@link LocationIdentifier}
   * @throws IllegalArgumentException application id is not present in the property map
   */
  LocationIdentifier getLocationIdentifier(Map<String, String> propertyMap) {

    String namespaceId = propertyMap.get(TAG_NAMESPACE_ID);
    String applicationId = propertyMap.get(TAG_APPLICATION_ID);

    Preconditions.checkArgument(!Strings.isNullOrEmpty(applicationId),
                                String.format("%s is expected but not found in the context %s",
                                              TAG_APPLICATION_ID, propertyMap));

    return new LocationIdentifier(namespaceId, applicationId);
  }

  /**
   * Returns outpustream for log file created as: <basePath>/namespaceId/applicationId/<filePath>
   *
   * @param locationIdentifier location identifier for this event
   * @param filePath           filePath for this event
   * @return returns {@link LocationOutputStream} for an event
   * @throws IOException throws exception while creating a file
   */
  OutputStream getLocationOutputStream(LocationIdentifier locationIdentifier, String filePath) throws IOException {
    if (activeLocations.containsKey(locationIdentifier)) {
      return activeLocations.get(locationIdentifier);
    }

    Location logFile = getLogLocation(locationIdentifier).append(filePath);
    Location logDir = getParent(logFile);

    if (logDir == null) {
      // this should never happen
      LOG.error("Log directories not created");
      throw new IOException(String.format("Parent Directory for %s is null", logFile.toURI().toString()));
    }

    // check if parent directories exist
    mkdirsIfNotExists(logDir, dirPermissions);

    LOG.info("Created directory: {}", logDir.toURI().toString());
    if (logFile.exists()) {
      // The file name for a given application exists if the appender was stopped and then started again but file was
      // not rolled over. In this case, since the roll over size is typically small, we can rename the old file and
      // copy its contents to new file and delete old file.
      long now = System.currentTimeMillis();

      // rename existing file to temp file
      Location tempLocation = logFile.renameTo(logDir.append("temp-" + Long.toString(now)));

      if (tempLocation == null) {
        throw new IOException(String.format("Can not rename file %s", logFile.toURI().toString()));
      }

      try (InputStream inputStream = tempLocation.getInputStream()) {
        // create new file and open outputstream on it
        logFile.createNew(filePermissions);
        // TODO: Handle existing file in a better way rather than copying it over
        OutputStream outputStream = new LocationOutputStream(logFile, logFile.getOutputStream());
        activeLocations.put(locationIdentifier, (LocationOutputStream) outputStream);
        ByteStreams.copy(inputStream, outputStream);
      } catch (IOException e) {
        activeLocations.remove(locationIdentifier);
        throw e;
      }

      try {
        // clean up all the temp files which were failed
        tempLocation.delete();
        for (Location location : logDir.list()) {
          if (location.toURI().toString().contains("temp-")) {
            location.delete();
          }
        }
      } catch (IOException e) {
        // do not throw any exception while deleting temp directory
        LOG.warn("Not able to delete temp location, will be retried later");
      }

    } else {
      // create file with correct permissions
      logFile.createNew(filePermissions);
      activeLocations.put(locationIdentifier, new LocationOutputStream(logFile, logFile.getOutputStream()));
    }

    return activeLocations.get(locationIdentifier);
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

  /**
   * Creates a {@link Location} instance which represents the parent of the given location.
   *
   * @param location location to extra parent from.
   * @return an instance representing the parent location or {@code null} if there is no parent.
   */
  @Nullable
  private static Location getParent(Location location) {
    URI source = location.toURI();

    // If it is root, return null
    if ("/".equals(source.getPath())) {
      return null;
    }

    URI resolvedParent = URI.create(source.toString() + "/..").normalize();
    // NOTE: if there is a trailing slash at the end, rename(), getName() and other operations on file
    // does not work in MapR. so we remove the trailing slash (if any) at the end.
    if (resolvedParent.toString().endsWith("/")) {
      String parent = resolvedParent.toString();
      resolvedParent = URI.create(parent.substring(0, parent.length() - 1));
    }
    return location.getLocationFactory().create(resolvedParent);
  }

  /**
   * Create the directory represented by the location with provided permissions if not exists.
   * @param location the location for the directory.
   * @param permissions permissions on directory
   * @throws IOException If the location cannot be created
   */
  private static void mkdirsIfNotExists(Location location, String permissions) throws IOException {
    // Need to check && mkdir && check to deal with race condition
    if (!location.isDirectory() && !location.mkdirs(permissions) && !location.isDirectory()) {
      throw new IOException("Failed to create directory at " + location);
    }
  }
}
