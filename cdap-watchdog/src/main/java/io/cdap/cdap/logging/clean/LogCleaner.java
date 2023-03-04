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

package io.cdap.cdap.logging.clean;

import io.cdap.cdap.common.io.Locations;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * cleanup expired log files
 */
public class LogCleaner {

  public static final int FOLDER_CLEANUP_BATCH_SIZE = 100000;

  private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);
  private static final long DEFAULT_DELAY_IN_MILLIS = -1L;
  private static final long FOLDER_CLEANUP_DELAY_IN_MILLIS = TimeUnit.HOURS.toMillis(1);

  private final FileMetadataCleaner fileMetadataCleaner;
  private final LocationFactory locationFactory;
  private final Location logsDirectoryLocation;
  private final long retentionDurationMs;
  private final int folderCleanupBatchSize;
  private final int fileCleanupBatchSize;

  private int folderCleanupCount;

  public LogCleaner(FileMetadataCleaner fileMetadataCleaner, LocationFactory locationFactory,
      Location logsDirectoryLocation, long retentionDurationMs, int folderCleanupBatchSize,
      int fileCleanupBatchSize) {
    this.fileMetadataCleaner = fileMetadataCleaner;
    this.locationFactory = locationFactory;
    this.logsDirectoryLocation = logsDirectoryLocation;
    this.retentionDurationMs = retentionDurationMs;
    this.folderCleanupBatchSize = folderCleanupBatchSize;
    this.fileCleanupBatchSize = fileCleanupBatchSize;
    LOG.debug("Log retention duration = {}ms", retentionDurationMs);
  }

  public long run() {
    cleanupLogFolders();

    LOG.info("Starting log cleanup");
    long startTime = System.currentTimeMillis();
    long tillTime = startTime - retentionDurationMs;
    List<FileMetadataCleaner.DeletedEntry> deleteEntries =
        fileMetadataCleaner.scanAndGetFilesToDelete(tillTime, fileCleanupBatchSize);
    int deleteCount = 0;
    int failureCount = 0;
    for (FileMetadataCleaner.DeletedEntry deletedEntry : deleteEntries) {
      try {
        boolean status = Locations.getLocationFromAbsolutePath(locationFactory,
            deletedEntry.getPath()).delete();
        if (!status) {
          failureCount++;
          LOG.warn("File {} delete failed", deletedEntry.getPath());
        } else {
          deleteCount++;
          LOG.trace("File {} deleted by log cleanup", deletedEntry.getPath());
          deleteDirectoryIfEmpty(deletedEntry.getPath());
        }
      } catch (IOException e) {
        LOG.warn("Exception while deleting file {}", deletedEntry.getPath(), e);
      }
    }
    long completionTime = System.currentTimeMillis();
    LOG.info(
        "File cleanup completed, Successful file deletes - {}. Failed file deletes - {}. Log Cleanup took {} ms",
        deleteCount, failureCount, (completionTime - startTime));

    return folderCleanupCount < folderCleanupBatchSize ? DEFAULT_DELAY_IN_MILLIS
        : FOLDER_CLEANUP_DELAY_IN_MILLIS;
  }

  private void cleanupLogFolders() {
    LOG.info("Starting log folder cleanup");
    try {
      long startTime = System.currentTimeMillis();
      LOG.debug("Starting log folder cleanup {}", startTime);
      folderCleanupCount = 0;

      // Exclude folders which were last modified within retention period.
      // Clean up stale empty folders inside logs folder.
      long checkpointTimestamp = startTime - retentionDurationMs;
      for (Location location : logsDirectoryLocation.list()) {
        cleanupFoldersIfEmpty(location, checkpointTimestamp);
      }

      LOG.debug("Completed cleaning up {} log folders in {} ms", folderCleanupCount,
          System.currentTimeMillis() - startTime);
    } catch (IOException ex) {
      LOG.warn("Exception while deleting log folders", ex);
    }
  }

  /**
   * Traverses folders in Depth-First manner and recursively deletes the folder in bottom-up manner
   * if it is empty or sub folders are empty.
   *
   * @param location Root folder to start the traversal.
   * @return Returns false if the folder cannot be cleaned up if it has files or maximum number of
   *     folders are cleaned up, true otherwise.
   */
  private boolean cleanupFoldersIfEmpty(Location location, long checkpointTimestamp)
      throws IOException {
    // Cleanup in not required in cases where:
    // 1. Parent directory has files.
    // 2. Folder is within retention period.
    // 3. Cleanup count is greater than or equal to batch size.
    if (!location.isDirectory()
        || folderCleanupCount >= folderCleanupBatchSize) {
      return false;
    }

    boolean isEmpty = true;
    List<Location> childLocations = location.list();
    for (Location childLocation : childLocations) {
      if (childLocation.lastModified() > checkpointTimestamp) {
        isEmpty = false;
      }
      if (!cleanupFoldersIfEmpty(childLocation, checkpointTimestamp)) {
        isEmpty = false;
        if (folderCleanupCount >= folderCleanupBatchSize) {
          break;
        }
      }
    }

    // If the sub folders were empty, delete the current folder.
    if (isEmpty) {
      if (location.delete()) {
        folderCleanupCount++;
      } else {
        isEmpty = false;
      }
    }

    return isEmpty;
  }

  private void deleteDirectoryIfEmpty(String logFilePath) throws IOException {
    Location logFileLocation = Locations.getLocationFromAbsolutePath(locationFactory, logFilePath);
    Location folderLocation = Locations.getParent(logFileLocation);
    while (Locations.LOCATION_COMPARATOR.compare(logsDirectoryLocation, folderLocation) != 0) {
      if (!folderLocation.delete()) {
        // Deletion will fail if folder is empty. Stop the bottom-up deletion.
        break;
      }

      folderLocation = Locations.getParent(folderLocation);
    }
  }
}
