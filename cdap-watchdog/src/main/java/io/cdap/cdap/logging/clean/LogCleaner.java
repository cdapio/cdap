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
import io.cdap.cdap.logging.appender.system.LogFileManager;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * cleanup expired log files
 */
public class LogCleaner {
  public static final int FOLDER_CLEANUP_BATCH_SIZE = 100000;

  private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);
  private static final long MILLISECONDS_IN_DAY = TimeUnit.DAYS.toMillis(1);
  private static final long DEFAULT_DELAY_IN_MILLIS = -1L;
  private static final long FOLDER_CLEANUP_DELAY_IN_MILLIS = TimeUnit.HOURS.toMillis(1);
  private static final int EXCLUDE_FOLDER_CLEANUP_DAYS = 2;

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
        boolean status = Locations.getLocationFromAbsolutePath(locationFactory, deletedEntry.getPath()).delete();
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
    LOG.info("File cleanup completed, Successful file deletes - {}. Failed file deletes - {}. Log Cleanup took {} ms",
        deleteCount, failureCount, (completionTime - startTime));

    return folderCleanupCount < folderCleanupBatchSize ? DEFAULT_DELAY_IN_MILLIS : FOLDER_CLEANUP_DELAY_IN_MILLIS;
  }

  private void cleanupLogFolders() {
    LOG.info("Starting log folder cleanup");
    try {
      long startTime = System.currentTimeMillis();
      LOG.debug("Starting log folder cleanup");
      folderCleanupCount = 0;

      // TODO: (CDAP-19437): Exclude recent log folders during cleanups in LogCleaner service.
      // For now exclude folders for current date and current date - 1.
      long currentTime = System.currentTimeMillis();
      Set<String> excludeFolders = new HashSet<String>();
      for (int day = 0; day < EXCLUDE_FOLDER_CLEANUP_DAYS; day++) {
        long time = currentTime - day * MILLISECONDS_IN_DAY;
        String date = LogFileManager.formatLogDirectoryName(time);
        excludeFolders.add(date);
      }

      // Clean up empty folders
      cleanupEmptyFolders(logsDirectoryLocation, excludeFolders);
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
   *         folders are cleaned up, true otherwise.
   * @throws IOException
   */
  private boolean cleanupEmptyFolders(@Nullable Location location, Set<String> excludeFolders) throws IOException {
    if (location == null) {
      return true;
    }
    if (!location.isDirectory()) {
      // Parent directory has files.
      return false;
    }
    if (excludeFolders.contains(location.getName())) {
      // No need to traverse excluded folders
      return false;
    }
    if (folderCleanupCount >= folderCleanupBatchSize) {
      return false;
    }

    boolean isEmpty = true;
    List<Location> children = location.list();
    for (Location child : children) {
      if (!cleanupEmptyFolders(child, excludeFolders)) {
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
