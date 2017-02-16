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

package co.cask.cdap.logging.clean;

import co.cask.cdap.common.io.Locations;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * cleanup expired log files
 */
public class LogCleaner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);

  private final FileMetadataScanner fileMetadataScanner;
  private final LocationFactory locationFactory;
  private final long retentionDurationMs;
  private final int transactionTimeout;

  public LogCleaner(FileMetadataScanner fileMetadataScanner, LocationFactory locationFactory,
                    long retentionDurationMs, int fileCleanupTransactionTimeout) {
    this.fileMetadataScanner = fileMetadataScanner;
    this.locationFactory = locationFactory;
    this.retentionDurationMs = retentionDurationMs;
    this.transactionTimeout = fileCleanupTransactionTimeout;
    LOG.debug("Log retention duration = {}ms", retentionDurationMs);
  }

  @Override
  public void run() {
    long startTime = System.currentTimeMillis();
    LOG.info("Starting log cleanup at {}", startTime);
    long tillTime = startTime - retentionDurationMs;
    List<FileMetadataScanner.DeleteEntry> deleteEntries =
      fileMetadataScanner.scanAndGetFilesToDelete(tillTime, transactionTimeout);
    int deleteCount = 0;
    int failureCount = 0;
    for (FileMetadataScanner.DeleteEntry deleteEntry : deleteEntries) {
      try {
        boolean status = Locations.getLocationFromAbsolutePath(locationFactory,
                                                               deleteEntry.getLocationIdentifier().getPath()).delete();
        if (!status) {
          failureCount++;
          LOG.warn("File {} delete failed", deleteEntry.getLocationIdentifier());
        } else {
          deleteCount++;
          LOG.trace("File {} deleted by log cleanup", deleteEntry.getLocationIdentifier());
        }
      } catch (IOException e) {
        LOG.warn("Exception while deleting file {}", deleteEntry.getLocationIdentifier(), e);
      }
    }
    LOG.info("File cleanup completed, Successful file deletes {} Failed file deletes {}", deleteCount, failureCount);
    long completionTime = System.currentTimeMillis();
    LOG.info("Log cleanup completed at {} took {}ms", System.currentTimeMillis(), (completionTime - startTime));
  }
}
