/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.logging.write;

import co.cask.cdap.common.io.Locations;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Handles log file retention.
 */
public final class LogCleanup implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogCleanup.class);

  private final FileMetaDataManager fileMetaDataManager;
  private final Location rootDir;
  private final long retentionDurationMs;

  public LogCleanup(FileMetaDataManager fileMetaDataManager, Location rootDir, long retentionDurationMs) {
    this.fileMetaDataManager = fileMetaDataManager;
    this.rootDir = rootDir;
    this.retentionDurationMs = retentionDurationMs;

    LOG.info("Log retention duration = {} ms", retentionDurationMs);
  }

  @Override
  public void run() {
    LOG.info("Running log cleanup...");
    try {
      long tillTime = System.currentTimeMillis() - retentionDurationMs;
      final SetMultimap<String, Location> parentDirs = HashMultimap.create();
      fileMetaDataManager.cleanMetaData(tillTime,
                                        new FileMetaDataManager.DeleteCallback() {
                                          @Override
                                          public void handle(Location location, String namespacedLogBaseDir) {
                                            try {
                                              if (location.exists()) {
                                                LOG.info("Deleting log file {}", location.toURI());
                                                location.delete();
                                              }
                                              parentDirs.put(namespacedLogBaseDir, getParent(location));
                                            } catch (IOException e) {
                                              LOG.error("Got exception when deleting path {}", location.toURI(), e);
                                              throw Throwables.propagate(e);
                                            }
                                          }
                                        });
      // Delete any empty parent dirs
      for (String namespacedLogBaseDir : parentDirs.keySet()) {
        Set<Location> locations = parentDirs.get(namespacedLogBaseDir);
        for (Location location : locations) {
          deleteEmptyDir(namespacedLogBaseDir, location);
        }
      }
    } catch (Throwable e) {
      LOG.error("Got exception when cleaning up. Will try again later.", e);
    }
  }

  Location getParent(Location location) {
    Location parent = Locations.getParent(location);
    return (parent == null) ? location : parent;
  }

  /**
   * For the specified directory to be deleted, finds its namespaced log location, then deletes
   * @param namespacedLogBaseDir namespaced log base dir without the root dir prefixed
   * @param dir dir to delete
   * @throws IOException
   */
  void deleteEmptyDir(String namespacedLogBaseDir, Location dir) throws IOException {
    LOG.debug("Got path {}", dir.toURI());
    Location namespacedLogBaseLocation = rootDir.append(namespacedLogBaseDir);
    deleteEmptyDirsInNamespace(namespacedLogBaseLocation, dir);
  }

  /**
   * Given a namespaced log dir - e.g. /{root}/ns1/logs, deletes dir if it is empty, and recursively deletes parent dirs
   * if they are empty too. The recursion stops at non-empty parent or the specified namespaced log base directory.
   * If dir is not child of base directory then the recursion stops at root.
   * @param dirToDelete dir to be deleted.
   */
  private void deleteEmptyDirsInNamespace(Location namespacedLogBaseDir, Location dirToDelete) {
    // Don't delete a dir if it is equal to or a parent of logBaseDir
    if (namespacedLogBaseDir.toURI().equals(dirToDelete.toURI()) ||
      !dirToDelete.toURI().getRawPath().startsWith(namespacedLogBaseDir.toURI().getRawPath())) {
      LOG.debug("{} not deletion candidate.", dirToDelete.toURI());
      return;
    }

    try {
      if (dirToDelete.list().isEmpty() && dirToDelete.delete()) {
        LOG.info("Deleted empty dir {}", dirToDelete.toURI());

        // See if parent dir is empty, and needs deleting
        Location parent = getParent(dirToDelete);
        LOG.debug("Deleting parent dir {}", parent);
        deleteEmptyDirsInNamespace(namespacedLogBaseDir, parent);
      } else {
        LOG.debug("Not deleting non-dir or non-empty dir {}", dirToDelete.toURI());
      }
    } catch (IOException e) {
      LOG.error("Got exception while deleting dir {}", dirToDelete.toURI(), e);
    }
  }
}
