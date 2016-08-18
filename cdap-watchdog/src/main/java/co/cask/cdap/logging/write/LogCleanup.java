/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Handles log file retention.
 */
public final class LogCleanup implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogCleanup.class);

  private final FileMetaDataManager fileMetaDataManager;
  private final RootLocationFactory rootLocationFactory;
  private final long retentionDurationMs;
  private final Impersonator impersonator;

  // this class takes a root location factory because for custom mapped namespaces the namespace is mapped to a
  // location from the root of system and the logs are generated in the custom mapped location. To clean up  these
  // locations we need to work with root based location factory
  public LogCleanup(FileMetaDataManager fileMetaDataManager, RootLocationFactory rootLocationFactory,
                    long retentionDurationMs, Impersonator impersonator) {
    this.fileMetaDataManager = fileMetaDataManager;
    this.rootLocationFactory = rootLocationFactory;
    this.retentionDurationMs = retentionDurationMs;
    this.impersonator = impersonator;

    LOG.debug("Log retention duration = {} ms", retentionDurationMs);
  }

  @Override
  public void run() {
    LOG.info("Running log cleanup...");
    try {
      long tillTime = System.currentTimeMillis() - retentionDurationMs;
      final SetMultimap<String, Location> parentDirs = HashMultimap.create();
      final Map<String, NamespaceId> namespacedLogBaseDirMap = new HashMap<>();
      fileMetaDataManager.cleanMetaData(tillTime,
                                        new FileMetaDataManager.DeleteCallback() {
                                          @Override
                                          public void handle(NamespaceId namespaceId, final Location location,
                                                             final String namespacedLogBaseDir) {
                                            try {
                                              impersonator.doAs(namespaceId, new Callable<Void>() {
                                                @Override
                                                public Void call() throws Exception {
                                                  if (location.exists()) {
                                                    LOG.info("Deleting log file {}", location);
                                                    location.delete();
                                                    parentDirs.put(namespacedLogBaseDir, getParent(location));
                                                  }
                                                  return null;
                                                }
                                              });
                                              namespacedLogBaseDirMap.put(namespacedLogBaseDir, namespaceId);
                                            } catch (Exception e) {
                                              LOG.error("Got exception when deleting path {}", location, e);
                                              throw Throwables.propagate(e);
                                            }
                                          }
                                        });
      // Delete any empty parent dirs
      for (final String namespacedLogBaseDir : parentDirs.keySet()) {
        // this ensures that we only do doAs which will make an RPC call only once for a namespace
        NamespaceId namespaceId = namespacedLogBaseDirMap.get(namespacedLogBaseDir);
        impersonator.doAs(namespaceId, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            Set<Location> locations = parentDirs.get(namespacedLogBaseDir);
            for (Location location : locations) {
              deleteEmptyDir(namespacedLogBaseDir, location);
            }
            return null;
          }
        });
      }
    } catch (Throwable e) {
      LOG.error("Got exception when cleaning up. Will try again later.", e);
    }
  }

  private Location getParent(Location location) {
    Location parent = Locations.getParent(location);
    return (parent == null) ? location : parent;
  }

  /**
   * For the specified directory to be deleted, finds its namespaced log location, then deletes
   * @param namespacedLogBaseDir namespaced log base dir without the root dir prefixed
   * @param dir dir to delete
   */
  void deleteEmptyDir(String namespacedLogBaseDir, Location dir) {
    LOG.debug("Got path {}", dir);
    Location namespacedLogBaseLocation = rootLocationFactory.create(namespacedLogBaseDir);
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
    URI namespacedLogBaseURI = namespacedLogBaseDir.toURI();
    URI dirToDeleteURI = dirToDelete.toURI();
    if (namespacedLogBaseURI.equals(dirToDeleteURI) ||
      !dirToDeleteURI.getRawPath().startsWith(namespacedLogBaseURI.getRawPath())) {
      LOG.debug("{} not deletion candidate.", dirToDelete);
      return;
    }

    try {
      if (dirToDelete.list().isEmpty() && dirToDelete.delete()) {
        LOG.info("Deleted empty dir {}", dirToDelete);

        // See if parent dir is empty, and needs deleting
        Location parent = getParent(dirToDelete);
        LOG.debug("Deleting parent dir {}", parent);
        deleteEmptyDirsInNamespace(namespacedLogBaseDir, parent);
      } else {
        LOG.debug("Not deleting non-dir or non-empty dir {}", dirToDelete);
      }
    } catch (IOException e) {
      LOG.error("Got exception while deleting dir {}", dirToDelete, e);
    }
  }
}
