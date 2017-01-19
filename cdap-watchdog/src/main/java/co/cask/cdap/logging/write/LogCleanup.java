/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.LocationStatus;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.io.Processor;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Handles log file retention.
 */
public final class LogCleanup implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogCleanup.class);
  private static final int MAX_DISK_FILES_SCANNED = 50000;

  private final FileMetaDataManager fileMetaDataManager;
  private final RootLocationFactory rootLocationFactory;
  private final String logBaseDir;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final long retentionDurationMs;
  private final long maxMetaFileScanned;
  private final Impersonator impersonator;

  // this class takes a root location factory because for custom mapped namespaces the namespace is mapped to a
  // location from the root of system and the logs are generated in the custom mapped location. To clean up  these
  // locations we need to work with root based location factory
  public LogCleanup(FileMetaDataManager fileMetaDataManager, RootLocationFactory rootLocationFactory,
                    NamespaceQueryAdmin namespaceQueryAdmin, NamespacedLocationFactory namespacedLocationFactory,
                    String logBaseDir, long retentionDurationMs, CConfiguration cConf, Impersonator impersonator) {
    this.fileMetaDataManager = fileMetaDataManager;
    this.rootLocationFactory = rootLocationFactory;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.logBaseDir = logBaseDir;
    this.retentionDurationMs = retentionDurationMs;
    this.maxMetaFileScanned = cConf.getLong(LoggingConfiguration.LOG_CLEANUP_MAX_NUM_FILES);
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

      cleanupFiles(tillTime, maxMetaFileScanned, namespacedLogBaseDirMap, parentDirs);

      try {
        // clean log files which does not have corresponding meta data
        cleanFilesWithoutMeta(tillTime, namespacedLogBaseDirMap, parentDirs,
                              MAX_DISK_FILES_SCANNED, maxMetaFileScanned);
      } catch (Exception e) {
        LOG.warn("Got exception while cleaning up disk files without meta data", e);
      }

      // Delete any empty parent dirs
      for (final String namespacedLogBaseDir : parentDirs.keySet()) {
        // this ensures that we only do doAs which will make an RPC call only once for a namespace
        NamespaceId namespaceId = namespacedLogBaseDirMap.get(namespacedLogBaseDir);
        try {
          impersonator.doAs(namespaceId, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              Set<Location> locations = parentDirs.get(namespacedLogBaseDir);
              for (Location location : locations) {
                try {
                  deleteEmptyDir(namespacedLogBaseDir, location);
                } catch (Exception e) {
                  LOG.warn("Got exception while deleting empty directory {}", location, e);
                }
              }
              return null;
            }
          });
        } catch (Exception e) {
          LOG.warn("Got exception while deleting parent directories in namespace {}", namespaceId.getEntityName(), e);
        }
      }
    } catch (Throwable e) {
      LOG.error("Got exception when cleaning up. Will try again later.", e);
    }
  }

  /**
   * Clean up metadata and disk files which are older than tillTime
   *
   * @param tillTime                time till the meta data will be deleted.
   * @param batchSize               max number files to be deleted in one iteration of log cleanup
   * @param namespacedLogBaseDirMap namespace to directory map
   * @param parentDirs              parent directories for deleted files
   */
  @VisibleForTesting
  void cleanupFiles(final long tillTime, final long batchSize,
                    final Map<String, NamespaceId> namespacedLogBaseDirMap,
                    final SetMultimap<String, Location> parentDirs) {
    FileMetaDataManager.TableKey nextTableKey = null;

    do {
      long count = 0;
      List<FileMetaDataManager.ScannedEntryInfo> metaEntriesToDelete = new ArrayList<>();

      // Get the log metadata in batches
      FileMetaDataManager.MetaEntryProcessor<List<FileMetaDataManager.ScannedEntryInfo>> metaEntryProcessor =
        new FileMetaDataManager.MetaEntryProcessor<List<FileMetaDataManager.ScannedEntryInfo>>() {
          List<FileMetaDataManager.ScannedEntryInfo> scannedFiles = new ArrayList<>();

          @Override
          public void process(FileMetaDataManager.ScannedEntryInfo scannedFileInfo) {
            scannedFiles.add(scannedFileInfo);
          }

          @Override
          public List<FileMetaDataManager.ScannedEntryInfo> getCollectedEntries() {
            return scannedFiles;
          }
        };

      nextTableKey = fileMetaDataManager.scanFiles(nextTableKey, batchSize, metaEntryProcessor);

      for (FileMetaDataManager.ScannedEntryInfo scannedEntryInfo : metaEntryProcessor.getCollectedEntries()) {
        final URI file = scannedEntryInfo.getUri();

        try {
          Location fileLocation = impersonator.doAs(scannedEntryInfo.getNamespace(), new Callable<Location>() {
            @Override
            public Location call() throws Exception {
              return rootLocationFactory.create(file);
            }
          });

          if (!fileLocation.exists()) {
            LOG.warn("Log file {} does not exist, but metadata is present", fileLocation.toString());
            metaEntriesToDelete.add(scannedEntryInfo);
            count++;
          } else if (fileLocation.lastModified() < tillTime) {
            try {
              deleteLogFiles(parentDirs, namespacedLogBaseDirMap, scannedEntryInfo.getNamespace(), fileLocation);
              metaEntriesToDelete.add(scannedEntryInfo);
              count++;
            } catch (Exception e) {
              LOG.warn("Got exception processing deleting file {}", fileLocation, e);
            }
          }

        } catch (Exception e) {
          if (e instanceof NamespaceNotFoundException) {
            LOG.warn("Namespace does not exist for {}. Going to delete metadata for it", file.toString(), e);
            metaEntriesToDelete.add(scannedEntryInfo);
            count++;
          } else {
            LOG.warn("Got exception while accessing path {}", file.toString(), e);
          }
        }
      }

      LOG.debug("Collected {} number of metadata entries for deletion", count);

      // Remove metadata entries
      fileMetaDataManager.cleanMetadata(metaEntriesToDelete);

    } while (nextTableKey != null);
  }

  /**
   * Clean log files which does not have corresponding meta data
   *
   * @param tillTime                time till the meta data will be deleted.
   * @param namespacedLogBaseDirMap namespace to directory map
   * @param parentDirs              parent directories for deleted files
   * @param maxDiskFilesScanned     batch size for disk files to be scanned
   * @param maxMetaEntriesScanned   batch size for metadata entries to be scanned
   * @throws Exception Any exception occurred while getting list of namespaces
   */
  @VisibleForTesting
  void cleanFilesWithoutMeta(final long tillTime, final Map<String, NamespaceId> namespacedLogBaseDirMap,
                             final SetMultimap<String, Location> parentDirs, final int maxDiskFilesScanned,
                             final long maxMetaEntriesScanned) throws Exception {
    LOG.info("Starting deletion of log files older than {} without metadata", tillTime);
    List<NamespaceMeta> namespaces = namespaceQueryAdmin.list();
    // For all the namespaces present in NamespaceMeta, gather log files from disk and log meta. Then delete the log
    // files for which metadata is not present
    for (NamespaceMeta namespaceMeta : namespaces) {
      try {
        final NamespaceId namespaceId = namespaceMeta.getNamespaceId();
        final Location namespacedLogBaseDir = LoggingContextHelper
          .getNamespacedBaseDirLocation(namespacedLocationFactory, logBaseDir, namespaceId, impersonator);

        if (namespacedLogBaseDir.exists()) {
          // Get all the disk log files on disk for a given namespace
          Set<Location> diskFileLocations = getDiskLocations(namespaceId, namespacedLogBaseDir, tillTime,
                                                             maxDiskFilesScanned);
          // remove log files from diskFileLocations for which metadata is present.
          filterLocationsWithMeta(namespaceId, diskFileLocations, maxMetaEntriesScanned);

          // delete all the disk locations for which metadata is not present
          for (final Location locationToDelete : diskFileLocations) {
            try {
              deleteLogFiles(parentDirs, namespacedLogBaseDirMap, namespaceId, locationToDelete);
            } catch (Exception e) {
              LOG.warn("Got exception processing log cleanup for file {}", locationToDelete, e);
            }
          }
        }
      } catch (Exception e) {
        LOG.warn("Got exception processing log cleanup for namespace {}", namespaceMeta.getName(), e);
      }
    }

    LOG.info("Finished deletion of log files older than {} without metadata", tillTime);
  }

  /**
   * Filter all the disk files from diskFileLocations for which meta data is present
   *
   * @param namespaceId           namespace for which metadata needs to be scanned
   * @param diskFileLocations     log files present on disk for given namespace
   * @param maxMetaEntriesScanned max number entries to be scanned in one iteration
   */
  @VisibleForTesting
  void filterLocationsWithMeta(NamespaceId namespaceId, final Set<Location> diskFileLocations,
                               final long maxMetaEntriesScanned) {
    // get the logging context for a given namespace
    LoggingContext loggingContext = new LogNamespaceLoggingContext(namespaceId.getNamespace());
    String logPartition = loggingContext.getLogPartition() + ":";

    // Create nextKey by using namespaceId as startKey and incremented namespaceId by one as stopKey
    FileMetaDataManager.TableKey nextTableKey = new FileMetaDataManager
      .TableKey(logPartition.getBytes(), Bytes.stopKeyForPrefix(logPartition.getBytes()), null);

    FileMetaDataManager.MetaEntryProcessor<Set<URI>> metaEntriesCollector =
      new FileMetaDataManager.MetaEntryProcessor<Set<URI>>() {
        Set<URI> scannedFiles = new HashSet<>();

        @Override
        public void process(FileMetaDataManager.ScannedEntryInfo scannedFileInfo) {
          scannedFiles.add(scannedFileInfo.getUri());
        }

        @Override
        public Set<URI> getCollectedEntries() {
          return scannedFiles;
        }
      };

    // Get all the log metadata in batches of maxMetaEntriesScanned
    do {
      nextTableKey = fileMetaDataManager.scanFiles(nextTableKey, maxMetaEntriesScanned, metaEntriesCollector);
      // create location from scanned uris and remove all the metadata files that has corresponding disk file available
      for (final URI uri : metaEntriesCollector.getCollectedEntries()) {
        try {
          Location fileLocation = impersonator.doAs(namespaceId, new Callable<Location>() {
            @Override
            public Location call() throws Exception {
              return rootLocationFactory.create(uri);
            }
          });

          if (diskFileLocations.contains(fileLocation)) {
            diskFileLocations.remove(fileLocation);
          }
        } catch (Exception e) {
          LOG.warn("Got exception while accessing path {}", uri.toString(), e);
        }
      }
    } while (nextTableKey != null);
  }

  /**
   * @param namespaceId          namespace for which metadata needs to be scanned
   * @param namespacedLogBaseDir namespaced log base dir without the root dir prefixed
   * @param tillTime             time till disk locations are scanned.
   * @param maxDiskFilesScanned  Max disk files scanned. If reached this limit, other files will be processed in next
   *                             cleanup run
   * @return set of locations on disk
   */
  private Set<Location> getDiskLocations(NamespaceId namespaceId, final Location namespacedLogBaseDir,
                                         final long tillTime, final int maxDiskFilesScanned) {
    // processor to get list of log files older than retention duration
    final Processor<LocationStatus, Set<Location>> diskFilesProcessor = getDiskFilesProcessor(namespaceId, tillTime,
                                                                                              maxDiskFilesScanned);
    try {
      impersonator.doAs(namespaceId, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Locations.processLocations(namespacedLogBaseDir, true, diskFilesProcessor);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.warn("Got exception while accessing path {}", namespacedLogBaseDir, e);
    }
    return diskFilesProcessor.getResult();
  }

  /**
   * Logging context for a namespace
   */
  private class LogNamespaceLoggingContext extends NamespaceLoggingContext {
    /**
     * Constructs NamespaceLoggingContext.
     *
     * @param namespaceId namespace id
     */
    private LogNamespaceLoggingContext(String namespaceId) {
      super(namespaceId);
    }
  }

  private Processor<LocationStatus, Set<Location>> getDiskFilesProcessor(final NamespaceId namespaceId,
                                                                         final long tillTime,
                                                                         final int maxDiskFilesScanned) {
    return new Processor<LocationStatus, Set<Location>>() {
      private Set<Location> locations = new HashSet<>();

      @Override
      public boolean process(final LocationStatus input) {
        try {
          Location location = impersonator.doAs(namespaceId, new Callable<Location>() {
            @Override
            public Location call() throws Exception {
              return rootLocationFactory.create(input.getUri());
            }
          });
          // For each namespace, only scan maxDiskFilesScanned disk files per clean up run
          if (!input.isDir() && location.lastModified() < tillTime && locations.size() < maxDiskFilesScanned) {
            locations.add(location);
          }

          // Stop processing when we reach maxDiskFilesScanned
          if (locations.size() >= maxDiskFilesScanned) {
            return false;
          }

        } catch (Exception e) {
          LOG.warn("Got exception in getting last modified location for log file {} during log clean up",
                   input.getUri().getPath(), e);
        }
        return true;
      }

      @Override
      public Set<Location> getResult() {
        return locations;
      }
    };
  }

  private void deleteLogFiles(final SetMultimap<String, Location> parentDirs,
                              final Map<String, NamespaceId> namespacedLogBaseDirMap,
                              NamespaceId namespaceId, final Location location) throws Exception {
      final Location namespacedLogBaseDir = LoggingContextHelper.getNamespacedBaseDirLocation(namespacedLocationFactory,
                                                                                              logBaseDir, namespaceId,
                                                                                              impersonator);
      impersonator.doAs(namespaceId, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          if (location.exists()) {
            LOG.info("Deleting log file {}", location);
            location.delete();
            parentDirs.put(namespacedLogBaseDir.toString(), getParent(location));
          }
          return null;
        }
      });
      namespacedLogBaseDirMap.put(namespacedLogBaseDir.toString(), namespaceId);
  }

  private Location getParent(Location location) {
    Location parent = Locations.getParent(location);
    return (parent == null) ? location : parent;
  }

  /**
   * For the specified directory to be deleted, finds its namespaced log location, then deletes
   *
   * @param namespacedLogBaseDir namespaced log base dir without the root dir prefixed
   * @param dir                  dir to delete
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
   *
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
