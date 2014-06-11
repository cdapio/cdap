/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.write;

import com.continuuity.common.io.Locations;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
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
  private final Location logBaseDir;
  private final long retentionDurationMs;

  public LogCleanup(FileMetaDataManager fileMetaDataManager, Location logBaseDir, long retentionDurationMs) {
    this.fileMetaDataManager = fileMetaDataManager;
    this.logBaseDir = logBaseDir;
    this.retentionDurationMs = retentionDurationMs;

    LOG.info("Log base dir = {}", logBaseDir.toURI());
    LOG.info("Log retention duration = {} ms", retentionDurationMs);
  }

  @Override
  public void run() {
    LOG.info("Running log cleanup...");
    try {
      long tillTime = System.currentTimeMillis() - retentionDurationMs;
      final Set<Location> parentDirs = Sets.newHashSet();
      fileMetaDataManager.cleanMetaData(tillTime,
                                        new FileMetaDataManager.DeleteCallback() {
                                          @Override
                                          public void handle(Location location) {
                                            try {
                                              if (location.exists()) {
                                                LOG.info(String.format("Deleting log file %s", location.toURI()));
                                                location.delete();
                                              }
                                              parentDirs.add(getParent(location));
                                            } catch (IOException e) {
                                              LOG.error(
                                                String.format("Got exception when deleting path %s",
                                                              location.toURI()), e);
                                              throw Throwables.propagate(e);
                                            }
                                          }
                                        });

      // Delete any empty parent dirs
      for (Location dir : parentDirs) {
        deleteEmptyDir(dir);
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
   * Deletes dir if it is empty, and recursively deletes parent dirs if they are empty too. The recursion stops at
   * non-empty parent or base directory. If dir is not child of base directory then the recursion stops at root.
   * @param dir dir to be deleted.
   */
  void deleteEmptyDir(Location dir) {
    LOG.debug("Got path {}", dir.toURI());

    // Don't delete a dir if it is equal to or a parent of logBaseDir
    if (logBaseDir.toURI().equals(dir.toURI()) ||
      !dir.toURI().getRawPath().startsWith(logBaseDir.toURI().getRawPath())) {
      LOG.debug("{} not deletion candidate.", dir.toURI());
      return;
    }

    try {
      if (dir.list().isEmpty() && dir.delete()) {
        LOG.info("Deleted empty dir {}", dir.toURI());

        // See if parent dir is empty, and needs deleting
        Location parent = getParent(dir);
        LOG.debug("Deleting parent dir {}", parent);
        deleteEmptyDir(parent);
      } else {
        LOG.debug("Not deleting non-dir or non-empty dir {}", dir.toURI());
      }
    } catch (IOException e) {
      LOG.error("Got exception while deleting dir {}", dir.toURI(), e);
    }
  }
}
