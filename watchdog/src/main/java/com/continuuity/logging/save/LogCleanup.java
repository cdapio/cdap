/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.save;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

/**
 * Handles log file retention.
 */
public final class LogCleanup implements Runnable, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LogCleanup.class);

  private final FileSystem fileSystem;
  private final FileMetaDataManager fileMetaDataManager;
  private final Path logBaseDir;
  private final long retentionDurationMs;

  public LogCleanup(FileSystem fileSystem, FileMetaDataManager fileMetaDataManager, Path logBaseDir,
             long retentionDurationMs) {
    this.fileSystem = fileSystem;
    this.fileMetaDataManager = fileMetaDataManager;
    this.logBaseDir = logBaseDir;
    this.retentionDurationMs = retentionDurationMs;
  }

  @Override
  public void run() {
    try {
      long tillTime = System.currentTimeMillis() - retentionDurationMs;
      final Set<Path> parentDirs = Sets.newHashSet();
      fileMetaDataManager.cleanMetaData(tillTime,
                                        new FileMetaDataManager.DeleteCallback() {
                                          @Override
                                          public void handle(Path path) {
                                            try {
                                              if (fileSystem.exists(path)) {
                                                LOG.info(String.format("Deleting log file %s", path.toUri()));
                                                fileSystem.delete(path, false);
                                              }

                                              parentDirs.add(path.getParent());
                                            } catch (IOException e) {
                                              LOG.error(
                                                String.format("Got exception when deleting path %s", path), e);
                                              throw Throwables.propagate(e);
                                            }
                                          }
                                        });

      // Delete any empty parent dirs
      for (Path dir : parentDirs) {
        deleteEmptyDir(dir);
      }

    } catch (Throwable e){
      LOG.error("Got exception when cleaning up. Will try again later.", e);
    }
  }

  /**
   * Deletes dir if it is empty, and recursively deletes parent dirs if they are empty too. The recursion stops at
   * non-empty parent or base directory. If dir is not child of base directory then the recursion stops at root.
   * @param dir dir to be deleted.
   */
  void deleteEmptyDir(Path dir) {
    try {
      // Don't delete a dir if it is equal to or a parent of logBaseDir
      if (dir.equals(logBaseDir) || dir.isRoot() || dir.depth() <= logBaseDir.depth()) {
        return;
      }

      if (!fileSystem.isDirectory(dir)) {
        LOG.warn(String.format("Expected dir for cleanup but got non-dir %s", dir.toUri()));
        return;
      }

      RemoteIterator<LocatedFileStatus> files = fileSystem.listLocatedStatus(dir);
      if (!files.hasNext()) {
        // Directory is empty, delete it
        LOG.info(String.format("Deleting empty dir %s", dir.toUri()));
        fileSystem.delete(dir, false);

        // See if parent dir is empty, and needs deleting
        deleteEmptyDir(dir.getParent());
      }
    } catch (IOException e) {
      LOG.error(String.format("Got exception when trying to delete dir %s", dir));
    }
  }

  @Override
  public void close() throws IOException {
    fileSystem.close();
  }
}
