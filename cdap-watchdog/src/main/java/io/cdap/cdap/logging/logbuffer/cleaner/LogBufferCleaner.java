/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.logbuffer.cleaner;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.logbuffer.LogBufferFileOffset;
import io.cdap.cdap.logging.meta.CheckpointManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runnable to clean up log buffer files for which logs are already persisted.
 */
public class LogBufferCleaner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogBufferCleaner.class);
  private final List<CheckpointManager<LogBufferFileOffset>> checkpointManagers;
  private final File baseLogDir;
  private final AtomicBoolean shouldCleanup;

  public LogBufferCleaner(CConfiguration cConf, List<CheckpointManager<LogBufferFileOffset>> checkpointManagers,
                          AtomicBoolean shouldCleanup) {
    this(checkpointManagers, cConf.get(Constants.LogBuffer.LOG_BUFFER_BASE_DIR), shouldCleanup);
  }

  @VisibleForTesting
  LogBufferCleaner(List<CheckpointManager<LogBufferFileOffset>> checkpointManagers, String baseLogDir,
                   AtomicBoolean shouldCleanup) {
    this.checkpointManagers = checkpointManagers;
    this.baseLogDir = new File(baseLogDir);
    this.shouldCleanup = shouldCleanup;
  }

  @Override
  public void run() {
    if (!shouldCleanup.get() || !baseLogDir.exists()) {
      LOG.debug("Log buffer base directory {} does not exist or recovery is still running. " +
                  "So cleanup task will not run.", baseLogDir);
      return;
    }

    try {
      // From all the pipelines, get the largest file id for which logs have been persisted. So the cleaner can delete
      // files with file id smaller than that file id.
      long largestFileId = getLargestFileIdToDelete(checkpointManagers);

      // scan files under baseLogDir and delete file which has fileId smaller than largestFileId
      scanAndDelete(baseLogDir, largestFileId);
    } catch (Exception e) {
      LOG.warn("Exception while cleaning up log buffer.", e);
    }
  }

  /**
   * Scans base log directory and deletes file which has fileId smaller than or equal to largestFileId.
   */
  private void scanAndDelete(File baseLogDir, long largestFileId) {
    // if the offsets are not persisted yet, then nothing needs to be done.
    if (largestFileId == -1) {
      return;
    }

    File[] files = baseLogDir.listFiles();
    if (files != null) {
      int index = 0;
      while (index < files.length) {
        File file = files[index++];
        if (getFileId(file.getName()) <= largestFileId) {
          // if the file is not deleted, in the next scan it should get deleted.
          file.delete();
        }
      }
    }
  }

  /**
   * Get largest fileId for which logs have been persisted.
   */
  private long getLargestFileIdToDelete(List<CheckpointManager<LogBufferFileOffset>> checkpointManagers)
    throws IOException {
    // there will be atleast one log pipeline
    LogBufferFileOffset minOffset = checkpointManagers.get(0).getCheckpoint(0).getOffset();

    for (int i = 1; i < checkpointManagers.size(); i++) {
      LogBufferFileOffset offset = checkpointManagers.get(i).getCheckpoint(0).getOffset();
      // keep track of minimum offset of all the pipeline
      minOffset = minOffset.compareTo(offset) > 0 ? offset : minOffset;
    }

    return minOffset.getFileId() - 1;
  }

  /**
   * Given the file name, returns the file id.
   */
  private long getFileId(String fileName) {
    String[] splitted = fileName.split("\\.");
    return Long.parseLong(splitted[0]);
  }
}
