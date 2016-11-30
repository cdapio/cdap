/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations.hdfs;

import co.cask.cdap.operations.OperationalStats;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * {@link OperationalStats} for HDFS.
 */
@SuppressWarnings("unused")
public class HDFSStorage extends AbstractHDFSStats implements HDFSStorageMXBean {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSStorage.class);

  @VisibleForTesting
  static final String STAT_TYPE = "storage";

  private long totalBytes;
  private long usedBytes;
  private long availableBytes;
  private long missingBlocks;
  private long underReplicatedBlocks;
  private long corruptBlocks;

  public HDFSStorage() {
    this(new Configuration());
  }

  @VisibleForTesting
  HDFSStorage(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return STAT_TYPE;
  }

  @Override
  public long getTotalBytes() {
    return totalBytes;
  }

  @Override
  public long getUsedBytes() {
    return usedBytes;
  }

  @Override
  public long getRemainingBytes() {
    return availableBytes;
  }

  @Override
  public long getMissingBlocks() {
    return missingBlocks;
  }

  @Override
  public long getUnderReplicatedBlocks() {
    return underReplicatedBlocks;
  }

  @Override
  public long getCorruptBlocks() {
    return corruptBlocks;
  }

  @Override
  public synchronized void collect() throws IOException {
    try (DistributedFileSystem dfs = createDFS()) {
      if (dfs == null) {
        return;
      }
      FsStatus status = dfs.getStatus();
      this.totalBytes = status.getCapacity();
      this.availableBytes = status.getRemaining();
      this.usedBytes = status.getUsed();
      this.missingBlocks = dfs.getMissingBlocksCount();
      this.underReplicatedBlocks = dfs.getUnderReplicatedBlocksCount();
      this.corruptBlocks = dfs.getCorruptBlocksCount();
    }
  }

  @Nullable
  private DistributedFileSystem createDFS() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      LOG.debug("The filesystem configured on this cluster is {}, which is not {}. HDFS storage stats will not be " +
                  "reported.", fs.getClass().getName(), DistributedFileSystem.class.getName());
      return null;
    }
    return (DistributedFileSystem) fs;
  }
}
