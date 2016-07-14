/*
 * Copyright © 2015 Cask Data, Inc.
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


package co.cask.cdap.logging.save;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.Map;

/**
 * Writes checkpoints of kafka partitions and offsets.
 */
public class CheckPointWriter implements Flushable, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(CheckPointWriter.class);

  private final Map<Integer, Checkpoint> partitionCheckpoints;
  private final CheckpointManager checkpointManager;

  public CheckPointWriter(CheckpointManager checkpointManager, Map<Integer, Checkpoint> partitionCheckpoints) {
    this.partitionCheckpoints = partitionCheckpoints;
    this.checkpointManager = checkpointManager;
  }

  public void updateCheckPoint(Integer partition, Checkpoint checkpoint) {
    partitionCheckpoints.put(partition, checkpoint);
  }

  @Override
  public void run() {
    try {
      checkpoint();
    } catch (Exception e) {
      LOG.error("Got exception while check-pointing: ", e);
    }
  }

  @Override
  public void flush() throws IOException {
    checkpoint();
  }

  private void checkpoint() {
    try {
      checkpointManager.saveCheckpoint(partitionCheckpoints);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
