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

package co.cask.cdap.operations.yarn;

import co.cask.cdap.operations.OperationalStats;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.util.List;

/**
 * {@link OperationalStats} for reporting Yarn Queue Stats.
 */
public class YarnQueues extends AbstractYarnStats implements YarnQueuesMXBean {

  private int stopped;
  private int running;

  @SuppressWarnings("unused")
  public YarnQueues() {
    this(new Configuration());
  }

  @VisibleForTesting
  YarnQueues(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return "queues";
  }

  @Override
  public int getTotal() {
    return running + stopped;
  }

  @Override
  public int getStopped() {
    return stopped;
  }

  @Override
  public int getRunning() {
    return running;
  }

  @Override
  public synchronized void collect() throws Exception {
    reset();
    List<QueueInfo> queues;
    YarnClient yarnClient = createYARNClient();
    try {
      queues = yarnClient.getAllQueues();
    } finally {
      yarnClient.stop();
    }
    for (QueueInfo queue : queues) {
      switch (queue.getQueueState()) {
        case RUNNING:
          running++;
          break;
        case STOPPED:
          stopped++;
          break;
      }
    }
  }

  private void reset() {
    running = 0;
    stopped = 0;
  }
}
