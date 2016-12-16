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
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.util.List;

/**
 * {@link OperationalStats} for collecting Yarn app statistics.
 */
public class YarnApps extends AbstractYarnStats implements OperationalStats, YarnAppsMXBean {

  private int newApps;
  private int accepted;
  private int submitted;
  private int running;
  private int finished;
  private int failed;
  private int killed;
  private int total;

  @SuppressWarnings("unused")
  public YarnApps() {
    this(new Configuration());
  }

  @VisibleForTesting
  YarnApps(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return "apps";
  }

  @Override
  public int getNew() {
    return newApps;
  }

  @Override
  public int getTotal() {
    return total;
  }

  @Override
  public int getSubmitted() {
    return submitted;
  }

  @Override
  public int getAccepted() {
    return accepted;
  }

  @Override
  public int getRunning() {
    return running;
  }

  @Override
  public int getFinished() {
    return finished;
  }

  @Override
  public int getFailed() {
    return failed;
  }

  @Override
  public int getKilled() {
    return killed;
  }

  @Override
  public synchronized void collect() throws Exception {
    reset();
    YarnClient yarnClient = createYARNClient();
    List<ApplicationReport> applications;
    try {
      applications = yarnClient.getApplications();
    } finally {
      yarnClient.stop();
    }
    for (ApplicationReport application : applications) {
      switch (application.getYarnApplicationState()) {
        case NEW:
        case NEW_SAVING:
          newApps++;
          break;
        case ACCEPTED:
          accepted++;
          break;
        case SUBMITTED:
          submitted++;
          break;
        case RUNNING:
          running++;
          break;
        case FINISHED:
          finished++;
          break;
        case FAILED:
          failed++;
          break;
        case KILLED:
          killed++;
          break;
      }
    }
    total = applications.size();
  }

  private void reset() {
    newApps = 0;
    accepted = 0;
    submitted = 0;
    running = 0;
    finished = 0;
    failed = 0;
    killed = 0;
    total = 0;
  }
}
