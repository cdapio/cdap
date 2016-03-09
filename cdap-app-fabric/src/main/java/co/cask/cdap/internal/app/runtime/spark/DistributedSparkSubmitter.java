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

package co.cask.cdap.internal.app.runtime.spark;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link SparkSubmitter} to submit Spark job that runs on cluster.
 */
public class DistributedSparkSubmitter extends AbstractSparkSubmitter {

  private final String schedulerQueueName;

  public DistributedSparkSubmitter(@Nullable String schedulerQueueName) {
    this.schedulerQueueName = schedulerQueueName;
  }

  @Override
  protected Map<String, String> getSubmitConf() {
    if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
      return Collections.singletonMap("spark.yarn.queue", schedulerQueueName);
    }
    return Collections.emptyMap();
  }

  @Override
  protected String getMaster() {
    return "yarn-client";
  }

  @Override
  protected void triggerShutdown(ExecutionSparkContext sparkContext) {
    sparkContext.close();
  }
}
