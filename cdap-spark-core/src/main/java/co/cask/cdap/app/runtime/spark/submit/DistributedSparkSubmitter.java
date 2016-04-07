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

package co.cask.cdap.app.runtime.spark.submit;

import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link SparkSubmitter} to submit Spark job that runs on cluster.
 */
public class DistributedSparkSubmitter extends AbstractSparkSubmitter {

  private final String schedulerQueueName;
  private final Configuration hConf;

  public DistributedSparkSubmitter(Configuration hConf, @Nullable String schedulerQueueName) {
    this.hConf = hConf;
    this.schedulerQueueName = schedulerQueueName;
  }

  @Override
  protected Map<String, String> getSubmitConf() {
    Map<String, String> submitConf = new HashMap<>();

    // Copy all hadoop configurations to the submission conf, prefix with "spark.hadoop.". This is
    // how Spark YARN client get hold of Hadoop configurations if those configurations are not in classpath,
    // which is true in CM cluster due to private hadoop conf directory (SPARK-13441) and YARN-4727
    for (Map.Entry<String, String> entry : hConf) {
      submitConf.put("spark.hadoop." + entry.getKey(), hConf.get(entry.getKey()));
    }

    if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
      submitConf.put("spark.yarn.queue", schedulerQueueName);
    }
    return submitConf;
  }

  @Override
  protected String getMaster(Map<String, String> configs) {
    return "yarn-client";
  }
}
