/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.spark.inmemory.InMemorySparkContextBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;

/**
 * Provides access to SparkContext for Spark job tasks
 */
public class SparkContextProvider {

  private final Configuration hConf;
  private final SparkContextConfig contextConfig;
  private BasicSparkContext context;
  private AbstractSparkContextBuilder contextBuilder;

  public SparkContextProvider(Configuration hConf) {
    this.hConf = hConf;
    this.contextConfig = new SparkContextConfig(hConf);
    this.contextBuilder = null;
  }

  /**
   * Creates an instance of {@link BasicSparkContext} that the {@link co.cask.cdap.app.program.Program} contained
   * inside cannot load program classes. It is used for the cases where only the application specification is needed,
   * but no need to load any class from it.
   */
  public synchronized BasicSparkContext get() {
    if (context == null) {
      CConfiguration conf = contextConfig.getConf();
      context = getBuilder(conf)
        .build(contextConfig.getRunId(),
               contextConfig.getLogicalStartTime(),
               contextConfig.getWorkflowBatch(),
               contextConfig.getArguments(),
               contextConfig.getTx(),
               hConf.getClassLoader(),
               contextConfig.getProgramLocation()
        );
    }
    return context;
  }

  private synchronized AbstractSparkContextBuilder getBuilder(CConfiguration conf) {
    if (contextBuilder == null) {
      String mrFramework = hConf.get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
      if ("local".equals(mrFramework)) {
        contextBuilder = new InMemorySparkContextBuilder(conf);
      } else {
        throw new RuntimeException("Spark does not run in distributed mode right now");
      }
    }
    return contextBuilder;
  }
}
