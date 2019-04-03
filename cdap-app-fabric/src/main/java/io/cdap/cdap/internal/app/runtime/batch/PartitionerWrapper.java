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

package co.cask.cdap.internal.app.runtime.batch;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Wraps user-defined implementation of {@link Partitioner} class which allows to perform extra configuration.
 */
public class PartitionerWrapper extends Partitioner implements Configurable {

  private static final String ATTR_CLASS = "c.partitioner.class";

  private Partitioner delegate;
  private Configuration conf;

  /**
   * Wraps the partitioner defined in the job with this {@link PartitionerWrapper} if it is defined.
   * @param job The MapReduce job
   */
  public static void wrap(Job job) {
    if (WrapperUtil.setIfDefined(job, MRJobConfig.PARTITIONER_CLASS_ATTR, ATTR_CLASS)) {
      job.setPartitionerClass(PartitionerWrapper.class);
    }
  }

  @Override
  public int getPartition(Object o, Object o2, int numPartitions) {
    return delegate.getPartition(o, o2, numPartitions);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.delegate = WrapperUtil.createDelegate(conf, ATTR_CLASS);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
