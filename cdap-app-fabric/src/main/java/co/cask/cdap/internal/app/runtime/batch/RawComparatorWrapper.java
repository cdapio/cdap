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
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * Wraps user-defined implementation of {@link RawComparator} class which allows to perform extra configuration,
 * such as initialization with MapReduceTaskContext, if the delegate class implements ProgramLifeCycle.
 */
abstract class RawComparatorWrapper implements RawComparator, Configurable {

  private RawComparator delegate;
  private Configuration conf;

  abstract String getDelegateClassAttr();

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.delegate = WrapperUtil.createDelegate(conf, getDelegateClassAttr());
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return delegate.compare(b1, s1, l1, b2, s2, l2);
  }

  @Override
  public int compare(Object o1, Object o2) {
    return delegate.compare(o1, o2);
  }

  static final class CombinerGroupComparatorWrapper extends RawComparatorWrapper {
    private static final String ATTR_CLASS = "c.combiner.group.comparator.class";

    /**
     * Wraps the combiner group comparator defined in the job with this {@link CombinerGroupComparatorWrapper} if it is
     * defined.
     * @param job The MapReduce job
     */
    static void wrap(Job job) {
      if (WrapperUtil.setIfDefined(job, MRJobConfig.COMBINER_GROUP_COMPARATOR_CLASS, ATTR_CLASS)) {
        job.setCombinerKeyGroupingComparatorClass(CombinerGroupComparatorWrapper.class);
      }
    }

    @Override
    String getDelegateClassAttr() {
      return ATTR_CLASS;
    }
  }

  static final class GroupComparatorWrapper extends RawComparatorWrapper {
    private static final String ATTR_CLASS = "c.group.comparator.class";

    /**
     * Wraps the group comparator defined in the job with this {@link GroupComparatorWrapper} if it is defined.
     * @param job The MapReduce job
     */
    static void wrap(Job job) {
      if (WrapperUtil.setIfDefined(job, MRJobConfig.GROUP_COMPARATOR_CLASS, ATTR_CLASS)) {
        job.setGroupingComparatorClass(GroupComparatorWrapper.class);
      }
    }

    @Override
    String getDelegateClassAttr() {
      return ATTR_CLASS;
    }
  }

  static final class KeyComparatorWrapper extends RawComparatorWrapper {
    private static final String ATTR_CLASS = "c.key.comparator.class";

    /**
     * Wraps the key comparator defined in the job with this {@link KeyComparatorWrapper} if it is defined.
     * @param job The MapReduce job
     */
    static void wrap(Job job) {
      if (WrapperUtil.setIfDefined(job, MRJobConfig.KEY_COMPARATOR, ATTR_CLASS)) {
        job.setSortComparatorClass(KeyComparatorWrapper.class);
      }
    }

    @Override
    String getDelegateClassAttr() {
      return ATTR_CLASS;
    }
  }
}
