/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * An {@link InputFormat} that delegates to another {@link InputFormat}.
 *
 * @param <K> type of key to read
 * @param <V> type of value to read
 */
public abstract class DelegatingInputFormat<K, V> extends InputFormat<K, V> {

  /**
   * Returns the name of the config key to the delegating {@link InputFormat} class name configuration.
   */
  protected abstract String getDelegateClassNameKey();

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return getDelegate(context.getConfiguration()).getSplits(context);
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
                                               TaskAttemptContext context) throws IOException, InterruptedException {
    return getDelegate(context.getConfiguration()).createRecordReader(split, context);
  }

  /**
   * Returns the delegating {@link InputFormat} based on the current configuration.
   *
   * @param conf the Hadoop {@link Configuration} for this input format
   * @throws IOException if failed to instantiate the input format class
   */
  protected final InputFormat<K, V> getDelegate(Configuration conf) throws IOException {
    String delegateClassName = conf.get(getDelegateClassNameKey());
    if (delegateClassName == null) {
      throw new IllegalArgumentException("Missing configuration " + getDelegateClassNameKey()
                                           + " for the InputFormat to use");
    }
    if (delegateClassName.equals(getClass().getName())) {
      throw new IllegalArgumentException("Cannot delegate InputFormat to the same class " + delegateClassName);
    }
    try {
      //noinspection unchecked
      InputFormat<K, V> inputFormat = (InputFormat<K, V>) conf.getClassLoader().loadClass(delegateClassName)
        .newInstance();
      if (inputFormat instanceof Configurable) {
        ((Configurable) inputFormat).setConf(conf);
      }
      return inputFormat;
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new IOException("Unable to instantiate delegate input format " + delegateClassName, e);
    }
  }
}
