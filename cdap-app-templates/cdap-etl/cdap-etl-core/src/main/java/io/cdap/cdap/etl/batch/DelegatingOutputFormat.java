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

import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An {@link OutputFormat} that delegates to another {@link OutputFormat}.
 *
 * @param <K> type of key to write
 * @param <V> type of value to write
 */
public class DelegatingOutputFormat<K, V> extends OutputFormat<K, V> {

  public static final String DELEGATE_CLASS_NAME = "io.cdap.pipeline.delegate.output.classname";

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return getDelegate(context.getConfiguration()).getRecordWriter(context);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    getDelegate(context.getConfiguration()).checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return getDelegate(context.getConfiguration()).getOutputCommitter(context);
  }

  /**
   * Returns the delegating {@link OutputFormat} based on the configuration.
   *
   * @param conf the Hadoop {@link Configuration} for this output format
   */
  protected final OutputFormat<K, V> getDelegate(Configuration conf) {
    String delegateClassName = conf.get(DELEGATE_CLASS_NAME);
    if (delegateClassName == null) {
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
        String.format("Missing configuration '%s' for the OutputFormat to use.",
          DELEGATE_CLASS_NAME), String.format("Please provide correct configuration for delegate "
          + "OutputFormat class key '%s'.", DELEGATE_CLASS_NAME), ErrorType.SYSTEM, false, null);
    }
    if (delegateClassName.equals(getClass().getName())) {
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
        String.format("Cannot delegate OutputFormat to the same class '%s'.", delegateClassName),
        String.format("Please provide correct configuration for delegate " 
          + "OutputFormat class name '%s'.", delegateClassName), ErrorType.SYSTEM, false, null);
    }
    try {
      //noinspection unchecked
      OutputFormat<K, V> outputFormat = (OutputFormat<K, V>) conf.getClassLoader()
          .loadClass(delegateClassName)
          .newInstance();
      if (outputFormat instanceof Configurable) {
        ((Configurable) outputFormat).setConf(conf);
      }
      return outputFormat;
    } catch (Exception e) {
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
        String.format("Unable to instantiate delegate output format class '%s'.",
          delegateClassName), e.getMessage(), ErrorType.SYSTEM, false, e);
    }
  }
}
