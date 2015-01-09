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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.app.metrics.MapReduceMetrics;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Wrapper for a mapreduce {@link OutputFormat}. This will wrap another output format and make sure classloading
 * is done correctly. This is required because these methods can be called by mapreduce tasks before our MapperWrapper
 * and ReducerWrapper classes get a chance to do anything.
 *
 * @param <KEY> Output key type.
 * @param <VALUE> Output value type.
 */
public class OutputFormatWrapper<KEY, VALUE> extends OutputFormat<KEY, VALUE> {
  private static final String OUTPUT_FORMAT_CLASS = "cdap.mapreduce.wrapped.output.format.class";
  private OutputFormat<KEY, VALUE> outputFormat = null;

  public static void setOutputFormatClass(Job job, String className) {
    job.getConfiguration().set(OUTPUT_FORMAT_CLASS, className);
  }

  @Override
  public RecordWriter<KEY, VALUE> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    return getOutputFormat(context).getRecordWriter(context);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    // TODO: do something here
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return getOutputFormat(context).getOutputCommitter(context);
  }

  private OutputFormat<KEY, VALUE> getOutputFormat(TaskAttemptContext context) {
    if (outputFormat != null) {
      return outputFormat;
    }
    // we are assuming the wrapped format doesn't emit cdap metrics, otherwise the actual TaskType should be passed in.
    MapReduceContextProvider contextProvider = new MapReduceContextProvider(context, MapReduceMetrics.TaskType.Reducer);
    BasicMapReduceContext mrContext = contextProvider.get();
    ClassLoader programClassLoader = mrContext.getProgram().getClassLoader();

    String outputFormatClass = context.getConfiguration().get(OUTPUT_FORMAT_CLASS);
    try {
      return (OutputFormat<KEY, VALUE>) programClassLoader.loadClass(outputFormatClass).newInstance();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
