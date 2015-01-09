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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;


/**
 * Wrapper for a mapreduce {@link InputFormat}. This will wrap another input format and make sure classloading
 * is done correctly. This is required because these methods can be called by mapreduce tasks before our MapperWrapper
 * and ReducerWrapper classes get a chance to do anything.
 *
 * @param <KEY> Input key type.
 * @param <VALUE> Input value type.
 */
public class InputFormatWrapper<KEY, VALUE> extends InputFormat<KEY, VALUE> {
  private static final String INPUT_FORMAT_CLASS = "cdap.mapreduce.wrapped.input.format.class";

  public static void setInputFormatClass(Job job, String className) {
    job.getConfiguration().set(INPUT_FORMAT_CLASS, className);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    // this should be called when the job is submitted from the MapReduceRuntimeService
    return getInputFormat(context.getConfiguration(),
                          Thread.currentThread().getContextClassLoader()).getSplits(context);
  }

  @Override
  public RecordReader<KEY, VALUE> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    MapReduceContextProvider contextProvider = new MapReduceContextProvider(context, MapReduceMetrics.TaskType.Mapper);
    BasicMapReduceContext mrContext = contextProvider.get();

    ClassLoader programClassLoader = mrContext.getProgram().getClassLoader();
    return getInputFormat(context.getConfiguration(), programClassLoader).createRecordReader(split, context);
  }

  private InputFormat<KEY, VALUE> getInputFormat(Configuration conf, ClassLoader classLoader) {
    String className = conf.get(INPUT_FORMAT_CLASS);
    try {
      classLoader = classLoader == null ? Thread.currentThread().getContextClassLoader() : classLoader;
      return (InputFormat<KEY, VALUE>) classLoader.loadClass(className).newInstance();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
