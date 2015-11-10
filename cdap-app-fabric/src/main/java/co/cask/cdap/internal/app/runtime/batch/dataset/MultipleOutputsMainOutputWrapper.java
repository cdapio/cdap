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

package co.cask.cdap.internal.app.runtime.batch.dataset;

import co.cask.cdap.common.lang.InstantiatorFactory;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * OutputFormat that wraps a root OutputFormat and provides an OutputFormatCommitter that delegates to multiple
 * preconfigured OutputFormatCommitters.
 *
 * @param <K> Type of key
 * @param <V> Type of value
 */
public class MultipleOutputsMainOutputWrapper<K, V> extends OutputFormat<K, V> {

  private static final String ROOT_OUTPUT_FORMAT =
    "co.cask.cdap.internal.app.runtime.batch.dataset.MultipleOutputsMainOutputWrapper.rootOutputFormat";
  private OutputFormat<K, V> innerFormat;
  private OutputCommitter committer;

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    OutputFormat<K, V> rootOutputFormat = getRootOutputFormat(job);
    return rootOutputFormat.getRecordWriter(job);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    for (String name : MultipleOutputs.getNamedOutputsList(context)) {
      Class<? extends OutputFormat> namedOutputFormatClass =
        MultipleOutputs.getNamedOutputFormatClass(context, name);

      OutputFormat outputFormat = new InstantiatorFactory(false).get(TypeToken.of(namedOutputFormatClass)).create();

      JobContext namedContext = MultipleOutputs.getNamedJobContext(context, name);
      outputFormat.checkOutputSpecs(namedContext);
    }
  }

  /**
   * Sets an OutputFormat class as the root OutputFormat for the Hadoop job.
   *
   * @param job the job on which to set the OutputFormat class
   * @param outputFormatClass the class to set as the root OutputFormat for the job
   * @param outputConfig the configuration to set for the specified OutputFormat
   */
  public static void setRootOutputFormat(Job job,  String outputFormatClass, Map<String, String> outputConfig) {
    job.getConfiguration().set(ROOT_OUTPUT_FORMAT, outputFormatClass);

    for (Map.Entry<String, String> confEntry : outputConfig.entrySet()) {
      job.getConfiguration().set(confEntry.getKey(), confEntry.getValue());
    }
  }

  // the root OutputFormat is used only for writing, not for checking output specs or committing of the output
  // because the root is also in the delegates, which check the output spec and commit the output.
  private OutputFormat<K, V> getRootOutputFormat(JobContext context) {
    if (innerFormat == null) {
      Configuration conf = context.getConfiguration();
      @SuppressWarnings("unchecked")
      Class<OutputFormat<K, V>> c = (Class<OutputFormat<K, V>>) conf.getClass(ROOT_OUTPUT_FORMAT,
                                                                              FileOutputFormat.class);
      try {
        innerFormat = c.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return innerFormat;
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    // return a MultipleOutputsCommitter that commits for the root output format as well as all delegate outputformats
    if (committer == null) {
      Map<String, OutputCommitter> committers = new HashMap<>();
      for (String name : MultipleOutputs.getNamedOutputsList(context)) {
        Class<? extends OutputFormat> namedOutputFormatClass =
          MultipleOutputs.getNamedOutputFormatClass(context, name);

        TaskAttemptContext namedContext = MultipleOutputs.getNamedTaskContext(context, name);

        OutputFormat outputFormat = new InstantiatorFactory(false).get(TypeToken.of(namedOutputFormatClass)).create();
        committers.put(name, outputFormat.getOutputCommitter(namedContext));
      }
      committer = new MultipleOutputsCommitter(committers);
    }

    return committer;
  }
}
