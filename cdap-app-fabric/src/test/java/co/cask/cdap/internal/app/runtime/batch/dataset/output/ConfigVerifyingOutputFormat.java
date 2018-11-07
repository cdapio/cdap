/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.internal.app.runtime.batch.dataset.output;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Noop output format to test correct configurations are available in task context
 */
public class ConfigVerifyingOutputFormat extends OutputFormat<LongWritable, Text> {

  /**
   * Noop record writer
   */
  @Override
  public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext context) throws IOException {
    String config = context.getConfiguration().get(AppWithSingleInputOutput.ADDITIONAL_CONFIG);
    // CDAP-14531 validate the correct conf values are present in the context for the output
    if (!config.equals(AppWithSingleInputOutput.SINK_CONFIG)) {
      throw new RuntimeException(String.format("Configuration value for key: %s should be %s, instead it was %s",
                                               AppWithSingleInputOutput.ADDITIONAL_CONFIG,
                                               AppWithSingleInputOutput.SINK_CONFIG, config));
    }

    return new RecordWriter<LongWritable, Text>() {
      @Override
      public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        // no-op
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // no-op
      }
    };
  }

  /**
   * Noop spec check
   */
  @Override
  public void checkOutputSpecs(JobContext jobContext) {
  }

  /**
   * Noop output committer
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    String config = context.getConfiguration().get(AppWithSingleInputOutput.ADDITIONAL_CONFIG);
    // CDAP-14531 validate the correct conf values are present in the context for the output committer
    if (!config.equals(AppWithSingleInputOutput.SINK_CONFIG)) {
      throw new RuntimeException(String.format("Configuration value for key: %s should be %s, instead it was %s",
                                               AppWithSingleInputOutput.ADDITIONAL_CONFIG,
                                               AppWithSingleInputOutput.SINK_CONFIG, config));
    }

    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {

      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {

      }
    };
  }
}
