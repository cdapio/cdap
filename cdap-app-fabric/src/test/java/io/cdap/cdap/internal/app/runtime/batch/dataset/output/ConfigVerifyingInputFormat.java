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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Noop inputformat that generates mock input splits and verifies if correct configuration values are available in
 * job and task context.
 */
public class ConfigVerifyingInputFormat extends InputFormat<LongWritable, Text> {

  /**
   * Get noop splits
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    String config = jobContext.getConfiguration().get(AppWithSingleInputOutput.ADDITIONAL_CONFIG);
    // CDAP-14531 validate the correct conf values are present in the context for the input splits
    if (!config.equals(AppWithSingleInputOutput.SOURCE_CONFIG)) {
      throw new RuntimeException(String.format("Configuration value for key: %s should be %s, instead it was %s",
                                               AppWithSingleInputOutput.ADDITIONAL_CONFIG,
                                               AppWithSingleInputOutput.SOURCE_CONFIG, config));
    }

    List<InputSplit> splits = new ArrayList<>();
    for (int i = 0; i < 1; i++) {
      splits.add(new MockInputSplit());
    }
    return splits;
  }

  /**
   * Noop record reader
   */
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(
    InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    String config = context.getConfiguration().get(AppWithSingleInputOutput.ADDITIONAL_CONFIG);
    // CDAP-14531 validate the correct conf values are present in the context for the input
    if (!config.equals(AppWithSingleInputOutput.SOURCE_CONFIG)) {
      throw new RuntimeException(String.format("Configuration value for key: %s should be %s, instead it was %s",
                                               AppWithSingleInputOutput.ADDITIONAL_CONFIG,
                                               AppWithSingleInputOutput.SOURCE_CONFIG, config));
    }

    return new RecordReader<LongWritable, Text>() {
      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // no-op
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
      }

      @Override
      public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable();
      }

      @Override
      public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return 0;
      }

      @Override
      public void close() throws IOException {
        // no-op
      }
    };
  }


  public static class MockInputSplit extends InputSplit implements Writable {

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 0L;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
      // no-op
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
      // no-op
    }
  }
}
