/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.examples.loganalysis;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * A MapReduce job which computes total number of hits for every unique url or path in apache access logs
 */
public class HitCounterProgram extends AbstractMapReduce {

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setMapperClass(Emitter.class);
    job.setReducerClass(Counter.class);

    context.addInput(Input.ofStream(LogAnalysisApp.LOG_STREAM));
    context.addOutput(Output.ofDataset(LogAnalysisApp.HIT_COUNT_STORE));
  }

  /**
   * A mapper that emits each url or path with a value of 1.
   */
  public static class Emitter extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    Text logText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      logText.set(ApacheAccessLog.parseFromLogLine(value.toString()).getEndpoint());
      context.write(logText, ONE);
    }
  }

  /**
   * A reducer that sums up the counts for each key.
   */
  public static class Counter extends Reducer<Text, IntWritable, byte[], byte[]> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      long sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(Bytes.toBytes(key.toString()), Bytes.toBytes(sum));
    }
  }
}

