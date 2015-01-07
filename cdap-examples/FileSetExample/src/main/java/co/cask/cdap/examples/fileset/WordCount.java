/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.fileset;

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
 * A simple word counter. It reads inputs from the "lines" FileSet and writes its output to
 * the "counts" FileSet. The input and output path can be configured as runtime arguments:
 * <ul>
 * <li>"dataset.lines.input.paths" for the input. Multiple paths can be given, separated by commas.</li>
 * <li>"dataset.counts.output.path" for the output.</li>
 * </ul>
 */
public class WordCount extends AbstractMapReduce {

  @Override
  public void configure() {
    setInputDataset("lines");
    setOutputDataset("counts");
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setMapperClass(Tokenizer.class);
    job.setReducerClass(Counter.class);
    job.setNumReduceTasks(1);
  }

  /**
   * A mapper that tokenizes each input line and emits each token with a value of 1.
   */
  public static class Tokenizer extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text data, Context context)
      throws IOException, InterruptedException {
      for (String token : data.toString().split(" ")) {
        word.set(token);
        context.write(word, ONE);
      }
    }
  }

  /**
   * A reducer that sums up the counts for each key.
   */
  public static class Counter extends Reducer<Text, IntWritable, String, Long> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      long sum = 0L;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key.toString(), sum);
    }
  }
}
