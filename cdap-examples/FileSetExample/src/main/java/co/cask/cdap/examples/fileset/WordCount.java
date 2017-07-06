/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

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
    setMapperResources(new Resources(1024));
  }

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    Job job = context.getHadoopJob();
    job.setMapperClass(Tokenizer.class);
    job.setNumReduceTasks(1);

    String inputDataset = context.getRuntimeArguments().get("input");
    inputDataset = inputDataset != null ? inputDataset : "lines";

    String outputDataset = context.getRuntimeArguments().get("output");
    outputDataset = outputDataset != null ? outputDataset : "counts";

    context.addInput(Input.ofDataset(inputDataset));
    context.addOutput(Output.ofDataset(outputDataset));
  }

  /**
   * A mapper that tokenizes each input line and emits each token with a value of 1.
   */
  public static class Tokenizer extends Mapper<Void, StructuredRecord, IntWritable, Text> {
    @Override
    public void map(Void key, StructuredRecord data, Context context) throws IOException, InterruptedException {
      int count = 1;
      for (Schema.Field field : data.getSchema().getFields()) {
        context.write(new IntWritable(count), new Text((String) data.get(field.getName())));
        count++;
      }
    }
  }
}
