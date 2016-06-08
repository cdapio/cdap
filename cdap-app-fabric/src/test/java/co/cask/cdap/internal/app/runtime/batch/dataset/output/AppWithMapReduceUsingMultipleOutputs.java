/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataset.output;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * App used to test whether M/R can write to multiple outputs. Tests writing to the same dataset, with different runtime
 * arguments, as two different outputs.
 */
public class AppWithMapReduceUsingMultipleOutputs extends AbstractApplication {

  public static final String PURCHASES = "purchases";
  public static final String SEPARATED_PURCHASES = "smallPurchases";

  @Override
  public void configure() {
    setName("AppWithMapReduceUsingMultipleOutputs");
    setDescription("Application with MapReduce job using multiple outputs");
    createDataset(PURCHASES, "fileSet", FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .build());
    createDataset(SEPARATED_PURCHASES, "fileSet", FileSetProperties.builder()
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, " ")
      .build());
    addMapReduce(new SeparatePurchases());
    addMapReduce(new InvalidMapReduce());
  }

  /**
   * Simple map-only MR that simply writes to a different output, depending on the spend amount.
   */
  public static class SeparatePurchases extends AbstractMapReduce {

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Map<String, String> inputArgs = new HashMap<>();
      FileSetArguments.setInputPath(inputArgs, "inputFile");

      // test using a stream with the same name, but aliasing it differently (so mapper gets the alias'd name)
      context.addInput(Input.ofDataset(PURCHASES, inputArgs), FileMapper.class);

      Map<String, String> output1Args = new HashMap<>();
      FileSetArguments.setOutputPath(output1Args, "small_purchases");
      context.addOutput(Output.ofDataset(SEPARATED_PURCHASES, output1Args).alias("small_purchases"));

      Map<String, String> output2Args = new HashMap<>();
      FileSetArguments.setOutputPath(output2Args, "large_purchases");
      context.addOutput(Output.ofDataset(SEPARATED_PURCHASES, output2Args).alias("large_purchases"));

      Job job = context.getHadoopJob();
      job.setMapperClass(FileMapper.class);
      job.setNumReduceTasks(0);
    }
  }

  /**
   * This is an invalid MR because it adds an output a second time, with the same alias.
   */
  public static class InvalidMapReduce extends SeparatePurchases {
    @Override
    public void initialize() throws Exception {
      super.initialize();
      getContext().addOutput(Output.ofDataset(SEPARATED_PURCHASES).alias("small_purchases"));
    }
  }

  public static class FileMapper extends Mapper<LongWritable, Text, LongWritable, Text>
    implements ProgramLifecycle<MapReduceTaskContext<NullWritable, Text>> {

    private MapReduceTaskContext<NullWritable, Text> mapReduceTaskContext;

    @Override
    public void initialize(MapReduceTaskContext<NullWritable, Text> context) throws Exception {
      this.mapReduceTaskContext = context;
    }

    @Override
    public void map(LongWritable key, Text data, Context context) throws IOException, InterruptedException {
      String spend = data.toString().split(" ")[1];
      String output = Integer.valueOf(spend) > 50 ? "large_purchases" : "small_purchases";
      mapReduceTaskContext.write(output, NullWritable.get(), data);
    }

    @Override
    public void destroy() {
    }
  }

}
