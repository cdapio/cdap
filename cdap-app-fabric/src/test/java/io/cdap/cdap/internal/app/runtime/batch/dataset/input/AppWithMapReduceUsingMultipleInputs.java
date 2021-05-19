/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch.dataset.input;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputContext;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.PartitionedFileSetInputContext;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.mapreduce.MapReduceTaskContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * App used to test whether M/R can read and perform a join across multiple inputs.
 */
public class AppWithMapReduceUsingMultipleInputs extends AbstractApplication {

  public static final String PURCHASES = "purchases";
  public static final String PURCHASES2 = "purchases2";
  public static final String CUSTOMERS = "customers";
  public static final String OUTPUT_DATASET = "saturatedRecords";

  @Override
  public void configure() {
    setName("AppWithMapReduceUsingMultipleInputs");
    setDescription("Application with MapReduce job using multiple inputs");
    createDataset(PURCHASES, "fileSet", FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .build());
    createDataset(PURCHASES2, "fileSet", FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .build());
    createDataset(CUSTOMERS, "fileSet", FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .build());
    createDataset(OUTPUT_DATASET, "fileSet", FileSetProperties.builder()
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, " ")
      .build());
    addMapReduce(new ComputeSum());
    addMapReduce(new InvalidMapReduce());
  }

  /**
   * Computes sum of a customer's spending, while also joining on a lookup table, to get more data.
   */
  public static class ComputeSum extends AbstractMapReduce {

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Map<String, String> inputArgs = new HashMap<>();
      FileSetArguments.setInputPath(inputArgs, "inputFile");

      context.addInput(Input.ofDataset(PURCHASES, inputArgs), FileMapper.class);
      // A second input, aliasing so mapper gets the alias'd name
      context.addInput(Input.ofDataset(PURCHASES2, inputArgs).alias("secondPurchases"), FileMapper2.class);
      // since we set a Mapper class on the job itself, omitting the mapper in the addInput call will default to that
      context.addInput(Input.ofDataset(CUSTOMERS, inputArgs));

      Map<String, String> outputArgs = new HashMap<>();
      FileSetArguments.setOutputPath(outputArgs, "output");
      context.addOutput(Output.ofDataset(OUTPUT_DATASET, outputArgs));

      Job job = context.getHadoopJob();
      job.setMapperClass(FileMapper.class);
      job.setReducerClass(FileReducer.class);
    }
  }

  /**
   * This is an invalid MR because it adds an input a second time, with the same alias.
   */
  public static final class InvalidMapReduce extends ComputeSum {
    @Override
    public void initialize() {
      getContext().addInput(Input.ofDataset(PURCHASES, ImmutableMap.of("key", "value")));
    }
  }

  public static class FileMapper extends Mapper<LongWritable, Text, LongWritable, Text>
    implements ProgramLifecycle<MapReduceTaskContext> {

    protected String source;

    @Override
    public void initialize(MapReduceTaskContext context) throws Exception {
      System.setProperty("mapper.initialized", "true");
      source = context.getInputContext().getInputName();

      InputContext inputContext = context.getInputContext();
      // just want to assert that we're not getting any unexpected type of InputContext
      Preconditions.checkArgument(!(inputContext instanceof PartitionedFileSetInputContext));
      Preconditions.checkNotNull(source);
      validateSource(source);
    }

    protected void validateSource(String source) {
      Preconditions.checkArgument(PURCHASES.equals(source) || CUSTOMERS.equals(source));
    }

    @Override
    public void destroy() {
      System.setProperty("mapper.destroyed", "true");
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      // assert that the user gets FileInputSplit (as opposed to the MultiInputTaggedSplit) from the context
      Preconditions.checkArgument(context.getInputSplit() instanceof FileSplit);
      try {
        // assert that the user gets the TextInputFormat, as opposed to the MultiInputFormat from the context
        Preconditions.checkArgument(context.getInputFormatClass() == TextInputFormat.class);
      } catch (ClassNotFoundException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void map(LongWritable key, Text data, Context context) throws IOException, InterruptedException {
      String[] split = data.toString().split(" ");
      // tag each record with the source, so the reducer is simpler
      context.write(new LongWritable(Long.valueOf(split[0])), new Text(source + " " + split[1]));
    }
  }

  public static class FileMapper2 extends FileMapper {

    @Override
    public void initialize(MapReduceTaskContext context) throws Exception {
      super.initialize(context);

      // Change the source so that the map() method will emit data with the same "source" id for the reducer to match.
      source = PURCHASES;
    }

    @Override
    protected void validateSource(@Nullable String source) {
      // we aliased the fileset 'purchases2' as 'secondPurchases'
      Preconditions.checkArgument("secondPurchases".equals(source));
    }
  }

  public static class FileReducer extends Reducer<LongWritable, Text, String, String> {

    @Override
    public void reduce(LongWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
      String name = null;
      int totalSpend = 0;

      for (Text value : values) {
        String[] split = value.toString().split(" ");
        String source = split[0];
        String data = split[1];

        if (PURCHASES.equals(source)) {
          totalSpend += Integer.valueOf(data);
        } else if (CUSTOMERS.equals(source)) {
          name = data;
        }
      }
      Preconditions.checkNotNull(name);
      context.write(key.toString(), name + " " + totalSpend);
    }
  }

}
