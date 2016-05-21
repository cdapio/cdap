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

package co.cask.cdap.examples.clicksandviews;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A MapReduce program that reads from a CLICKS stream and a VIEWS stream. It performs a join across these two streams,
 * based upon the viewId of the records. It then keeps track of how many clicks a particular view resulted in.
 */
public class ClicksAndViewsMapReduce extends AbstractMapReduce {
  static final String NAME = "ClicksAndViewsMapReduce";

  private static final Joiner TAB_JOINER = Joiner.on("\t");

  @Override
  public void configure() {
    setName(NAME);
    setMapperResources(new Resources(1024));
    setReducerResources(new Resources(1024));
  }

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    context.addInput(Input.ofStream(ClicksAndViews.CLICKS));
    context.addInput(Input.ofStream(ClicksAndViews.VIEWS));

    PartitionedFileSet joinedPFS = context.getDataset(ClicksAndViews.JOINED);
    PartitionKey outputPartitionKey =
      PartitionedFileSetArguments.getOutputPartitionKey(context.getRuntimeArguments(), joinedPFS.getPartitioning());

    if (outputPartitionKey == null) {
      outputPartitionKey = PartitionKey.builder().addLongField("runtime", context.getLogicalStartTime()).build();
    }

    Map<String, String> outputArgs = new HashMap<>();
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, outputPartitionKey);
    context.addOutput(Output.ofDataset(ClicksAndViews.JOINED, outputArgs));

    Job job = context.getHadoopJob();
    job.setMapperClass(ImpressionKeyingMapper.class);
    job.setReducerClass(JoiningReducer.class);
  }

  /**
   * A Mapper which tags all of the records with the source that it is coming from.
   * It also keys all of the output records with the viewId, so that a single reducer gets all of the records
   * for a particular viewId.
   */
  public static class ImpressionKeyingMapper extends Mapper<LongWritable, Text, LongWritable, Text>
    implements ProgramLifecycle<MapReduceTaskContext<LongWritable, Text>> {

    private String inputName;

    @Override
    public void initialize(MapReduceTaskContext<LongWritable, Text> context) throws Exception {
      inputName = context.getInputName();
      Preconditions.checkNotNull(inputName);
      Preconditions.checkArgument(ClicksAndViews.CLICKS.equals(inputName) || ClicksAndViews.VIEWS.equals(inputName));
    }

    @Override
    public void destroy() {
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] parts = value.toString().split("\t");
      // tag each record with the input name, so the reducer is simpler
      context.write(new LongWritable(Long.valueOf(parts[0])), new Text(TAB_JOINER.join(inputName, value.toString())));
    }
  }

  /**
   * Reducer class which looks at all the records for the viewId.
   * It outputs the view record, while appending the count of how many click records there are for it.
   */
  public static class JoiningReducer extends Reducer<LongWritable, Text, NullWritable, String> {

    @Override
    public void reduce(LongWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
      String viewData = null;
      int totalClicks = 0;

      for (Text value : values) {
        String[] parts = value.toString().split("\t", 2);
        String source = parts[0];
        if (ClicksAndViews.CLICKS.equals(source)) {
          totalClicks += 1;
        } else if (ClicksAndViews.VIEWS.equals(source)) {
          viewData = parts[1];
        }
      }
      Preconditions.checkNotNull(viewData);
      // the viewId (which we key on from the mapper output) is already in the viewData
      context.write(NullWritable.get(), TAB_JOINER.join(viewData, totalClicks));
    }
  }
}
