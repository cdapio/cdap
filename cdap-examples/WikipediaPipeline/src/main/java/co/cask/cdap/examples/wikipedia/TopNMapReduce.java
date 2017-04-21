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

package co.cask.cdap.examples.wikipedia;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * MapReduce program that outputs the top N words from the input text.
 */
public class TopNMapReduce extends AbstractMapReduce {
  public static final String NAME = TopNMapReduce.class.getSimpleName();

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("A MapReduce job that returns the top-n words in a dataset.");
    setMapperResources(new Resources(512));
    setReducerResources(new Resources(512));
  }

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    Map<String, String> runtimeArguments = context.getRuntimeArguments();
    Job job = context.getHadoopJob();
    WorkflowToken workflowToken = context.getWorkflowToken();
    int topNRank = 10;
    if (runtimeArguments.containsKey("topn.rank"))  {
      topNRank = Integer.parseInt(runtimeArguments.get("topn.rank"));
    }
    if (workflowToken != null) {
      workflowToken.put("topn.rank", Value.of(topNRank));
    }
    int numReduceTasks = 1;
    if (runtimeArguments.containsKey("num.reduce.tasks")) {
      numReduceTasks = Integer.parseInt(runtimeArguments.get("num.reduce.tasks"));
    }
    job.setNumReduceTasks(numReduceTasks);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(TopNReducer.class);
    String dataNamespace = runtimeArguments.get(WikipediaPipelineApp.NAMESPACE_ARG);
    dataNamespace = dataNamespace == null ? getContext().getNamespace() : dataNamespace;
    context.addInput(Input.ofDataset(WikipediaPipelineApp.NORMALIZED_WIKIPEDIA_DATASET).fromNamespace(dataNamespace));
    context.addOutput(Output.ofDataset(WikipediaPipelineApp.MAPREDUCE_TOPN_OUTPUT).fromNamespace(dataNamespace));
  }

  @Override
  public void destroy() {
    WorkflowToken workflowToken = getContext().getWorkflowToken();
    if (workflowToken != null) {
      boolean isSuccessful = getContext().getState().getStatus() == ProgramStatus.COMPLETED;
      workflowToken.put("result", Value.of(isSuccessful));
    }
  }

  /**
   * Mapper that emits tokens.
   */
  public static class TokenizerMapper extends Mapper<byte [], byte [], Text, IntWritable> {
    private final Text outputKey = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(byte [] key, byte [] value, Context context) throws IOException, InterruptedException {
      StringTokenizer tokenizer = new StringTokenizer(Bytes.toString(value));
      while (tokenizer.hasMoreTokens()) {
        String word = tokenizer.nextToken().trim();
        outputKey.set(word);
        context.write(outputKey, one);
      }
    }
  }

  /**
   * Reducer that outputs top N tokens. Implements {@link ProgramLifecycle} to demonstrate accessing workflow
   * token in a Mapper/Reducer class.
   */
  public static class TopNReducer extends Reducer<Text, IntWritable, byte [], byte []>
    implements ProgramLifecycle<MapReduceContext> {
    private final Map<Text, Integer> countMap = new HashMap<>();
    private int n = 10;

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      WorkflowToken workflowToken = context.getWorkflowToken();
      if (workflowToken != null) {
        Value value = workflowToken.get("topn.rank");
        if (value != null) {
          n = value.getAsInt();
        }
      }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
      int sum = Iterables.size(values);
      // Need to create a new Text instance because the instance we receive is the same for all records.
      countMap.put(new Text(key), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      List<Map.Entry<Text, Integer>> entries = new ArrayList<>(countMap.entrySet());
      Collections.sort(entries, new Comparator<Map.Entry<Text, Integer>>() {
        @Override
        public int compare(Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2) {
          return o2.getValue().compareTo(o1.getValue());
        }
      });
      for (int i = 0; i < n; i++) {
        Map.Entry<Text, Integer> entry = entries.get(i);
        context.write(Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(entry.getValue()));
        context.getCounter("custom", "num.records").increment(1);
      }
    }

    @Override
    public void destroy() {
      // no-op
    }
  }
}
