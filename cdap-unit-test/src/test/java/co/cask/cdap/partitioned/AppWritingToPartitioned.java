/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.partitioned;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import com.google.common.base.Preconditions;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An application used to test MapReduce writing partitions and proper rollback.
 */
public class AppWritingToPartitioned extends AbstractApplication {

  static final String INPUT = "input";
  static final String PFS = "pfs";
  static final String OTHER = "other";
  static final String MAPREDUCE = "PFSWriter";

  @Override
  public void configure() {
    createDataset(INPUT, KeyValueTable.class.getName(), DatasetProperties.EMPTY);
    // create two pfs, identical except for their (table) names
    for (String name : new String[] { PFS, OTHER }) {
      createDataset(name, PartitionedFileSet.class.getName(), PartitionedFileSetProperties.builder()
        .setPartitioning(Partitioning.builder().addIntField("number").build())
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
        .setEnableExploreOnCreate(true)
        .setExploreTableName(name)
        .setExploreSchema("key STRING, value STRING")
        .setExploreFormat("csv")
        .build());
    }
    addMapReduce(new PartitionWriterMR());
  }

  public static class PartitionWriterMR extends AbstractMapReduce {

    @Override
    public void configure() {
      setName(MAPREDUCE);
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(TokenMapper.class);
      job.setNumReduceTasks(0);

      String inputText = getContext().getRuntimeArguments().get("input.text");
      Preconditions.checkNotNull(inputText);
      KeyValueTable kvTable = getContext().getDataset(INPUT);
      kvTable.write("key", inputText);
      context.addInput(Input.ofDataset(INPUT, kvTable.getSplits(1, null, null)));

      String outputDatasets = getContext().getRuntimeArguments().get("output.datasets");
      outputDatasets = outputDatasets != null ? outputDatasets : PFS;
      for (String outputName : outputDatasets.split(",")) {
        String outputPartition = getContext().getRuntimeArguments().get(outputName + ".output.partition");
        PartitionKey outputPartitionKey = outputPartition == null ? null :
          PartitionKey.builder().addField("number", Integer.parseInt(outputPartition)).build();
        Map<String, String> outputArguments = new HashMap<>();
        if (outputPartitionKey != null) {
          PartitionedFileSetArguments.setOutputPartitionKey(outputArguments, outputPartitionKey);
        } else {
          PartitionedFileSetArguments.setDynamicPartitioner(outputArguments, KeyPartitioner.class);
        }
        context.addOutput(Output.ofDataset(outputName, outputArguments));
      }
    }

    public static final class KeyPartitioner extends DynamicPartitioner<String, String> {
      @Override
      public PartitionKey getPartitionKey(String key, String value) {
        return PartitionKey.builder().addIntField("number", Integer.parseInt(key.substring(0, 1))).build();
      }
    }

    /**
     * A mapper that splits each input line and emits each token with a value of 1.
     */
    public static class TokenMapper extends Mapper<byte[], byte[], String, String>
      implements ProgramLifecycle<MapReduceTaskContext<String, String>> {

      MapReduceTaskContext<String, String> taskContext;
      String[] outputs = null;

      @Override
      public void initialize(MapReduceTaskContext<String, String> context) throws Exception {
        taskContext = context;
        String outputDatasets = context.getRuntimeArguments().get("output.datasets");
        if (outputDatasets != null) {
          String[] splitOutputs = outputDatasets.split(",");
          if (splitOutputs.length > 1) {
            outputs = splitOutputs;
          }
        }
      }

      @Override
      public void destroy() {
      }

      @Override
      public void map(byte[] key, byte[] data, Context context)
        throws IOException, InterruptedException {
        for (String word : Bytes.toString(data).split(" ")) {
          if (outputs != null) {
            for (String output : outputs) {
              taskContext.write(output, word, word);
            }
          } else {
            context.write(word, word);
          }
        }
      }
    }
  }
}
