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

package co.cask.cdap.test.app;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An app that has a simple MR using dynamic partitioning. It maps every key to a partition of (x=key),
 * and it writes to all output datasets given in its runtime argument "outputs".
 */
public class AppWithDynamicPartitioning extends AbstractApplication {

  @Override
  public void configure() {
    addMapReduce(new DynamicPartitioningMR());
    createDataset("input", KeyValueTable.class);
  }

  public static class DynamicPartitioningMR extends AbstractMapReduce {

    @Override
    protected void initialize() throws Exception {
      getContext().addInput(Input.ofDataset("input"));
      Map<String, String> outputArgs = new HashMap<>();
      PartitionedFileSetArguments.setDynamicPartitioner(outputArgs, KeyPartitioner.class);
      String[] outputs = getContext().getRuntimeArguments().get("outputs").split(" ");
      for (String outputDataset : outputs) {
        getContext().addOutput(Output.ofDataset(outputDataset, outputArgs));
      }
      Job job = getContext().getHadoopJob();
      job.setMapperClass(DynamicMapper.class);
      job.setNumReduceTasks(0);
    }

    public static final class KeyPartitioner extends DynamicPartitioner<String, String> {
      @Override
      public PartitionKey getPartitionKey(String key, String value) {
        return PartitionKey.builder()
          .addStringField("x", key)
          .build();
      }
    }

    public static class DynamicMapper extends Mapper<byte[], byte[], String, String>
      implements ProgramLifecycle<MapReduceTaskContext> {

      private MapReduceTaskContext context;
      private String[] outputs;

      @Override
      public void initialize(MapReduceTaskContext context) throws Exception {
        this.context = context;
        outputs = context.getRuntimeArguments().get("outputs").split(" ");
      }

      @Override
      public void destroy() {
        // no-op
      }

      @Override
      protected void map(byte[] key, byte[] value, Context ctx) throws IOException, InterruptedException {
        if (outputs.length == 1) {
          ctx.write(Bytes.toString(key), Bytes.toString(value));
        } else {
          for (String output : outputs) {
            context.write(output, Bytes.toString(key), Bytes.toString(value));
          }
        }
      }
    }
  }
}

