/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.BatchPartitionConsumer;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * App used to test that MapReduce can incrementally consume partitions.
 */
public class AppWithMapReduceConsumingPartitions extends AbstractApplication {

  @Override
  public void configure() {
    setName("AppWithMapReduceConsumingPartitions");
    setDescription("Application with MapReduce job consuming partitions of a PartitionedFileSet Dataset");
    createDataset("consumingState", KeyValueTable.class);
    createDataset("counts", IncrementingKeyValueTable.class);
    addMapReduce(new WordCount());
    addService(new DatasetService());

    // Create the "lines" partitioned file set, configure it to work with MapReduce
    createDataset("lines", PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").build())
        // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
      .build());
  }

  // BatchWritable which increments the values of the underlying KeyValue table, upon each write from the batch job.
  public static class IncrementingKeyValueTable extends AbstractDataset implements BatchWritable<byte[], Long> {

    private final KeyValueTable keyValueTable;

    public IncrementingKeyValueTable(DatasetSpecification spec,
                                     @EmbeddedDataset("store") KeyValueTable keyValueTable) {
      super(spec.getName(), keyValueTable);
      this.keyValueTable = keyValueTable;
    }

    @Override
    public void write(byte[] key, Long value) {
      keyValueTable.increment(key, value);
    }

    @Nullable
    public Long read(String key) {
      byte[] read = keyValueTable.read(key);
      return read == null ? null : Bytes.toLong(read);
    }
  }

  public static class WordCount extends AbstractMapReduce {

    private final BatchPartitionConsumer batchPartitionConsumer = new BatchPartitionConsumer() {
      private static final String STATE_KEY = "state.key";

      @Nullable
      @Override
      protected byte[] readBytes(DatasetContext datasetContext) {
        return ((KeyValueTable) datasetContext.getDataset("consumingState")).read(STATE_KEY);
      }

      @Override
      protected void writeBytes(DatasetContext datasetContext, byte[] stateBytes) {
        ((KeyValueTable) datasetContext.getDataset("consumingState")).write(STATE_KEY, stateBytes);
      }
    };

    @Override
    public void configure() {
      setOutputDataset("counts");
      setMapperResources(new Resources(1024));
      setReducerResources(new Resources(1024));
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      PartitionedFileSet lines = batchPartitionConsumer.getPartitionedFileSet(context, "lines");
      context.setInput("lines", lines);

      Job job = context.getHadoopJob();
      job.setMapperClass(Tokenizer.class);
      job.setReducerClass(Counter.class);
      job.setNumReduceTasks(1);
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      if (succeeded) {
        batchPartitionConsumer.onFinish(context);
      }
      super.onFinish(succeeded, context);
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
    public static class Counter extends Reducer<Text, IntWritable, byte[], Long> {

      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        long sum = 0L;
        for (IntWritable value : values) {
          sum += value.get();
        }
        context.write(key.getBytes(), sum);
      }
    }
  }

  public static class DatasetService extends AbstractService {

    @Override
    protected void configure() {
      setName("DatasetService");
      addHandler(new DatasetServingHandler());
    }

    /**
     * A handler that allows reading and writing with lines and counts Datasets.
     */
    public static class DatasetServingHandler extends AbstractHttpServiceHandler {

      @UseDataSet("lines")
      private PartitionedFileSet lines;

      @UseDataSet("counts")
      private IncrementingKeyValueTable keyValueTable;

      @PUT
      @Path("lines")
      public void write(HttpServiceRequest request, HttpServiceResponder responder,
                        @QueryParam("time") Long time) {

        PartitionKey key = PartitionKey.builder().addLongField("time", time).build();
        PartitionOutput partitionOutput = lines.getPartitionOutput(key);
        Location location = partitionOutput.getLocation();

        try {
          try (WritableByteChannel channel = Channels.newChannel(location.getOutputStream())) {
            channel.write(request.getContent());
          }
          partitionOutput.addPartition();
        } catch (IOException e) {
          responder.sendError(400, String.format("Unable to write path '%s'", location));
          return;
        }
        responder.sendStatus(200);
      }

      @GET
      @Path("counts")
      public void get(HttpServiceRequest request, HttpServiceResponder responder,
                      @QueryParam("word") String word) {
        Long count = keyValueTable.read(word);
        if (count == null) {
          // if the word is missing from the table, it has a word count of 0
          count = 0L;
        }
        responder.sendJson(count);
      }

    }
  }
}
