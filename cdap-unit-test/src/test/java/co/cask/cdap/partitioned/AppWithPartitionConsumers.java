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

package co.cask.cdap.partitioned;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.partitioned.KVTableStatePersistor;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionBatchInput;
import co.cask.cdap.api.dataset.lib.partitioned.TransactionalPartitionConsumer;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.worker.AbstractWorker;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;
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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * App used to test that MapReduce and Worker can incrementally consume partitions.
 */
public class AppWithPartitionConsumers extends AbstractApplication {

  @Override
  public void configure() {
    setName("AppWithPartitionConsumers");
    setDescription("Application with MapReduce job and Worker consuming partitions of a PartitionedFileSet Dataset");
    createDataset("consumingState", KeyValueTable.class);
    createDataset("counts", IncrementingKeyValueTable.class);
    addMapReduce(new WordCountMapReduce());
    addWorker(new WordCountWorker());
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

    // Create the "outputLines" partitioned file set, configure it to work with MapReduce
    createDataset("outputLines", PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").build())
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
      // enable explore
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreFormatProperty("delimiter", "\n")
      .setExploreSchema("record STRING")
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


  public static class WordCountWorker extends AbstractWorker {
    public static final String NAME = "WordCountWorker";

    @Override
    public void run() {
      TransactionalPartitionConsumer partitionConsumer =
        new TransactionalPartitionConsumer(getContext(), "lines",
                                           new KVTableStatePersistor("consumingState", "state.key"));
      final List<PartitionDetail> partitions = partitionConsumer.consumePartitions().getPartitions();

      if (partitions.isEmpty()) {
        return;
      }

      // process the partitions (same as WordCountMapReduce):
      //   - read the partitions' files
      //   - increment the words' counts in the 'counts' dataset accordingly
      //   - write the counts to the 'outputLines' partitioned fileset
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {

          Map<String, Long> wordCounts = new HashMap<>();
          for (PartitionDetail partition : partitions) {
            ByteBuffer content;
            Location location = partition.getLocation();
            content = ByteBuffer.wrap(ByteStreams.toByteArray(location.getInputStream()));
            String string = Bytes.toString(Bytes.toBytes(content));

            for (String token : string.split(" ")) {
              Long count = Objects.firstNonNull(wordCounts.get(token), 0L);
              wordCounts.put(token, count + 1);
            }
          }

          IncrementingKeyValueTable counts = context.getDataset("counts");
          for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            counts.write(Bytes.toBytes(entry.getKey()), entry.getValue());
          }

          PartitionedFileSet outputLines = context.getDataset("outputLines");
          PartitionKey partitionKey = PartitionKey.builder().addLongField("time", System.currentTimeMillis()).build();
          PartitionOutput outputPartition = outputLines.getPartitionOutput(partitionKey);

          Location partitionDir = outputPartition.getLocation();
          partitionDir.mkdirs();
          Location outputLocation = partitionDir.append("file");
          outputLocation.createNew();
          try (OutputStream outputStream = outputLocation.getOutputStream()) {
            outputStream.write(Bytes.toBytes(Joiner.on("\n").join(wordCounts.values())));
          }
          outputPartition.addPartition();

        }
      });

      partitionConsumer.onFinish(partitions, true);
    }
  }

  public static class WordCountMapReduce extends AbstractMapReduce {
    public static final String NAME = "WordCountMapReduce";

    private PartitionBatchInput.BatchPartitionCommitter batchPartitionCommitter;

    @Override
    public void configure() {
      setOutputDataset("counts");
      setMapperResources(new Resources(1024));
      setReducerResources(new Resources(1024));
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      batchPartitionCommitter =
        PartitionBatchInput.setInput(context, "lines", new KVTableStatePersistor("consumingState", "state.key"));

      Map<String, String> outputArgs = new HashMap<>();
      PartitionKey partitionKey = PartitionKey.builder().addLongField("time", context.getLogicalStartTime()).build();
      PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, partitionKey);

      // We know that PartitionedFileSet is an OutputFormatProvider, so we set an instance of it as an output to the
      // MapReduce job to test MapReduceContext#addOutput(Output#OutputFormatProviderOutput)
      final PartitionedFileSet outputLines = context.getDataset("outputLines", outputArgs);
      context.addOutput(Output.of("outputLines", outputLines));

      Job job = context.getHadoopJob();
      job.setMapperClass(Tokenizer.class);
      job.setReducerClass(Counter.class);
      job.setNumReduceTasks(1);
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      batchPartitionCommitter.onFinish(succeeded);
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
    public static class Counter extends Reducer<Text, IntWritable, byte[], Long>
      implements ProgramLifecycle<MapReduceTaskContext<byte[], Long>> {

      private MapReduceTaskContext<byte[], Long> mapReduceTaskContext;

      @Override
      public void initialize(MapReduceTaskContext<byte[], Long> context) throws Exception {
        this.mapReduceTaskContext = context;
      }

      @Override
      public void destroy() {
      }

      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        long sum = 0L;
        for (IntWritable value : values) {
          sum += value.get();
        }
        mapReduceTaskContext.write("counts", key.getBytes(), sum);
        mapReduceTaskContext.write("outputLines", null, sum);
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
