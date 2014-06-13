package com.continuuity.batch.stream;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.mapreduce.AbstractMapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Flow stream integration tests.
 */
public class TestBatchStreamIntegrationApp implements Application {
  private static final Logger LOG = LoggerFactory.getLogger(TestBatchStreamIntegrationApp.class);

  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}.
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("TestFlowStreamIntegrationApp")
      .setDescription("Application for testing batch stream dequeue")
      .withStreams().add(new Stream("s1"))
      .withDataSets().add(new KeyValueTable("results"))
      .withFlows().add(new StreamTestFlow())
      .noProcedure()
      .withMapReduce().add(new StreamTestBatch())
      .noWorkflow()
      .build();
  }

  public static class StreamTestBatch extends AbstractMapReduce {

    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("StreamTestBatch")
        .setDescription("Batch job for testing batch stream read")
        .useInputDataSet("stream://s1")
        .useOutputDataSet("results")
        .build();
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(StreamTestBatchMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setReducerClass(StreamTestBatchReducer.class);
    }
  }

  public static class StreamTestBatchMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.write(new Text(""), value);
    }
  }
  public static class StreamTestBatchReducer extends Reducer<Text, Text, byte[], byte[]> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text value : values) {
        context.write(value.getBytes(), Bytes.toBytes(""));
      }
    }
  }

  /**
   * Stream test flow.
   */
  public static class StreamTestFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("StreamTestFlow")
        .setDescription("Flow for testing batch stream dequeue")
        .withFlowlets().add(new StreamReader())
        .connect().fromStream("s1").to("StreamReader")
        .build();
    }
  }

  /**
   * StreamReader flowlet.
   */
  public static class StreamReader extends AbstractFlowlet {

    @ProcessInput
    @Batch(100)
    public void foo(Iterator<StreamEvent> it) {
      List<StreamEvent> events = ImmutableList.copyOf(it);
      LOG.warn("Number of batched stream events = " + events.size());
      Assert.assertTrue(events.size() > 1);

      List<Integer> out = Lists.newArrayList();
      for (StreamEvent event : events) {
        out.add(Integer.parseInt(Charsets.UTF_8.decode(event.getBody()).toString()));
      }
      LOG.info("Read events=" + out);
    }
  }
}
