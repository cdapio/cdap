package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.mapreduce.AbstractMapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.google.common.base.Charsets;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Simple app for testing data set handling by the program runners.
 * - a flow reading from stream "xx" and writing to dataset "foo"
 * - a query with method get(key) that reads "foo"
 * - a map/reduce job that reads "foo" and writes to another dataset "bar"
 * The datasets are key/value tables that track the number of times each operation
 * (open/close/read/write/getsplits) are called, so the unit test can verify.
 */
@SuppressWarnings("unused")
public class DummyAppWithTrackingTable implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("dummy")
      .setDescription("dummy app with a dataset that tracks open and close")
      .withStreams().add(new Stream("xx"))
      .withDataSets().add(new TrackingTable("foo")).add(new TrackingTable("bar"))
      .withFlows().add(new DummyFlow())
      .withProcedures().add(new DummyProcedure())
      .withMapReduce().add(new DummyBatch())
      .noWorkflow()
      .build();
  }

  /**
   * A flow.
   */
  public static class DummyFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("dummy-flow")
        .setDescription("a dummy flow that does not much")
        .withFlowlets().add("fwlt", new DummyFlowlet())
        .connect().fromStream("xx").to("fwlt")
        .build();
    }
  }

  /**
   * A flowlet.
   */
  public static class DummyFlowlet extends AbstractFlowlet {

    @UseDataSet("foo")
    TrackingTable table;

    @ProcessInput
    public void process(StreamEvent event) {
      byte[] keyAndValue = Bytes.toBytes(event.getBody());
      table.write(keyAndValue, keyAndValue);
    }
  }

  /**
   * A procedure.
   */
  public static class DummyProcedure extends AbstractProcedure {

    @UseDataSet("foo")
    TrackingTable table;

    @Handle("get")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      byte[] key = request.getArgument("key").getBytes(Charsets.UTF_8);
      byte[] value = table.read(key);
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), new String(value, Charsets.UTF_8));
    }
  }

  /**
   * A map/reduce job.
   */
  public static class DummyBatch extends AbstractMapReduce {

    @UseDataSet("foo")
    private TrackingTable table;

    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
                                   .setName("dummy-batch")
                                   .setDescription("batch job that copies from foo to bar")
                                   .useDataSet("foo")
                                   .useInputDataSet("foo")
                                   .useOutputDataSet("bar")
                                   .build();
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(DummyMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setReducerClass(DummyReducer.class);
    }
  }

  /**
   * A mapper.
   */
  public static class DummyMapper extends Mapper<byte[], byte[], Text, Text> {

    @UseDataSet("foo")
    TrackingTable table;

    @Override
    protected void map(byte[] key, byte[] value, Context context)
      throws IOException, InterruptedException {
      byte[] val = table.read(key);
      context.write(new Text(key), new Text(val));
    }
  }

  /**
   * A reducer.
   */
  public static class DummyReducer extends Reducer<Text, Text, byte[], byte[]> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      StringBuilder str = new StringBuilder();
      for (Text text : values) {
        str.append(text.toString());
      }
      context.write(key.getBytes(), str.toString().getBytes(Charsets.UTF_8));
    }
  }
}
