package com.notcontinuuity.examples.bundlejar;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
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
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.schedule.Schedule;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * BundleJarApp contains a procedure that uses a third party library.
 */
public class BundleJarApp implements Application {
  private static final Logger LOG = LoggerFactory.getLogger(BundleJarApp.class);
  public static final String EXPECTED_LOAD_TEST_CLASSES_OUTPUT =
    "hello_HelloWorld__com_continuuity_api_schedule_Schedule";

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("BundleJarApp")
      .setDescription("Demonstrates usage of bundle jar applications " + loadTestClasses())
      .withStreams()
        .add(new Stream("simpleInputStream"))
      .withDataSets()
        .add(new KeyValueTable("simpleInputDataset"))
        .add(new KeyValueTable("simpleOutputDataset"))
      .withFlows().add(new SimpleFlow())
      .withProcedures()
        .add(new SimpleGetOutput())
        .add(new SimpleGetInput())
        .add(new PrintProcedure())
      .withMapReduce().add(new SimpleMapReduce())
      .withWorkflows().add(new SimpleWorkflow())
      .build();
  }

  public static String loadTestClasses() {
    try {
      ClassLoader classLoader = BundleJarApp.class.getClassLoader();
      String result = classLoader.loadClass("hello.HelloWorld").getName() + "__" + Schedule.class.getName();
      return result.replaceAll("\\.", "_");
    } catch (ClassNotFoundException e) {
      LOG.error("Error loading test classes with " + BundleJarApp.class.getClassLoader(), e);
      return "null";
    }
  }

  /**
   * Contains a method that can be run to check if expected classes are loaded.
   */
  public static class PrintProcedure extends AbstractProcedure {

    @Handle("load")
    public void load(ProcedureRequest request, ProcedureResponder responder)
      throws IOException, InterruptedException {

      String className = request.getArgument("class");

      responder.sendJson(
        ImmutableMap.builder()
          .put("Class.forName", loadClassForName(className))
          .build());
    }

    private String loadClassForName(String className) {
      try {
        return Class.forName(className).getName();
      } catch (Exception e) {
        LOG.error("Error", e);
        return e.getMessage();
      }
    }
  }

  public static class SimpleGetOutput extends AbstractProcedure {

    @UseDataSet("simpleOutputDataset")
    private KeyValueTable output;

    @Handle("get")
    public void get(ProcedureRequest request, ProcedureResponder responder)
      throws IOException, InterruptedException {

      LOG.info("Hello " + loadTestClasses());

      String key = request.getArgument("key");
      String value = StringUtils.defaultString(Bytes.toString(output.read(Bytes.toBytes(key))), "null");

      responder.sendJson(ImmutableMap.of(key, value));
    }
  }

  public static class SimpleGetInput extends AbstractProcedure {

    @UseDataSet("simpleInputDataset")
    private KeyValueTable input;

    @Handle("get")
    public void get(ProcedureRequest request, ProcedureResponder responder)
      throws IOException, InterruptedException {

      LOG.info("Hello " + loadTestClasses());

      String key = request.getArgument("key");
      String value = StringUtils.defaultString(Bytes.toString(input.read(Bytes.toBytes(key))), "null");

      responder.sendJson(ImmutableMap.of(key, value));
    }
  }

  public static class SimpleMapReduce extends AbstractMapReduce {

    @UseDataSet("simpleInputDataset")
    private KeyValueTable input;

    public SimpleMapReduce() {

    }

    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("SimpleMapReduce")
        .setDescription("Description" + loadTestClasses())
        .useInputDataSet("simpleInputDataset")
        .useOutputDataSet("simpleOutputDataset")
        .build();
    }

    /**
     * Define a MapReduce job.
     * @param context the context of a MapReduce job
     * @throws Exception
     */
    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      LOG.info("Hello " + loadTestClasses());

      Job job = context.getHadoopJob();
      context.setInput(input, input.getSplits());
      job.setMapperClass(SimpleMapper.class);
      job.setMapOutputKeyClass(BytesWritable.class);
      job.setMapOutputValueClass(BytesWritable.class);

      job.setReducerClass(SimpleReducer.class);
    }

    /**
     * A Mapper that transforms log data into key value pairs,
     * where key is the timestamp on the hour scale and value
     * the occurrence of a log. The Mapper receive a log in a
     * key value pair (<byte[], TimeseriesTable.Entry>) from
     * the input DataSet and outputs data in another key value pair
     * (<LongWritable, IntWritable>) to the Reducer.
     */
    public static class SimpleMapper extends Mapper<byte[], byte[], BytesWritable, BytesWritable> {

      public SimpleMapper() {

      }

      @Override
      public void map(byte[] key, byte[] value, Context context) throws IOException, InterruptedException {
        LOG.info("Hello " + loadTestClasses());
        byte[] realVal = Bytes.toBytes(Bytes.toString(value) + "=map=" + loadTestClasses());
        context.write(new BytesWritable(key), new BytesWritable(realVal));
      }
    }

    /**
     * Aggregate the number of requests per hour and store the results in a SimpleTimeseriesTable.
     */
    public static class SimpleReducer extends Reducer<BytesWritable, BytesWritable, byte[], byte[]> {

      public SimpleReducer() {

      }

      /**
       * Aggregate the number of requests by hour and store the results in the output DataSet.
       * @param key the timestamp in hour
       * @param values the occurrence of logs sent in one hour
       * @param context the context of a MapReduce job
       * @throws java.io.IOException
       * @throws InterruptedException
       */
      @Override
      protected void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context)
        throws IOException, InterruptedException {
        LOG.info("Hello " + loadTestClasses());
        for (BytesWritable val : values) {
          byte[] realVal = Bytes.toBytes(
            Bytes.toString(val.getBytes()) + "=reduce=" + loadTestClasses() + "=time=" + System.currentTimeMillis());
          context.write(key.getBytes(), realVal);
        }
      }
    }
  }

  public static class SimpleWorkflow implements Workflow {
    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("SimpleWorkflow")
        .setDescription("Description" + loadTestClasses())
        .onlyWith(new SimpleWorkflowAction())
        .build();
    }

    private class SimpleWorkflowAction extends com.continuuity.api.workflow.AbstractWorkflowAction {
      @Override
      public WorkflowActionSpecification configure() {
        return WorkflowActionSpecification.Builder.with()
          .setName("SimpleWorkflowAction")
          .setDescription("Description" + loadTestClasses())
          .build();
      }

      @Override
      public void run() {
        LOG.info("Hello " + loadTestClasses());
      }
    }
  }

  public static class SimpleFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("SimpleFlow")
        .setDescription("Description " + loadTestClasses())
        .withFlowlets().add("simpleFlowlet", new SimpleFlowlet())
        .connect().from(new Stream("simpleInputStream")).to("simpleFlowlet")
        .build();
    }

    private class SimpleFlowlet extends AbstractFlowlet {

      @UseDataSet("simpleInputDataset")
      private KeyValueTable input;

      @ProcessInput
      public void process(StreamEvent event) {
        LOG.info("Hello " + loadTestClasses());
        String body = new String(event.getBody().array());
        String key = body.split(":")[0];
        String value = body.split(":")[1];
        input.write(key, value + loadTestClasses());
      }
    }
  }
}
