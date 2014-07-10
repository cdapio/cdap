package net.fake.test.app;

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
      .setDescription("Demonstrates usage of bundle jar applications")
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
      .noWorkflow()
      //.withWorkflows().add(new SimpleWorkflow())
      .build();
  }

  public static String loadTestClasses() {
    try {
      // Use context classloader instead of BundleJarApp.class.getClassLoader() b/c this is used only in unit test
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
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
    private static final Logger LOG = LoggerFactory.getLogger(PrintProcedure.class);

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
        // Use context classloader instead of BundleJarApp.class.getClassLoader() b/c this is used only in unit test
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return classLoader.loadClass(className).getName();
      } catch (Exception e) {
        LOG.error("Error", e);
        return "null";
      }
    }
  }

  /**
   * Queries simpleOutputDataset.
   */
  public static class SimpleGetOutput extends AbstractProcedure {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleGetOutput.class);

    @UseDataSet("simpleOutputDataset")
    private KeyValueTable output;

    @Handle("get")
    public void get(ProcedureRequest request, ProcedureResponder responder)
      throws IOException, InterruptedException {

      LOG.info("Hello " + loadTestClasses());

      String key = request.getArgument("key");
      String value = Bytes.toString(output.read(Bytes.toBytes(key)));
      if (value == null) {
        value = "null";
      }

      responder.sendJson(ImmutableMap.of(key, value));
    }
  }

  /**
   * Queries simpleInputDataset.
   */
  public static class SimpleGetInput extends AbstractProcedure {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleGetInput.class);

    @UseDataSet("simpleInputDataset")
    private KeyValueTable input;

    @Handle("get")
    public void get(ProcedureRequest request, ProcedureResponder responder)
      throws IOException, InterruptedException {

      LOG.info("Hello " + loadTestClasses());

      String key = request.getArgument("key");
      String value = Bytes.toString(input.read(Bytes.toBytes(key)));
      if (value == null) {
        value = "null";
      }

      responder.sendJson(ImmutableMap.of(key, value));
    }
  }

  /**
   * Transfers data without transformation from simpleInputDataset to simpleOutputDataset.
   */
  public static class SimpleMapReduce extends AbstractMapReduce {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleMapReduce.class);

    @UseDataSet("simpleInputDataset")
    private KeyValueTable input;

    public SimpleMapReduce() {

    }

    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("SimpleMapReduce")
        .setDescription("Description")
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
     * Transforms input key value data into key value + loadTestClasses().
     */
    public static class SimpleMapper extends Mapper<byte[], byte[], BytesWritable, BytesWritable> {
      private static final Logger LOG = LoggerFactory.getLogger(SimpleMapper.class);

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
     * Transforms input key value data into key value + loadTestClasses().
     */
    public static class SimpleReducer extends Reducer<BytesWritable, BytesWritable, byte[], byte[]> {
      private static final Logger LOG = LoggerFactory.getLogger(SimpleReducer.class);

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
          byte[] realVal = Bytes.toBytes(Bytes.toString(val.getBytes()) + "=reduce=" + loadTestClasses());
          context.write(key.getBytes(), realVal);
        }
      }
    }
  }

  /**
   * Runs a workflow action that calls loadTestClasses().
   */
  public static class SimpleWorkflow implements Workflow {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleWorkflow.class);

    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("SimpleWorkflow")
        .setDescription("Description")
        .onlyWith(new SimpleWorkflowAction())
        .build();
    }

    private class SimpleWorkflowAction extends com.continuuity.api.workflow.AbstractWorkflowAction {
      @Override
      public WorkflowActionSpecification configure() {
        return WorkflowActionSpecification.Builder.with()
          .setName("SimpleWorkflowAction")
          .setDescription("Description")
          .build();
      }

      @Override
      public void run() {
        LOG.info("Hello " + loadTestClasses());
      }
    }
  }

  /**
   * Flow that writes from simpleInputStream to simpleInputDataset.
   */
  public static class SimpleFlow implements Flow {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleFlow.class);

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("SimpleFlow")
        .setDescription("Description")
        .withFlowlets().add("simpleFlowlet", new SimpleFlowlet())
        .connect().from(new Stream("simpleInputStream")).to("simpleFlowlet")
        .build();
    }

    private static class SimpleFlowlet extends AbstractFlowlet {
      private static final Logger LOG = LoggerFactory.getLogger(SimpleFlowlet.class);

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
