/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package net.fake.test.app;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * BundleJarApp contains a service that uses a third party library.
 */
public class BundleJarApp extends AbstractApplication {
  private static final Logger LOG = LoggerFactory.getLogger(BundleJarApp.class);
  public static final String EXPECTED_LOAD_TEST_CLASSES_OUTPUT =
    "hello_HelloWorld__co_cask_cdap_api_schedule_Schedule";

  @Override
  public void configure() {
    setName("BundleJarApp");
    setDescription("Demonstrates usage of bundle jar applications");
    addStream(new Stream("simpleInputStream"));
    createDataset("simpleInputDataset", KeyValueTable.class);
    createDataset("simpleOutputDataset", KeyValueTable.class);
    addFlow(new SimpleFlow());
    addService(new BasicService("SimpleGetOutput", new SimpleGetOutput()));
    addService(new BasicService("SimpleGetInput", new SimpleGetInput()));
    addService(new BasicService("PrintService", new PrintHandler()));
    addMapReduce(new SimpleMapReduce());
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
  public static class PrintHandler extends AbstractHttpServiceHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PrintHandler.class);

    @GET
    @Path("load/{class}")
    public void load(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("class") String className)
      throws IOException, InterruptedException {

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
  public static class SimpleGetOutput extends AbstractHttpServiceHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleGetOutput.class);

    @UseDataSet("simpleOutputDataset")
    private KeyValueTable output;

    @GET
    @Path("get/{key}")
    public void get(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("key") String key)
      throws IOException, InterruptedException {

      LOG.info("Hello " + loadTestClasses());

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
  public static class SimpleGetInput extends AbstractHttpServiceHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleGetInput.class);

    @UseDataSet("simpleInputDataset")
    private KeyValueTable input;

    @GET
    @Path("get/{key}")
    public void get(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("key") String key)
      throws IOException, InterruptedException {

      LOG.info("Hello " + loadTestClasses());

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

    /**
     * Define a MapReduce job.
     * @throws Exception
     */
    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      LOG.info("Hello " + loadTestClasses());

      Job job = context.getHadoopJob();
      job.setMapperClass(SimpleMapper.class);
      job.setReducerClass(SimpleReducer.class);

      context.addInput(Input.ofDataset("simpleInputDataset", input.getSplits()));
      context.addOutput(Output.ofDataset("simpleOutputDataset"));
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
  public static class SimpleWorkflow extends AbstractWorkflow {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleWorkflow.class);

    @Override
    public void configure() {
        setName("SimpleWorkflow");
        setDescription("Description");
        addAction(new SimpleWorkflowAction());
    }

    private class SimpleWorkflowAction extends AbstractCustomAction {
      @Override
      public void configure() {
        setName("SimpleWorkflowAction");
        setDescription("Description");
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
  public static class SimpleFlow extends AbstractFlow {

    @Override
    protected void configure() {
      setName("SimpleFlow");
      setDescription("Description");
      addFlowlet("simpleFlowlet", new SimpleFlowlet());
      connectStream("simpleInputStream", "simpleFlowlet");
    }

    private static class SimpleFlowlet extends AbstractFlowlet {
      private static final Logger LOG = LoggerFactory.getLogger(SimpleFlowlet.class);

      @UseDataSet("simpleInputDataset")
      private KeyValueTable input;

      @ProcessInput
      public void process(StreamEvent event) {
        LOG.info("Hello " + loadTestClasses());
        String body = Bytes.toString(event.getBody());
        String key = body.split(":")[0];
        String value = body.split(":")[1];
        input.write(key, value + loadTestClasses());
      }
    }
  }
}
