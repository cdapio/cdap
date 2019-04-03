/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.service.BasicService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * BundleJarApp contains a service that uses a third party library.
 */
public class BundleJarApp extends AbstractApplication {
  private static final Logger LOG = LoggerFactory.getLogger(BundleJarApp.class);
  public static final String EXPECTED_LOAD_TEST_CLASSES_OUTPUT =
    "hello_HelloWorld__io_cdap_cdap_api_schedule_Trigger";

  @Override
  public void configure() {
    setName("BundleJarApp");
    setDescription("Demonstrates usage of bundle jar applications");
    createDataset("simpleInputDataset", KeyValueTable.class);
    createDataset("simpleOutputDataset", KeyValueTable.class);
    addService(new BasicService("SimpleGetOutput", new SimpleGetOutput()));
    addService(new BasicService("SimpleGetInput", new SimpleGetInput()));
    addService(new BasicService("PrintService", new PrintHandler()));
    addService("SimpleWrite", new SimpleWriteHandler());
    addMapReduce(new SimpleMapReduce());
  }

  public static String loadTestClasses() {
    try {
      // Use context classloader instead of BundleJarApp.class.getClassLoader() b/c this is used only in unit test
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      String result = classLoader.loadClass("hello.HelloWorld").getName() + "__" + Trigger.class.getName();
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
   * Handler that writes to simpleInputDataset.
   */
  public static final class SimpleWriteHandler extends AbstractHttpServiceHandler {

    @UseDataSet("simpleInputDataset")
    private KeyValueTable input;

    @PUT
    @Path("/put/{key}")
    public void put(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("key") String key) {
      String value = Bytes.toString(request.getContent());
      input.write(key, value + loadTestClasses());
      responder.sendStatus(200);
    }
  }
}
