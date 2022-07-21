/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.test.artifacts;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceContext;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.JavaSparkMain;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import io.cdap.cdap.api.workflow.WorkflowContext;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import scala.Tuple2;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * App that uses a Plugin.
 */
public class AppWithPlugin extends AbstractApplication {

  private static final String KEY = "toString";
  public static final String TEST = "this is a test string";
  public static final String WORKER = "testWorker";
  public static final String MAPREDUCE = "testMapReduce";
  public static final String SERVICE = "testService";
  public static final String SPARK = "testSpark";
  public static final String SPARK_INPUT = "sparkInput";
  public static final String SPARK_TABLE = "sparkTable";
  public static final String WORKFLOW = "testWorkflow";
  public static final String WORKFLOW_TABLE = "workflowTable";

  @Override
  public void configure() {
    addWorker(new WorkerWithPlugin());
    addMapReduce(new MapReduceWithPlugin());
    addService(new ServiceWithPlugin());
    addSpark(new SparkWithPlugin());
    usePlugin("t1", "n1", "mrid", PluginProperties.builder().add(KEY, TEST).build());
    addWorkflow(new WorkflowWithPlugin());
  }

  public static class WorkflowWithPlugin extends AbstractWorkflow {
    private Metrics metrics;

    @Override
    protected void configure() {
      setName(WORKFLOW);
      addMapReduce(MAPREDUCE);
      usePlugin("t1", "n1", "workflowplugin", PluginProperties.builder().add(KEY, TEST).build());
      createDataset(WORKFLOW_TABLE, KeyValueTable.class);
    }

    @Override
    public void destroy() {
      metrics.gauge(String.format("destroy.%s", WORKFLOW), 1);
      WorkflowContext context = getContext();
      KeyValueTable table = context.getDataset(WORKFLOW_TABLE);
      try {
        Object plugin = context.newPluginInstance("workflowplugin");
        table.write("val", plugin.toString());
      } catch (InstantiationException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public static class ServiceWithPlugin extends AbstractService {

    @Override
    protected void configure() {
      setName(SERVICE);
      addHandler(new SimpleHandler());
      usePlugin("t1", "n1", "sid", PluginProperties.builder().add(KEY, TEST).build());
    }
  }

  public static class SimpleHandler extends AbstractHttpServiceHandler {

    private Object object;

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      object = getContext().newPluginInstance("sid");
    }

    @Path("/dummy")
    @GET
    public void handle(HttpServiceRequest request, HttpServiceResponder responder) {
      Assert.assertEquals(TEST, object.toString());
      Assert.assertTrue(getContext().getPluginProperties("sid").getProperties().containsKey(KEY));
      responder.sendStatus(200);
    }
  }

  public static class MapReduceWithPlugin extends AbstractMapReduce {

    @Override
    protected void configure() {
      setName(MAPREDUCE);
      createDataset("input", KeyValueTable.class);
      createDataset("output", KeyValueTable.class);
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(SimpleMapper.class);
      job.setNumReduceTasks(0);
      context.addInput(Input.ofDataset("input"));
      context.addOutput(Output.ofDataset("output"));
    }
  }

  public static class SimpleMapper extends Mapper<LongWritable, BytesWritable, byte[], byte[]>
    implements ProgramLifecycle<MapReduceContext> {
    private Object obj;

    @Override
    protected void map(LongWritable key, BytesWritable value, Context context)
      throws IOException, InterruptedException {
      context.write(Bytes.toBytes(key.get()), Bytes.toBytes(key.get()));
    }

    @Override
    public void initialize(MapReduceContext mapReduceContext) throws Exception {
      obj = mapReduceContext.newPluginInstance("mrid");
      Assert.assertEquals(TEST, obj.toString());
      Assert.assertTrue(mapReduceContext.getPluginProperties("mrid").getProperties().containsKey(KEY));
    }

    @Override
    public void destroy() {

    }
  }

  public static class WorkerWithPlugin extends AbstractWorker {

    @Override
    public void run() {
      try {
        Object object = getContext().newPluginInstance("plug");
        Assert.assertEquals(TEST, object.toString());
        Assert.assertTrue(getContext().getPluginProperties("plug").getProperties().containsKey(KEY));
      } catch (InstantiationException e) {
        Assert.fail();
      }
    }

    @Override
    protected void configure() {
      setName(WORKER);
      usePlugin("t1", "n1", "plug", PluginProperties.builder().add(KEY, TEST).build());
    }
  }

  public static class SparkWithPlugin extends AbstractSpark implements JavaSparkMain {

    @Override
    protected void configure() {
      setName(SPARK);
      setMainClass(getClass());

      createDataset(SPARK_INPUT, FileSet.class, FileSetProperties.builder()
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
      createDataset(SPARK_TABLE, Table.class);

      usePlugin("t1", "n1", "plugin", PluginProperties.builder().add(KEY, TEST).build());
    }

    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      JavaSparkContext jsc = new JavaSparkContext();
      JavaPairRDD<LongWritable, Text> rdd = sec.fromDataset(SPARK_INPUT);

      final Object plugin = sec.getPluginContext().newPluginInstance("plugin");
      JavaPairRDD<byte[], Put> resultRDD = rdd.values()
        .map(text -> text + " " + plugin.toString())
        .mapToPair(str -> new Tuple2<>(str.getBytes(Charsets.UTF_8), new Put(str, str, str)));

      sec.saveAsDataset(resultRDD, SPARK_TABLE);
    }
  }
}
