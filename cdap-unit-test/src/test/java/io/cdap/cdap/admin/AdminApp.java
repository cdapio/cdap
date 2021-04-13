/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.admin;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.InstanceConflictException;
import io.cdap.cdap.api.dataset.InstanceNotFoundException;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.JavaSparkMain;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Assert;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

public class AdminApp extends AbstractApplication {

  public static final String MAPREDUCE_NAME = "AdminMapReduce";
  public static final String SPARK_NAME = "AdminSpark";
  public static final String SPARK_SCALA_NAME = "AdminScalaSpark";
  public static final String SERVICE_NAME = "AdminService";
  public static final String WORKER_NAME = "AdminWorker";
  public static final String WORKFLOW_NAME = "AdminWorkflow";

  @Override
  public void configure() {
    addMapReduce(new AdminMapReduce());
    addSpark(new AdminSpark());
    addSpark(new AdminScalaSpark());
    addWorker(new AdminWorker());
    addWorkflow(new AdminWorkflow());
    addService(new AbstractService() {
      @Override
      protected void configure() {
        setName(SERVICE_NAME);
        addHandler(new DatasetAdminHandler());
        addHandler(new CrossNamespaceHandler());
      }
    });
  }

  /**
   * Handler to test cross namespace methods.
   */
  public static class CrossNamespaceHandler extends AbstractHttpServiceHandler {

    @GET
    @Path("/namespaces/{namespace}")
    public void getNamespace(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("namespace") String namespace) throws IOException {
      Admin admin = getContext().getAdmin();
      NamespaceSummary summary = admin.getNamespaceSummary(namespace);
      if (summary == null) {
        responder.sendError(404, String.format("namespace '%s' not found.", namespace));
      } else {
        responder.sendJson(summary);
      }
    }

    @GET
    @Path("/namespaces/{namespace}/plugins")
    public void getPlugins(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("namespace") String namespace) throws IOException {
      Admin admin = getContext().getAdmin();
      if (!admin.namespaceExists(namespace)) {
        responder.sendError(404, String.format("namespace '%s' not found.", namespace));
        return;
      }
      List<ArtifactSummary> response = new ArrayList<>();
      for (ArtifactInfo artifactInfo : getContext().listArtifacts(namespace)) {
        getContext().createClassLoader(namespace, artifactInfo, getClass().getClassLoader());
        response.add(artifactInfo);
      }
      responder.sendJson(response);
    }
  }

  public static class DatasetAdminHandler extends AbstractHttpServiceHandler {

    private static final Gson GSON = new Gson();

    @GET
    @Path("exists/{dataset}")
    public void exists(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("dataset") String dataset) throws DatasetManagementException {
      Admin admin = getContext().getAdmin();
      responder.sendString(Boolean.toString(admin.datasetExists(dataset)));
    }

    @GET
    @Path("type/{dataset}")
    public void type(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("dataset") String dataset) throws DatasetManagementException {
      Admin admin = getContext().getAdmin();
      if (!admin.datasetExists(dataset)) {
        responder.sendStatus(404);
        return;
      }
      responder.sendString(admin.getDatasetType(dataset));
    }

    @GET
    @Path("props/{dataset}")
    public void properties(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("dataset") String dataset) throws DatasetManagementException {
      Admin admin = getContext().getAdmin();
      if (!admin.datasetExists(dataset)) {
        responder.sendStatus(404);
        return;
      }
      responder.sendJson(200, admin.getDatasetProperties(dataset).getProperties());
    }

    @PUT
    @Path("create/{dataset}/{type}")
    public void create(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("dataset") String dataset, @PathParam("type") String type)
      throws DatasetManagementException {

      DatasetProperties datasetProps = parseBodyAsProps(request);
      Admin admin = getContext().getAdmin();
      try {
        admin.createDataset(dataset, type, datasetProps);
        responder.sendStatus(200);
      } catch (InstanceConflictException e) {
        responder.sendStatus(409);
      }
    }

    @PUT
    @Path("update/{dataset}")
    public void update(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("dataset") String dataset)
      throws DatasetManagementException {

      DatasetProperties datasetProps = parseBodyAsProps(request);
      Admin admin = getContext().getAdmin();
      try {
        admin.updateDataset(dataset, datasetProps);
        responder.sendStatus(200);
      } catch (InstanceNotFoundException e) {
        responder.sendStatus(404);
      }
    }

    @POST
    @Path("truncate/{dataset}")
    public void truncate(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("dataset") String dataset)
      throws DatasetManagementException {

      Admin admin = getContext().getAdmin();
      try {
        admin.truncateDataset(dataset);
        responder.sendStatus(200);
      } catch (InstanceNotFoundException e) {
        responder.sendStatus(404);
      }
    }

    @DELETE
    @Path("delete/{dataset}")
    public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("dataset") String dataset)
      throws DatasetManagementException {

      Admin admin = getContext().getAdmin();
      try {
        admin.dropDataset(dataset);
        responder.sendStatus(200);
      } catch (InstanceNotFoundException e) {
        responder.sendStatus(404);
      }
    }

    private static DatasetProperties parseBodyAsProps(HttpServiceRequest request) {
      String body = Bytes.toString(request.getContent());
      if (body.isEmpty()) {
        return DatasetProperties.EMPTY;
      }
      Map<String, String> props = GSON.fromJson(body, new TypeToken<Map<String, String>>() { }.getType());
      return DatasetProperties.of(props);
    }
  }

  public static class AdminWorker extends AbstractWorker {

    @Override
    protected void configure() {
      super.configure();
      setName(WORKER_NAME);
    }

    @Override
    public void run() {
      performAdmin(getContext());
    }
  }

  // this will get called from the worker, also from a custom workflow action
  static void performAdmin(RuntimeContext context) {
    Admin admin = context.getAdmin();
    Map<String, String> args = context.getRuntimeArguments();
    try {
      // if invoked with dropAll=true, clean up all datasets (a, b, c, d)
      if ("true".equals(args.get("dropAll"))) {
        for (String name : new String[]{"a", "b", "c", "d"}) {
          if (admin.datasetExists(name)) {
            admin.dropDataset(name);
          }
        }
      } else {
        // create a, update b with /extra in base path, truncate c, drop d
        admin.createDataset("a", Table.class.getName(), DatasetProperties.EMPTY);

        String type = admin.getDatasetType("b");
        Assert.assertEquals(FileSet.class.getName(), type);
        DatasetProperties bProps = admin.getDatasetProperties("b");
        String base = bProps.getProperties().get("base.path");
        Assert.assertNotNull(base);
        String newBase = args.get("new.base.path");
        DatasetProperties newBProps = ((FileSetProperties.Builder) FileSetProperties.builder()
          .addAll(bProps.getProperties())).setDataExternal(true).setBasePath(newBase).build();
        admin.updateDataset("b", newBProps);

        admin.truncateDataset("c");

        admin.dropDataset("d");
      }
    } catch (DatasetManagementException e) {
      Throwables.propagate(e);
    }

  }

  public static class AdminWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName(WORKFLOW_NAME);
      addAction(new AdminAction());
    }

    public static class AdminAction extends AbstractCustomAction {
      @Override
      public void run() {
        performAdmin(getContext());
      }
    }
  }

  public static class AdminMapReduce extends AbstractMapReduce {

    @Override
    protected void configure() {
      setName(MAPREDUCE_NAME);
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(Tokenizer.class);
      job.setReducerClass(Counter.class);
      job.setNumReduceTasks(1);
      context.addInput(Input.ofDataset("lines"));
      context.addOutput(Output.ofDataset("counts"));

      // truncate the output dataset
      context.getAdmin().truncateDataset("counts");
    }

    public static class Tokenizer extends Mapper<byte[], byte[], Text, LongWritable> {

      static final LongWritable ONE = new LongWritable(1L);

      @Override
      protected void map(byte[] key, byte[] value, Context context) throws IOException, InterruptedException {
        for (String word : Bytes.toString(value).split(" ")) {
          context.write(new Text(word), ONE);
        }
      }
    }

    public static class Counter extends Reducer<Text, LongWritable, byte[], byte[]> {

      @Override
      protected void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
        context.write(Bytes.toBytes(key.toString()), Bytes.toBytes(Iterables.size(values)));
      }
    }
  }

  public static class AdminSpark extends AbstractSpark {

    @Override
    protected void configure() {
      setName(SPARK_NAME);
      setMainClass(WordCountSpark.class);
    }

    public static class WordCountSpark implements JavaSparkMain {

      @Override
      public void run(JavaSparkExecutionContext sec) throws Exception {
        JavaSparkContext jsc = new JavaSparkContext();
        JavaPairRDD<byte[], byte[]> input = sec.fromDataset("lines");
        JavaRDD<String> words = input.values().flatMap((FlatMapFunction<byte[], String>) line ->
          Arrays.asList(Bytes.toString(line).split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<>(s, 1);
          }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
          public Integer call(Integer a, Integer b) {
            return a + b;
          }
        });
        JavaPairRDD<byte[], byte[]> result = counts.mapToPair(
          new PairFunction<Tuple2<String, Integer>, byte[], byte[]>() {
            @Override
            public Tuple2<byte[], byte[]> call(Tuple2<String, Integer> input) throws Exception {
              return new Tuple2<>(Bytes.toBytes(input._1()), Bytes.toBytes(input._2()));
            }
          });
        sec.getAdmin().truncateDataset("counts");
        sec.saveAsDataset(result, "counts");
      }
    }
  }

  public static class AdminScalaSpark extends AbstractSpark {

    @Override
    protected void configure() {
      setName(SPARK_SCALA_NAME);
      setMainClass(ScalaAdminSparkProgram.class);
    }
  }
}

