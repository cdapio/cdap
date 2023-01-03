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

package io.cdap.cdap.client.app;

import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.ObjectMappedTable;
import io.cdap.cdap.api.dataset.lib.ObjectMappedTableProperties;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * App that contains all program types. Used to test Metadata store.
 */
public class AllProgramsApp extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(AllProgramsApp.class);

  public static final String NAME = "App";
  public static final String DESCRIPTION = "Application which has everything";
  public static final String DATASET_NAME = "kvt";
  public static final String DATASET_NAME2 = "kvt2";
  public static final String DATASET_NAME3 = "kvt3";
  public static final String DATASET_NAME4 = "fileSet";
  public static final String DATASET_NAME5 = "partitionedFileSet";
  public static final String DATASET_NAME6 = "fileSetNotExplorable";
  public static final String DATASET_NAME7 = "partitionedFileSetNotExplorable";
  public static final String PLUGIN_DESCRIPTION = "test plugin";
  public static final String PLUGIN_NAME = "mytestplugin";
  public static final String PLUGIN_TYPE = "testplugin";
  public static final String SCHEDULE_NAME = "testschedule";
  public static final String SCHEDULE_DESCRIPTION = "EveryMinute";
  public static final String DS_WITH_SCHEMA_NAME = "dsWithSchema";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Application which has everything");
    createDataset(DATASET_NAME, KeyValueTable.class,
                  DatasetProperties.builder().setDescription("test dataset").build());
    createDataset(DATASET_NAME2, KeyValueTable.class);
    createDataset(DATASET_NAME3, KeyValueTable.class);
    createDataset(DATASET_NAME4, FileSet.class,
                  FileSetProperties.builder()
                    .setDescription("fileSet")
                    .build());
    createDataset(DATASET_NAME5, PartitionedFileSet.class,
                  PartitionedFileSetProperties.builder()
                    .setDescription("partitonedFileSet")
                    .add("partitioning.fields.", "field1")
                    .add("partitioning.field.field1", "STRING")
                    .build());
    createDataset(DATASET_NAME6, FileSet.class,
                  FileSetProperties.builder()
                    .setDescription("fileSet")
                    .build());
    createDataset(DATASET_NAME7, PartitionedFileSet.class,
                  PartitionedFileSetProperties.builder()
                    .setDescription("partitonedFileSet")
                    .add("partitioning.fields.", "field1")
                    .add("partitioning.field.field1", "STRING")
                    .build());
    addMapReduce(new NoOpMR());
    addMapReduce(new NoOpMR2());
    addWorkflow(new NoOpWorkflow());
    addWorker(new NoOpWorker());
    addSpark(new NoOpSpark());
    addService(new NoOpService());
    schedule(buildSchedule(SCHEDULE_NAME, ProgramType.WORKFLOW, NoOpWorkflow.NAME)
               .setDescription(SCHEDULE_DESCRIPTION)
               .triggerByTime("* * * * *"));
    try {
      createDataset(DS_WITH_SCHEMA_NAME, ObjectMappedTable.class,
                    ObjectMappedTableProperties.builder()
                      .setType(DsSchema.class)
                      .setDescription("test object mapped table")
                      .build()
      );
    } catch (UnsupportedTypeException e) {
      // ignore for test
    }
  }

  /**
   *
   */
  @SuppressWarnings("unused")
  public static class DsSchema {
    String field1;
    int field2;
  }

  /**
   *
   */
  public static class NoOpMR extends AbstractMapReduce {
    public static final String NAME = "NoOpMR";

    @Override
    protected void configure() {
      setName(NAME);
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(NoOpMapper.class);
      job.setReducerClass(NoOpReducer.class);
      context.addInput(Input.ofDataset(DATASET_NAME3));
      context.addOutput(Output.ofDataset(DATASET_NAME));
    }
  }

  /**
   * Similar to {@link NoOpMR}.
   */
  public static class NoOpMR2 extends AbstractMapReduce {
    public static final String NAME = "NoOpMR2";

    @Override
    protected void configure() {
      setName(NAME);
    }

    @Override
    public void initialize() {
      MapReduceContext context = getContext();
      context.addInput(Input.ofDataset(DATASET_NAME2));
      context.addOutput(Output.ofDataset(DATASET_NAME));
    }
  }

  /**
   *
   */
  public static class NoOpMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {
    @Override
    protected void map(LongWritable key, BytesWritable value,
                       Context context) throws IOException, InterruptedException {
      Text output = new Text(value.copyBytes());
      context.write(output, output);
    }
  }

  /**
   *
   */
  public static class NoOpReducer extends Reducer<Text, Text, byte[], byte[]> {
    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {
      for (Text value : values) {
        byte[] bytes = value.copyBytes();
        context.write(bytes, bytes);
      }
    }
  }

  /**
   *
   */
  public static class NoOpSpark extends AbstractSpark {
    public static final String NAME = "NoOpSpark";

    @Override
    protected void configure() {
      setName(NAME);
      setMainClass(NoOpSparkProgram.class);
    }
  }

  /**
   *
   */
  public static class NoOpWorkflow extends AbstractWorkflow {

    public static final String NAME = "NoOpWorkflow";
    public static final String DESCRIPTION = "NoOp Workflow description";

    @Override
    public void configure() {
      setName(NAME);
      setDescription(DESCRIPTION);
      addAction(new NoOpAction());
      addMapReduce(NoOpMR.NAME);
    }
  }

  /**
   *
   */
  public static class NoOpAction extends AbstractCustomAction {

    @Override
    public void run() {

    }
  }

  /**
   *
   */
  public static class NoOpWorker extends AbstractWorker {

    public static final String NAME = "NoOpWorker";

    @Override
    public void configure() {
      setName(NAME);
    }

    @Override
    public void run() {
      try {
        getContext().execute(context -> {
          KeyValueTable table = context.getDataset(DATASET_NAME);
          table.write("NOOP", "NOOP");
        });
        makeServiceCall();
      } catch (Exception e) {
        LOG.error("Worker ran into error", e);
      }
    }

    private void makeServiceCall() throws IOException {
      URL serviceURL = getContext().getServiceURL(NoOpService.NAME);
      if (serviceURL != null) {
        URL endpoint = new URL(serviceURL + NoOpService.ENDPOINT);
        LOG.info("Calling service endpoint {}", endpoint);
        URLConnection urlConnection = endpoint.openConnection();
        urlConnection.connect();
        try (InputStream inputStream = urlConnection.getInputStream()) {
          ByteStreams.toByteArray(inputStream);
        }
      }
    }
  }

  /**
   *
   */
  public static class NoOpService extends AbstractService {

    public static final String NAME = "NoOpService";
    public static final String ENDPOINT = "no-op";

    @Override
    protected void configure() {
      addHandler(new NoOpHandler());
    }

    /**
     *
     */
    public class NoOpHandler extends AbstractHttpServiceHandler {

      @UseDataSet(DATASET_NAME)
      private KeyValueTable table;

      @Path(ENDPOINT)
      @GET
      public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
        LOG.info("Endpoint {} called in service {}", ENDPOINT, NAME);
        table = getContext().getDataset(DATASET_NAME);
        table.write("no-op-service", "no-op-service");
        responder.sendStatus(200);
      }
    }
  }

  /**
   *
   */
  public static class PConfig extends PluginConfig {
    private double y;
  }

  /**
   *
   */
  @Plugin(type = PLUGIN_TYPE)
  @Name(PLUGIN_NAME)
  @Description(PLUGIN_DESCRIPTION)
  public static class AppPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }
}
