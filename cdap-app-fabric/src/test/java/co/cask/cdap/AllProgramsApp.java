/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * App that contains all program types. Used to test Metadata store.
 */
public class AllProgramsApp extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(AllProgramsApp.class);

  public static final String NAME = "App";
  public static final String STREAM_NAME = "stream";
  public static final String DATASET_NAME = "kvt";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Application which has everything");
    addStream(new Stream(STREAM_NAME));
    createDataset(DATASET_NAME, KeyValueTable.class);
    addFlow(new NoOpFlow());
    addMapReduce(new NoOpMR());
    addWorkflow(new NoOpWorkflow());
    addWorker(new NoOpWorker());
    addSpark(new NoOpSpark());
    addService(new NoOpService());
  }

  /**
   *
   */
  public static class NoOpFlow implements Flow {

    public static final String NAME = "NoOpFlow";

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName(NAME)
        .setDescription("NoOpflow")
        .withFlowlets()
          .add(A.NAME, new A())
        .connect()
          .fromStream(STREAM_NAME).to(A.NAME)
        .build();
    }
  }

  /**
   *
   */
  public static final class A extends AbstractFlowlet {

    @UseDataSet(DATASET_NAME)
    private KeyValueTable store;

    public static final String NAME = "A";

    public A() {
      super(NAME);
    }

    @ProcessInput
    public void process(StreamEvent event) {
      // NO-OP
    }
  }

  /**
   *
   */
  public static class NoOpMR extends AbstractMapReduce {
    public static final String NAME = "NoOpMR";

    @Override
    protected void configure() {
      setName(NAME);
      useStreamInput(STREAM_NAME);
      setOutputDataset(DATASET_NAME);
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(NoOpMapper.class);
      job.setReducerClass(NoOpReducer.class);
    }
  }

  public static class NoOpMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {
    @Override
    protected void map(LongWritable key, BytesWritable value,
                       Context context) throws IOException, InterruptedException {
      Text output = new Text(value.copyBytes());
      context.write(output, output);
    }
  }

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
  public static class NoOpSparkProgram implements JavaSparkProgram {
    @Override
    public void run(SparkContext context) {
      JavaPairRDD<LongWritable, String> streamRDD = context.readFromStream(STREAM_NAME, String.class);
      LOG.info("Stream events: {}", streamRDD.count());

      JavaPairRDD<byte[], byte[]> datasetRDD = context.readFromDataset(DATASET_NAME, byte[].class, byte[].class);
      LOG.info("Dataset pairs: {}", datasetRDD.count());
    }
  }

  /**
   *
   */
  public static class NoOpWorkflow extends AbstractWorkflow {

    public static final String NAME = "NoOpWorkflow";

    @Override
    public void configure() {
        setName(NAME);
        setDescription("NoOp Workflow description");
        addAction(new NoOpAction());
        addMapReduce(NoOpMR.NAME);
    }
  }

  /**
   *
   */
  public static class NoOpAction extends AbstractWorkflowAction {

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
        getContext().write(STREAM_NAME, ByteBuffer.wrap(Bytes.toBytes("NO-OP")));
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            KeyValueTable table = context.getDataset(DATASET_NAME);
            table.write("NOOP", "NOOP");
          }
        });
        makeServiceCall();
      } catch (Exception e) {
        LOG.error("Worker ran into error", e);
      }
    }

    private void makeServiceCall() throws IOException {
      URL serviceURL = getContext().getServiceURL(NoOpService.NAME);
      if (serviceURL != null) {
        URL endpoint = new URL(serviceURL.toString() + NoOpService.ENDPOINT);
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

    public class NoOpHandler extends AbstractHttpServiceHandler {
      @Override
      protected void configure() {
        useDatasets(DATASET_NAME);
      }

      @Path(ENDPOINT)
      @GET
      public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
        LOG.info("Endpoint {} called in service {}", ENDPOINT, NAME);
        KeyValueTable table = getContext().getDataset(DATASET_NAME);
        table.write("no-op-service", "no-op-service");
        responder.sendStatus(200);
      }
    }
  }
}
