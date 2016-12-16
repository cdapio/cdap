/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowInfo;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tephra.TransactionFailureException;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 *
 */
public class ClusterNameTestApp extends AbstractApplication {

  public static final String CLUSTER_NAME_TABLE = "ClusterNameTable";
  public static final String INPUT_FILE_SET = "InputFileSet";
  public static final String OUTPUT_FILE_SET = "OutputFileSet";

  @Override
  public void configure() {
    addService(ClusterNameServiceHandler.class.getSimpleName(), new ClusterNameServiceHandler());
    addWorker(new ClusterNameWorker());
    addFlow(new ClusterNameFlow());
    addMapReduce(new ClusterNameMapReduce());
    addSpark(new ClusterNameSpark());
    addWorkflow(new ClusterNameWorkflow());

    createDataset(CLUSTER_NAME_TABLE, KeyValueTable.class);
  }

  /**
   * Service handler for testing cluster name.
   */
  public static final class ClusterNameServiceHandler extends AbstractHttpServiceHandler {

    @GET
    @Path("/clusterName")
    public void getName(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString(getContext().getClusterName());
    }
  }

  /**
   * Worker for testing cluster name.
   */
  public static final class ClusterNameWorker extends AbstractWorker {

    @Override
    public void run() {
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            KeyValueTable clusterNameTable = context.getDataset(CLUSTER_NAME_TABLE);
            clusterNameTable.write("worker.cluster.name", getContext().getClusterName());
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Flow for testing cluster name.
   */
  public static final class ClusterNameFlow extends AbstractFlow {

    @Override
    protected void configure() {
      addFlowlet(new ClusterNameFlowlet());
      addFlowlet(new WriterFlowlet());
      connect("ClusterNameFlowlet", "WriterFlowlet");
    }

    /**
     * Flowlet for testing cluster name.
     */
    public static final class ClusterNameFlowlet extends AbstractFlowlet {

      private OutputEmitter<String> emitter;

      @Tick(delay = 1)
      public void generate() {
        emitter.emit(getContext().getClusterName());
      }
    }

    /**
     * Writer flowlet to write cluster name.
     */
    public static final class WriterFlowlet extends AbstractFlowlet {

      @UseDataSet(CLUSTER_NAME_TABLE)
      private KeyValueTable clusterNameTable;

      @ProcessInput
      public void process(String clusterName) {
        clusterNameTable.write("flow.cluster.name", clusterName);
      }
    }
  }

  /**
   * MapReduce for testing cluster name.
   */
  public static final class ClusterNameMapReduce extends AbstractMapReduce {

    @UseDataSet(CLUSTER_NAME_TABLE)
    private KeyValueTable clusterNameTable;

    @Override
    protected void configure() {
      createDataset(INPUT_FILE_SET, FileSet.class, FileSetProperties.builder()
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class).build());
      createDataset(OUTPUT_FILE_SET, FileSet.class, FileSetProperties.builder()
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class).build());
    }

    @Override
    protected void initialize() throws Exception {
      Job job = getContext().getHadoopJob();
      job.setMapperClass(ClusterNameMapper.class);
      job.setReducerClass(ClusterNameReducer.class);
      getContext().addInput(Input.ofDataset(INPUT_FILE_SET));
      getContext().addOutput(Output.ofDataset(OUTPUT_FILE_SET));

      WorkflowInfo workflowInfo = getContext().getWorkflowInfo();
      String prefix = workflowInfo == null ? "" : workflowInfo.getName() + ".";
      clusterNameTable.write(prefix + "mr.client.cluster.name", getContext().getClusterName());
    }

    /**
     * Mapper for cluster name test.
     */
    public static final class ClusterNameMapper extends Mapper<LongWritable, Text, Text, LongWritable>
                                                implements ProgramLifecycle<MapReduceTaskContext<Text, LongWritable>> {
      @UseDataSet(CLUSTER_NAME_TABLE)
      private KeyValueTable clusterNameTable;
      private String clusterName;
      private String prefix;

      @Override
      public void initialize(MapReduceTaskContext<Text, LongWritable> context) throws Exception {
        clusterName = context.getClusterName();
        WorkflowInfo workflowInfo = context.getWorkflowInfo();
        prefix = workflowInfo == null ? "" : workflowInfo.getName() + ".";
      }

      @Override
      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text output = new Text();
        LongWritable one = new LongWritable(1L);
        for (String word : value.toString().split("\\w+")) {
          output.set(word);
          context.write(output, one);
        }

        clusterNameTable.write(prefix + "mapper.cluster.name", clusterName);
      }

      @Override
      public void destroy() {
        // no-op
      }
    }

    /**
     * Reducer for cluster name test
     */
    public static final class ClusterNameReducer extends Reducer<Text, LongWritable, Text, LongWritable>
                                                 implements ProgramLifecycle<MapReduceTaskContext<Text, LongWritable>> {

      @UseDataSet(CLUSTER_NAME_TABLE)
      private KeyValueTable clusterNameTable;
      private String clusterName;
      private String prefix;

      @Override
      public void initialize(MapReduceTaskContext<Text, LongWritable> context) throws Exception {
        clusterName = context.getClusterName();
        WorkflowInfo workflowInfo = context.getWorkflowInfo();
        prefix = workflowInfo == null ? "" : workflowInfo.getName() + ".";
      }

      @Override
      protected void reduce(Text key, Iterable<LongWritable> values,
                            Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
          sum += value.get();
        }
        context.write(key, new LongWritable(sum));
        clusterNameTable.write(prefix + "reducer.cluster.name", clusterName);
      }

      @Override
      public void destroy() {
        // no-op
      }
    }
  }

  /**
   * Spark for testing cluster name.
   */
  public static final class ClusterNameSpark extends AbstractSpark implements JavaSparkMain {

    @Override
    protected void configure() {
      setMainClass(ClusterNameSpark.class);
    }

    @Override
    public void run(final JavaSparkExecutionContext sec) throws Exception {
      sec.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          KeyValueTable clusterNameTable = context.getDataset(CLUSTER_NAME_TABLE);
          WorkflowInfo workflowInfo = sec.getWorkflowInfo();
          String prefix = workflowInfo == null ? "" : workflowInfo.getName() + ".";

          clusterNameTable.write(prefix + "spark.cluster.name", sec.getClusterName());
        }
      });
    }
  }

  /**
   * Workflow for testing cluster name.
   */
  public static final class ClusterNameWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      fork()
        .addMapReduce(ClusterNameMapReduce.class.getSimpleName())
        .addSpark(ClusterNameSpark.class.getSimpleName())
        .addAction(new ClusterNameAction())
        .join();
    }

    /**
     * Action for testing cluster name.
     */
    public static final class ClusterNameAction extends AbstractCustomAction {

      @Override
      public void run() throws Exception {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            KeyValueTable clusterNameTable = context.getDataset(CLUSTER_NAME_TABLE);
            WorkflowInfo workflowInfo = getContext().getWorkflowInfo();
            String prefix = workflowInfo == null ? "" : workflowInfo.getName() + ".";
            clusterNameTable.write(prefix + "action.cluster.name", getContext().getClusterName());
          }
        });
      }
    }
  }
}
