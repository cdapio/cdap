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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.worker.AbstractWorker;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;

/**
 * App to test authorization in programs.
 */
public class StreamAuthApp extends AbstractApplication {

  public static final String APP = "StreamWithMRApp";
  public static final String STREAM = "inputStream";
  public static final String STREAM2 = "inputStream2";
  public static final String MAPREDUCE = "MRCopy";
  public static final String KVTABLE = "kvtable";
  public static final String WORKER = "StreamWriter";
  public static final String FLOW = "FetchFlow";
  public static final String SPARK = "SparkCopy";

  @Override
  public void configure() {
    setName(APP);
    setDescription("Copy Data from Stream to KVTable.");
    addStream(STREAM);
    addStream(STREAM2);
    createDataset(KVTABLE, KeyValueTable.class);
    addMapReduce(new CopyMapReduce());
    addWorker(new StreamWriter());
    addFlow(new FetchFlow());
    addSpark(new SparkCopyProgramSpec());
  }

  public static class SparkCopyProgramSpec extends AbstractSpark {
    @Override
    protected void configure() {
      setName(SPARK);
      setMainClass(SparkCopyProgram.class);
    }
  }

  public static class SparkCopyProgram implements JavaSparkMain {
    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      JavaSparkContext jsc = new JavaSparkContext();
      JavaPairRDD<Long, String> rdd = sec.fromStream(STREAM, String.class);
      JavaPairRDD<byte[], byte[]> resultRDD = rdd.mapToPair(new PairFunction<Tuple2<Long, String>, byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(Tuple2<Long, String> tuple2) throws Exception {
          return new Tuple2<>(Bytes.toBytes(tuple2._2()), Bytes.toBytes(tuple2._2()));
        }
      });
      sec.saveAsDataset(resultRDD, KVTABLE);
    }
  }

  public static class FetchFlow extends AbstractFlow {
    @Override
    protected void configure() {
      super.configure();
      setName(FLOW);
      addFlowlet(new StreamFetchFlowlet());
      connectStream(STREAM, new StreamFetchFlowlet());
      connectStream(STREAM2, new StreamFetchFlowlet());
    }
  }

  public static class StreamFetchFlowlet extends AbstractFlowlet {

    @UseDataSet(KVTABLE)
    KeyValueTable kvTable;

    @ProcessInput(maxRetries = 3)
    public void process(StreamEvent event) {
      String body = Bytes.toString(event.getBody());
      kvTable.write(body, body);
    }
  }

  public static class CopyMapReduce extends AbstractMapReduce {

    @Override
    public void configure() {
      setName(MAPREDUCE);
    }

    @Override
    public void initialize() {
      MapReduceContext context = getContext();
      context.addInput(Input.ofStream(STREAM));
      context.addOutput(Output.ofDataset(KVTABLE));
      Job hadoopJob = context.getHadoopJob();
      hadoopJob.setMapperClass(IdentityMapper.class);
      hadoopJob.setNumReduceTasks(0);
    }

    public static class IdentityMapper extends Mapper<LongWritable, String, byte[], byte[]> {

      public void map(LongWritable ts, String event, Context context) throws IOException, InterruptedException {
        context.write(Bytes.toBytes(event), Bytes.toBytes(event));
      }
    }
  }

  public static class StreamWriter extends AbstractWorker {
    public static final Logger LOG = LoggerFactory.getLogger(StreamWriter.class);

    @Override
    protected void configure() {
      super.configure();
      setName(WORKER);
    }

    @Override
    public void run() {
      for (int i = 0; i < 5; i++) {
        try {
          getContext().write(STREAM, String.format("Hello%d", i));
        } catch (IOException e) {
          LOG.debug("Error writing to Stream {}", STREAM, e);
        }
      }
    }
  }
}
