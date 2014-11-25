/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import com.google.common.base.Charsets;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Simple app for testing data set handling by the program runners.
 * - a flow reading from stream "xx" and writing to dataset "foo"
 * - a query with method get(key) that reads "foo"
 * - a map/reduce job that reads "foo" and writes to another dataset "bar"
 * The datasets are key/value tables that track the number of times each operation
 * (open/close/read/write/getsplits) are called, so the unit test can verify.
 */
@SuppressWarnings("unused")
public class DummyAppWithTrackingTable extends AbstractApplication {

  @Override
  public void configure() {
    setName("dummy");
    setDescription("dummy app with a dataset that tracks open and close");
    addStream(new Stream("xx"));
    createDataset("foo", TrackingTable.class);
    createDataset("bar", TrackingTable.class);
    addFlow(new DummyFlow());
    addProcedure(new DummyProcedure());
    addMapReduce(new DummyBatch());
  }

  /**
   * A flow.
   */
  public static class DummyFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("dummy-flow")
        .setDescription("a dummy flow that does not much")
        .withFlowlets().add("fwlt", new DummyFlowlet())
        .connect().fromStream("xx").to("fwlt")
        .build();
    }
  }

  /**
   * A flowlet.
   */
  public static class DummyFlowlet extends AbstractFlowlet {

    @UseDataSet("foo")
    TrackingTable table;

    @ProcessInput
    public void process(StreamEvent event) {
      byte[] keyAndValue = Bytes.toBytes(event.getBody());
      table.write(keyAndValue, keyAndValue);
    }
  }

  /**
   * A procedure.
   */
  public static class DummyProcedure extends AbstractProcedure {

    @UseDataSet("foo")
    TrackingTable table;

    @Handle("get")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      byte[] key = request.getArgument("key").getBytes(Charsets.UTF_8);
      byte[] value = table.read(key);
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), new String(value, Charsets.UTF_8));
    }
  }

  /**
   * A map/reduce job.
   */
  public static class DummyBatch extends AbstractMapReduce {

    @UseDataSet("foo")
    private TrackingTable table;

    @Override
    public void configure() {
      setName("dummy-batch");
      setDescription("batch job that copies from foo to bar");
      useDatasets("foo");
      setInputDataset("foo");
      setOutputDataset("bar");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(DummyMapper.class);
      job.setReducerClass(DummyReducer.class);
    }
  }

  /**
   * A mapper.
   */
  public static class DummyMapper extends Mapper<byte[], byte[], Text, Text> {

    @UseDataSet("foo")
    TrackingTable table;

    @Override
    protected void map(byte[] key, byte[] value, Context context)
      throws IOException, InterruptedException {
      byte[] val = table.read(key);
      context.write(new Text(key), new Text(val));
    }
  }

  /**
   * A reducer.
   */
  public static class DummyReducer extends Reducer<Text, Text, byte[], byte[]> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      StringBuilder str = new StringBuilder();
      for (Text text : values) {
        str.append(text.toString());
      }
      context.write(key.getBytes(), str.toString().getBytes(Charsets.UTF_8));
    }
  }
}
