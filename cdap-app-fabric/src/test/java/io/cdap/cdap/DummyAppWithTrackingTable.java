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

package io.cdap.cdap;

import com.google.common.base.Charsets;
import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.service.BasicService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

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
    createDataset("foo", TrackingTable.class);
    createDataset("bar", TrackingTable.class);
    addMapReduce(new DummyBatch());
    addService(new BasicService("DummyService", new DummyHandler()));
  }

  /**
   * A handler.
   */
  public static class DummyHandler extends AbstractHttpServiceHandler {

    @UseDataSet("foo")
    TrackingTable table;

    @GET
    @Path("{key}")
    public void handle(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("key") String key) {
      byte[] value = table.read(Bytes.toBytes(key));
      responder.sendJson(Bytes.toString(value));
    }

    @PUT
    @Path("{key}")
    public void put(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("key") String key) {
      table.write(Bytes.toBytes(key), Bytes.toBytes(key));
      responder.sendStatus(200);
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
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(DummyMapper.class);
      job.setReducerClass(DummyReducer.class);
      context.addInput(Input.ofDataset("foo"));
      context.addOutput(Output.ofDataset("bar"));
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
