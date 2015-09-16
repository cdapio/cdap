/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test.artifacts;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.worker.AbstractWorker;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Assert;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * App that uses a Plugin.
 */
public class AppWithPlugin extends AbstractApplication {

  private static final String TEST = "this is a test string";
  private static final String KEY = "toString";
  public static final String WORKER = "testWorker";
  public static final String MAPREDUCE = "testMapReduce";
  public static final String SERVICE = "testService";

  @Override
  public void configure() {
    addWorker(new WorkerWithPlugin());
    addMapReduce(new MapReduceWithPlugin());
    addService(new ServiceWithPlugin());
    usePlugin("t1", "n1", "mrid", PluginProperties.builder().add(KEY, TEST).build());
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
      createDataset("output", KeyValueTable.class);
      addStream("input");
      setOutputDataset("output");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      super.beforeSubmit(context);
      Job job = context.getHadoopJob();
      job.setMapperClass(SimpleMapper.class);
      StreamBatchReadable.useStreamInput(context, "input", 0, Long.MAX_VALUE);
      job.setNumReduceTasks(0);
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
        getContext().write("input", "data");
      } catch (InstantiationException e) {
        Assert.fail();
      } catch (IOException e) {
        // writing to stream failed. but doesn't affect test.
      }
    }

    @Override
    protected void configure() {
      setName(WORKER);
      usePlugin("t1", "n1", "plug", PluginProperties.builder().add(KEY, TEST).build());
    }
  }
}
