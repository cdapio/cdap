/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package $package;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * An Application that calculates page rank of URLs from an input stream.
 */
public class SparkPageRankApp extends AbstractApplication {

  public static final String SERVICE_HANDLERS = "SparkPageRankService";
  public static final String BACKLINK_URL_STREAM = "backlinkURLStream";

  @Override
  public void configure() {
    setName("SparkPageRank");
    setDescription("Spark page rank application.");

    // Ingest data into the Application via a Stream
    addStream(new Stream(BACKLINK_URL_STREAM));

    // Run a Spark program on the acquired data
    addSpark(new PageRankSpark());

    // Runs MapReduce program on data emitted by Spark program
    addMapReduce(new RanksCounter());

    // Runs Spark followed by a MapReduce in a Workflow
    addWorkflow(new PageRankWorkflow());

    // Service to retrieve process data
    addService(SERVICE_HANDLERS, new SparkPageRankServiceHandler());

    // Store input and processed data in ObjectStore Datasets
    try {
      ObjectStores.createObjectStore(getConfigurer(), "ranks", Integer.class,
                                     DatasetProperties.builder().setDescription("Ranks Dataset").build());
      ObjectStores.createObjectStore(getConfigurer(), "rankscount", Integer.class,
                                     DatasetProperties.builder().setDescription("Ranks Count Dataset").build());
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because String and Double are actual classes.
      throw new RuntimeException(e);
    }
  }

  /**
   * PageRankWorkflow which connect a Spark program followed by a MapReduce
   */
  public static class PageRankWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setDescription("Runs PageRankSpark program followed by RanksCounter MapReduce program");
      addSpark(PageRankSpark.class.getSimpleName());
      addMapReduce(RanksCounter.class.getSimpleName());
    }
  }

  /**
   * A Spark program that calculates page rank.
   */
  public static final class PageRankSpark extends AbstractSpark {

    @Override
    public void configure() {
      setDescription("Spark page rank program");
      setMainClass(SparkPageRankProgram.class);
      setDriverResources(new Resources(1024));
      setExecutorResources(new Resources(1024));
    }
  }

  /**
   * A {@link Service} with handlers to get rank of a url, total number of pages for a given rank and transform a page
   * rank on a scale of 1 to 10
   */
  public static final class SparkPageRankServiceHandler extends AbstractHttpServiceHandler {

    private static final Gson GSON = new Gson();
    public static final String URL_KEY = "url";
    public static final String RANKS_PATH = "rank";
    public static final String TOTAL_PAGES_PATH = "total";
    public static final String TRANSFORM_PATH = "transform";

    @UseDataSet("rankscount")
    private ObjectStore<Integer> store;

    @UseDataSet("ranks")
    private ObjectStore<Integer> ranks;

    @Path(RANKS_PATH)
    @POST
    public void getRank(HttpServiceRequest request, HttpServiceResponder responder) {
      String urlRequest = Charsets.UTF_8.decode(request.getContent()).toString();

      String url = GSON.fromJson(urlRequest, JsonObject.class).get(URL_KEY).getAsString();
      if (url == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            "The url must be specified with \"url\" as key in JSON.");
        return;
      }

      // Get the rank from the ranks dataset
      Integer rank = ranks.read(url.getBytes(Charsets.UTF_8));
      if (rank == null) {
        responder.sendError(HttpURLConnection.HTTP_NO_CONTENT, String.format("No rank found of %s", url));
      } else {
        responder.sendString(rank.toString());
      }
    }

    @Path(TOTAL_PAGES_PATH + "/{pr}")
    @GET
    public void centers(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("pr") Integer pageRank) {

      Integer totalPages = store.read(Bytes.toBytes(pageRank));
      if (totalPages == null) {
        responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                             String.format("No pages found with pr: %s", pageRank), Charsets.UTF_8);
      } else {
        responder.sendString(HttpURLConnection.HTTP_OK, totalPages.toString(), Charsets.UTF_8);
      }
    }

    @Path(TRANSFORM_PATH + "/{pr}")
    @GET
    public void transform(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("pr") String pr) {
      responder.sendString(String.valueOf((int) (Math.round(Double.parseDouble(pr) * 10))));
    }
  }

  /**
   * MapReduce job which counts the total number of pages for every unique page rank
   */
  public static class RanksCounter extends AbstractMapReduce {

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(Emitter.class);
      job.setReducerClass(Counter.class);
      job.setNumReduceTasks(1);
      context.addInput(Input.ofDataset("ranks"));
      context.addOutput(Output.ofDataset("rankscount"));
    }

    /**
     * A mapper that emits each url's page rank with a value of 1.
     */
    public static class Emitter extends Mapper<byte[], Integer, IntWritable, IntWritable> {

      private static final IntWritable ONE = new IntWritable(1);

      @Override
      protected void map(byte[] key, Integer value, Context context)
        throws IOException, InterruptedException {
        context.write(new IntWritable(value), ONE);
      }
    }

    /**
     * A reducer that sums up the counts for each key.
     */
    public static class Counter extends Reducer<IntWritable, IntWritable, byte[], Integer> {

      @Override
      public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
          sum += value.get();
        }
        context.write(Bytes.toBytes(key.get()), sum);
      }
    }
  }
}
