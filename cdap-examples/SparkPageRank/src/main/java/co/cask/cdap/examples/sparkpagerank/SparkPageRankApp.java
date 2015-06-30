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

package co.cask.cdap.examples.sparkpagerank;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.data.stream.Stream;
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

  public static final String RANKS_SERVICE_NAME = "RanksService";
  public static final String GOOGLE_TYPE_PR_SERVICE_NAME = "GoogleTypePR";
  public static final String TOTAL_PAGES_PR_SERVICE_NAME = "TotalPagesPR";
  public static final String BACKLINK_URL_STREAM = "backlinkURLStream";

  @Override
  public void configure() {
    setName("SparkPageRank");
    setDescription("Spark page rank application.");

    // Ingest data into the Application via a Stream
    addStream(new Stream(BACKLINK_URL_STREAM));

    // Run a Spark program on the acquired data
    addSpark(new SparkPageRankSpecification());

    // Runs MapReduce program on data emitted by Spark program
    addMapReduce(new RanksCounter());

    // Runs Spark followed by a MapReduce in a Workflow
    addWorkflow(new PageRankWorkflow());

    // Retrieve the processed data using a Service
    addService(RANKS_SERVICE_NAME, new RanksServiceHandler());

    // Service which converts calculated pageranks to Google type page ranks
    addService(GOOGLE_TYPE_PR_SERVICE_NAME, new GoogleTypePRHandler());

    // Service which gives the total number of pages with a given page rank
    addService(TOTAL_PAGES_PR_SERVICE_NAME, new TotalPagesHandler());

    // Store input and processed data in ObjectStore Datasets
    try {
      ObjectStores.createObjectStore(getConfigurer(), "ranks", Integer.class);
      ObjectStores.createObjectStore(getConfigurer(), "rankscount", Integer.class);
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
      setName("PageRankWorkflow");
      setDescription("Runs SparkPageRankProgram followed by RanksCounter MapReduce");
      addSpark("SparkPageRankProgram");
      addMapReduce("RanksCounter");
    }
  }

  /**
   * A Spark program that calculates page rank.
   */
  public static final class SparkPageRankSpecification extends AbstractSpark {

    @Override
    public void configure() {
      setName(SparkPageRankProgram.class.getSimpleName());
      setDescription("Spark Page Rank Program");
      setMainClass(SparkPageRankProgram.class);
    }
  }

  /**
   * A {@link Service} that responds with rank of the URL.
   */
  public static final class RanksServiceHandler extends AbstractHttpServiceHandler {

    private static final Gson GSON = new Gson();
    public static final String URL_KEY = "url";
    public static final String RANKS_SERVICE_PATH = "rank";

    @UseDataSet("ranks")
    private ObjectStore<Integer> ranks;

    @Path(RANKS_SERVICE_PATH)
    @POST
    public void getRank(HttpServiceRequest request, HttpServiceResponder responder) {
      String urlRequest = Charsets.UTF_8.decode(request.getContent()).toString();
      if (urlRequest == null) {
        responder.sendString(HttpURLConnection.HTTP_BAD_REQUEST,
                             String.format("Please provide an url to query for its rank in JSON."), Charsets.UTF_8);
        return;
      }

      String url = GSON.fromJson(urlRequest, JsonObject.class).get(URL_KEY).getAsString();
      if (url == null) {
        responder.sendString(HttpURLConnection.HTTP_BAD_REQUEST,
                             String.format("The url must be specified with \"url\" as key in JSON."), Charsets.UTF_8);
        return;
      }

      // Get the rank from the ranks dataset
      Integer rank = ranks.read(url.getBytes(Charsets.UTF_8));
      if (rank == null) {
        responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                             String.format("No rank found of %s", url), Charsets.UTF_8);
      } else {
        responder.sendString(HttpURLConnection.HTTP_OK, rank.toString(), Charsets.UTF_8);
      }
    }
  }

  /**
   * A {@link Service} which converts the page rank to a Google Type page rank (from 0 to 10).
   */
  public static final class GoogleTypePRHandler extends AbstractHttpServiceHandler {

    @Path("transform/{pr}")
    @GET
    public void transform(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("pr") String pr) {
      responder.sendString(String.valueOf((int) (Math.round(Double.parseDouble(pr) * 10))));
    }
  }

  /**
   * Total Pages Handler which gives the total number of a pages which has then given page rank
   */
  public static final class TotalPagesHandler extends AbstractHttpServiceHandler {

    public static final String TOTAL_PAGES_PATH = "total";

    @UseDataSet("rankscount")
    private ObjectStore<Integer> store;

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
  }

  /**
   * MapReduce job which counts the total number of pages for every unique page rank
   */
  public static class RanksCounter extends AbstractMapReduce {

    @Override
    public void configure() {
      setName("RanksCounter");
      setInputDataset("ranks");
      setOutputDataset("rankscount");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(Emitter.class);
      job.setReducerClass(Counter.class);
      job.setNumReduceTasks(1);
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
