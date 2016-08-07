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

package co.cask.cdap.examples.sparkpagerank;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * An Application that calculates page rank of URLs from an input stream.
 */
public class SparkPageRankApp extends AbstractApplication {

  public static final String SERVICE_HANDLERS = "RanksCounterService";

  @Override
  public void configure() {
    setName("PageRankCounter");
    setDescription("Page rank application.");

    // Runs MapReduce program on data emitted by Spark program
    addMapReduce(new RanksCounter());

    // Service to retrieve process data
    addService(SERVICE_HANDLERS, new SparkPageRankServiceHandler());

    // Store input and processed data in ObjectStore Datasets
    try {
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
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(Emitter.class);
      job.setReducerClass(Counter.class);
      job.setNumReduceTasks(1);
      String datasetNamespace = context.getRuntimeArguments().get("dataset.namespace");
      String datasetName = context.getRuntimeArguments().get("dataset.name");
      context.addInput(Input.ofDataset(datasetName).fromNamespace(datasetNamespace));
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
