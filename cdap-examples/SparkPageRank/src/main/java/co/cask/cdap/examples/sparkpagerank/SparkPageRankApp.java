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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkClientContext;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.spark.SparkConf;

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

  @Override
  public void configure() {
    setName("SparkPageRank");
    setDescription("Spark page rank application to calculate page rank.");

    // Run a Spark program on the acquired data
    addSpark(new PageRankSpark());

    // Service to retrieve process data
    addService(SERVICE_HANDLERS, new SparkPageRankServiceHandler());

    // Store input and processed data in ObjectStore Datasets
    try {
      ObjectStores.createObjectStore(getConfigurer(), "ranks", Integer.class,
                                     DatasetProperties.builder().setDescription("Ranks Dataset").build());
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because String and Double are actual classes.
      throw new RuntimeException(e);
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

    @Override
    public void beforeSubmit(SparkClientContext context) throws Exception {
      context.setSparkConf(new SparkConf().set("spark.driver.extraJavaOptions", "-XX:MaxPermSize=256m"));
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

    @Path(TRANSFORM_PATH + "/{pr}")
    @GET
    public void transform(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("pr") String pr) {
      responder.sendString(String.valueOf((int) (Math.round(Double.parseDouble(pr) * 10))));
    }
  }
}
