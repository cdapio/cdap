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
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import com.google.common.base.Charsets;

import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * An Application that calculates page rank of URLs from an input stream.
 */
public class SparkPageRankApp extends AbstractApplication {

  public static final String RANKS_SERVICE_NAME = "RanksService";
  public static final String GOOGLE_TYPE_PR_SERVICE_NAME = "GoogleTypePR";

  @Override
  public void configure() {
    setName("SparkPageRank");
    setDescription("Spark page rank application.");

    // Ingest data into the Application via a Stream
    addStream(new Stream("backlinkURLStream"));

    // Run a Spark program on the acquired data
    addSpark(new SparkPageRankSpecification());

    // Retrieve the processed data using a Service
    addService(RANKS_SERVICE_NAME, new RanksServiceHandler());

    // Service which converts calculated pageranks to Google type page ranks
    addService(GOOGLE_TYPE_PR_SERVICE_NAME, new GoogleTypePRHandler());

    // Store input and processed data in ObjectStore Datasets
    try {
      ObjectStores.createObjectStore(getConfigurer(), "ranks", Integer.class);
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
  public static final class SparkPageRankSpecification extends AbstractSpark {

    @Override
    public void configure() {
      setName("SparkPageRankProgram");
      setDescription("Spark Page Rank Program");
      setMainClass(SparkPageRankProgram.class);
    }
  }

  /**
   * A {@link Service} that responds with rank of the URL.
   */
  public static final class RanksServiceHandler extends AbstractHttpServiceHandler {

    @UseDataSet("ranks")
    private ObjectStore<Integer> ranks;

    @Path("rank")
    @GET
    public void getRank(HttpServiceRequest request, HttpServiceResponder responder, @QueryParam("url") String url) {
      if (url == null) {
        responder.sendString(HttpURLConnection.HTTP_BAD_REQUEST,
                             String.format("The url parameter must be specified"), Charsets.UTF_8);
        return;
      }

      // Get the rank from the ranks dataset
      Integer rank = ranks.read(url.getBytes(Charsets.UTF_8));
      if (rank == null) {
        responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                             String.format("No rank found of %s", url), Charsets.UTF_8);
      } else {
        responder.sendString(rank.toString());
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
}
