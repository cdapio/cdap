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

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Application that calculates page rank of URLs from an input stream.
 */
public class SparkPageRankApp extends AbstractApplication {

  static final String SERVICE_NAME = "GoogleTypePR";

  @Override
  public void configure() {
    setName("SparkPageRank");
    setDescription("Spark page rank app");

    // Ingest data into the Application via a Stream
    addStream(new Stream("backlinkURLStream"));

    // Run a Spark program on the acquired data
    addSpark(new SparkPageRankSpecification());

    // Query the processed data using a Procedure
    addProcedure(new RanksProcedure());

    // Service which converts calculated pageranks to Google type page ranks
    addService(SERVICE_NAME, new GoogleTypePRHandler());

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
  public static class SparkPageRankSpecification extends AbstractSpark {
    @Override
    public SparkSpecification configure() {
      return SparkSpecification.Builder.with()
        .setName("SparkPageRankProgram")
        .setDescription("Spark Page Rank Program")
        .setMainClassName(SparkPageRankProgram.class.getName())
        .build();
    }
  }

  /**
   * A Procedure that returns rank of the URL.
   */
  public static class RanksProcedure extends AbstractProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(RanksProcedure.class);

    // Annotation indicates that ranks dataset is used in the Procedure.
    @UseDataSet("ranks")
    private ObjectStore<Integer> ranks;

    @Handle("rank")
    public void getRank(ProcedureRequest request, ProcedureResponder responder)
      throws IOException, InterruptedException {

      // Get the url from the query parameters.
      String urlParam = "";
      for (String key : request.getArguments().keySet()) {
        if (key.equalsIgnoreCase("url")) {
          urlParam = request.getArgument(key);
        }
      }
      if (urlParam.isEmpty()) {
        responder.error(ProcedureResponse.Code.CLIENT_ERROR, "URL must be given as argument");
      }
      byte[] url = urlParam.getBytes(Charsets.UTF_8);

      // Get the rank from the ranks dataset.
      Integer rank = ranks.read(url);
      if (rank == null) {
        responder.error(ProcedureResponse.Code.NOT_FOUND, "No rank found for " + urlParam);
        return;
      }

      LOG.trace("Key: {}, Data: {}", Arrays.toString(url), rank);

      // Send response in JSON format.
      responder.sendJson(String.valueOf(rank));
    }
  }

  /**
   * A {@link Service} which converts the page rank to a Google Type page rank (from 0 to 10)
   */
  public class GoogleTypePRHandler extends AbstractHttpServiceHandler {
    @Path("transform/{pr}")
    @GET
    public void transform(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("pr") String pr) {
      if (pr.isEmpty()) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "pagerank is empty");
      } else {
        responder.sendString(String.valueOf((int) (Math.round(Double.parseDouble(pr) * 10))));
      }
    }
  }
}
