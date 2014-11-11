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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Application that calculates page rank of URLs from an input stream.
 */
public class SparkPageRankApp extends AbstractApplication {

  public static final Charset UTF8 = Charset.forName("UTF-8");

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

    // Store input and processed data in ObjectStore Datasets
    try {
      ObjectStores.createObjectStore(getConfigurer(), "ranks", Double.class);
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
    private ObjectStore<Double> ranks;

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
      byte[] url = urlParam.getBytes(UTF8);

      // Get the rank from the ranks dataset.
      Double rank = ranks.read(url);
      if (rank == null) {
        responder.error(ProcedureResponse.Code.NOT_FOUND, "No rank found for " + urlParam);
        return;
      }

      LOG.trace("Key: {}, Data: {}", Arrays.toString(url), rank);

      // Send response in JSON format.
      responder.sendJson(String.valueOf(rank));
    }
  }
}
