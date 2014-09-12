/*
 * Copyright 2014 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.UUID;

/**
 * Application that calculates page rank of URLs from an input stream.
 */
public class SparkPageRankApp extends AbstractApplication {

  private static final Charset UTF8 = Charset.forName("UTF-8");

  @Override
  public void configure() {
    setName("SparkPageRank");
    setDescription("Spark page rank app");
    addStream(new Stream("neighborUrlStream"));
    addFlow(new PageRankFlow());
    addSpark(new SparkPageRankJob());
    addProcedure(new RanksProcedure());

    try {
      ObjectStores.createObjectStore(getConfigurer(), "neighborURLs", String.class);
      ObjectStores.createObjectStore(getConfigurer(), "ranks", Double.class);
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because String are actual classes.
      throw new RuntimeException(e);
    }
  }

  /**
   * A Spark job that calculates page rank.
   */
  public static class SparkPageRankJob extends AbstractSpark {
    @Override
    public SparkSpecification configure() {
      return SparkSpecification.Builder.with()
        .setName("SparkPageRankJob")
        .setDescription("Spark Page Rank Job")
        .setMainClassName(SparkPageRankJobBuilder.class.getName())
        .build();
    }
  }

  /**
   * This is a simple Flow that consumes url pair events from a Stream and stores them in a dataset.
   */
  public static class PageRankFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("PageRankFlow")
        .setDescription("Reads url pair and stores in dataset")
        .withFlowlets()
        .add("reader", new NeighborURLsReader())
        .connect()
        .fromStream("neighborUrlStream").to("reader")
        .build();
    }
  }

  /**
   * This Flowlet reads events from a Stream and saves them to a datastore.
   */
  public static class NeighborURLsReader extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(NeighborURLsReader.class);

    // Annotation indicates that neighborURLs dataset is used in the Flowlet.
    @UseDataSet("neighborURLs")
    private ObjectStore<String> neighborStore;

    /**
     * Input file should be in format of:
     * URL neighbor URL
     * URL neighbor URL
     * URL neighbor URL
     * ...
     * where URL and their neighbors are separated by space(s).
     */
    @ProcessInput
    public void process(StreamEvent event) {
      String body = new String(event.getBody().array());
      LOG.trace("Neighbor info: {}", body);
      // Store the URL pairs in one row. One pair is kept in the value of an table entry.
      neighborStore.write(getIdAsByte(UUID.randomUUID()), body);
    }

    private static byte[] getIdAsByte(UUID uuid) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
    }
  }

  /**
   * A Procedure that returns rank of the url.
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
      byte[] url = request.getArgument("url").getBytes(UTF8);
      // Get the rank from the ranks data set.
      Double rank = ranks.read(url);

      LOG.trace("Key: {}, Data: {}", Arrays.toString(url), rank);

      // Send response with JSON format.
      responder.sendJson(String.valueOf(rank));
    }
  }
}
