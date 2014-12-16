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

package co.cask.cdap.examples.sparkkmeans;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Application that demonstrate KMeans Clustering example.
 */
public class SparkKMeansApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("SparkKMeans");
    setDescription("Spark KMeans app");

    // Ingest data into the Application via a Stream
    addStream(new Stream("pointsStream"));

    // Process points data in real-time using a Flow
    addFlow(new PointsFlow());

    // Run a Spark program on the acquired data
    addSpark(new SparkKMeansSpecification());

    // Retrieve the processed data using a Service
    addService(new CentersService());

    // Store input and processed data in ObjectStore Datasets
    try {
      ObjectStores.createObjectStore(getConfigurer(), "points", String.class);
      ObjectStores.createObjectStore(getConfigurer(), "centers", String.class);
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because String is an actual class.
      throw new RuntimeException(e);
    }
  }

  /**
   * A Spark Program that uses KMeans algorithm.
   */
  public static final class SparkKMeansSpecification extends AbstractSpark {

    @Override
    public void configure() {
      setName("SparkKMeansProgram");
      setDescription("Spark KMeans Program");
      setMainClass(SparkKMeansProgram.class);
    }
  }

  /**
   * This Flowlet reads events from a Stream and saves them to a dataset.
   */
  public static final class PointsReader extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(PointsReader.class);

    @UseDataSet("points")
    private ObjectStore<String> pointsStore;

    @ProcessInput
    public void process(StreamEvent event) {
      String body = Bytes.toString(event.getBody());
      LOG.trace("Points info: {}", body);
      pointsStore.write(getIdAsByte(UUID.randomUUID()), body);
    }

    private static byte[] getIdAsByte(UUID uuid) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
    }
  }

  /**
   * This is a simple Flow that consumes points from a Stream and stores them in a dataset.
   */
  public static final class PointsFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("PointsFlow")
        .setDescription("Reads points information and stores in dataset")
        .withFlowlets()
        .add("reader", new PointsReader())
        .connect()
        .fromStream("pointsStream").to("reader")
        .build();
    }
  }

  /**
   * A {@link Service} that responds with calculated center based on index parameter.
   */
  public static final class CentersService extends AbstractService {

    public static final String SERVICE_NAME = "CentersService";

    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      setDescription("A service that responds with calculated center based on index parameter.");
      addHandler(new CentersServiceHandler());
    }
  }

  /**
   * Centers Service handler.
   */
  public static final class CentersServiceHandler extends AbstractHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CentersService.class);

    @UseDataSet("centers")
    private ObjectStore<String> store;

    @Path("centers/{index}")
    @GET
    public void centers(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("index") String index) {
      LOG.debug("Try to get centers for index: {}", index);

      String centers = store.read(index.getBytes());
      if (centers == null) {
        LOG.debug("No centers found");
        responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                             String.format("No centers found for index: %s", index), Charsets.UTF_8);
      } else {
        LOG.debug("Retrieved centers: {}", centers);
        responder.sendString(centers);
      }
    }
  }
}
