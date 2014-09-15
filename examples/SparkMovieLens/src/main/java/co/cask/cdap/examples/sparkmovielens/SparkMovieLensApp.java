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

package co.cask.cdap.examples.sparkmovielens;

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
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.UUID;

/**
 * Application that demonstrate ALS on MovieLens data.
 */
public class SparkMovieLensApp extends AbstractApplication {

  public static final Charset UTF8 = Charset.forName("UTF-8");

  @Override
  public void configure() {
    setName("SparkMovieLens");
    setDescription("Spark MovieLens app");
    addStream(new Stream("ratingsStream"));
    addFlow(new RatingsFlow());
    addSpark(new SparkMovieLensSpecification());
    addProcedure(new ALSProcedure());

    try {
      ObjectStores.createObjectStore(getConfigurer(), "ratings", String.class);
      ObjectStores.createObjectStore(getConfigurer(), "out", String.class);
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because String is an actual classes.
      throw new RuntimeException(e);
    }
  }

  /**
   * A Spark Program that uses .... // todo: add description
   */
  public static class SparkMovieLensSpecification extends AbstractSpark {
    @Override
    public SparkSpecification configure() {
      return SparkSpecification.Builder.with()
        .setName("SparkMovieLensProgram")
        .setDescription("Spark MovieLens Program")
        .setMainClassName(SparkMovieLensProgram.class.getName())
        .build();
    }
  }

  /**
   * This Flowlet reads events from a Stream and saves them to a datastore.
   */
  public static class RatingReader extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(RatingReader.class);

    @UseDataSet("ratings")
    private ObjectStore<String> ratingsStore;

    @ProcessInput
    public void process(StreamEvent event) {
      String body = new String(event.getBody().array());
      LOG.trace("Ratings info: {}", body);
      ratingsStore.write(getIdAsByte(UUID.randomUUID()), body);
    }

    private static byte[] getIdAsByte(UUID uuid) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
    }
  }

  /**
   * This is a simple Flow that consumes ratings from a Stream and stores them in a dataset.
   */
  public static class RatingsFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("RatingsFlow")
        .setDescription("Reads ratings information and stores in dataset")
        .withFlowlets()
        .add("reader", new RatingReader())
        .connect()
        .fromStream("ratingsStream").to("reader")
        .build();
    }
  }

  /**
   * // todo: add description
   */
  public static class ALSProcedure extends AbstractProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(ALSProcedure.class);

    // Annotation indicates that als dataset is used in the procedure.
    @UseDataSet("als")
    private ObjectStore<String> als;

    @Handle("als")
    public void getALS(ProcedureRequest request, ProcedureResponder responder)
      throws IOException, InterruptedException {
      String index = request.getArgument("index");
      if (index == null) {
        responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Index must be given as argument");
        return;
      }
      LOG.debug("get als for index {}", index);
      // Send response with JSON format.
      responder.sendJson(als.read(index.getBytes()));
    }
  }
}
