/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.examples.sparkstreaming;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
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
import com.google.common.base.Charsets;

import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Spark Streaming Example for classifying kafka messages through Spark MlLib NaiveBayesModel
 */
public class SparkSpamFiltering extends AbstractApplication {

  public static final String SERVICE_HANDLER = "SpamChecker";
  public static final String STREAM = "trainingDataStream";
  public static final String DATASET = "messageClassificationStore";

  @Override
  public void configure() {
    setName("SparkSpamFiltering");
    setDescription("A Spark Streaming Example for Kafka Message Classification");
    addStream(new Stream(STREAM));
    addSpark(new SparkSpamFilteringSpecification());
    addService(SERVICE_HANDLER, new SparkSpamFilteringServiceHandler());

    // Store for message classification status
    try {
      ObjectStores.createObjectStore(getConfigurer(), DATASET, Double.class,
                                     DatasetProperties.builder().setDescription("Kafka Message " +
                                                                                  "Classification").build());
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because Double is actual classes.
      throw new RuntimeException(e);
    }
  }

  /**
   * A Spark Streaming Program which classifies kafka messages
   */
  public static final class SparkSpamFilteringSpecification extends AbstractSpark {
    @Override
    public void configure() {
      setName("SparkSpamFilteringProgram");
      setDescription("Spark Streaming Kafka Message Classification Program");
      setMainClass(SparkSpamFilteringProgram.class);
    }
  }

  /**
   * A {@link Service} with handlers to get the kafka message classification
   */
  public static final class SparkSpamFilteringServiceHandler extends AbstractHttpServiceHandler {

    public static final String CLASSIFICATION_PATH = "get";

    @UseDataSet(DATASET)
    private ObjectStore<Double> messageClassificationStore;

    @Path(CLASSIFICATION_PATH + "/{message-key}")
    @GET
    public void centers(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("message-key") String
      messageKey) {

      Double value = messageClassificationStore.read(Bytes.toBytes(messageKey));
      if (value == null) {
        responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                             String.format("No message was classified with message key: %s", messageKey),
                             Charsets.UTF_8);
      } else if (value == 0.0) {
        responder.sendString(HttpURLConnection.HTTP_OK, "Not spam", Charsets.UTF_8);
      } else {
        responder.sendString(HttpURLConnection.HTTP_OK, "Spam", Charsets.UTF_8);
      }
    }
  }
}
