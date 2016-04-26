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
import com.google.common.base.Charsets;

import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Spark Streaming Example for classifying kafka messages through Spark MlLib NaiveBayesModel
 */
public class SpamClassifier extends AbstractApplication {

  static final String SERVICE_HANDLER = "MessageClassification";
  public static final String STREAM = "trainingDataStream";
  public static final String DATASET = "messageClassificationStore";

  @Override
  public void configure() {
    setName("SpamClassifier");
    setDescription("A Spark Streaming Example for Kafka Message Classification");
    addStream(new Stream(STREAM));
    addSpark(new SpamClassifierProgram());
    addService(SERVICE_HANDLER, new SpamClassifierServiceHandler());

    // Store for message classification status
    try {
      ObjectStores.createObjectStore(getConfigurer(), DATASET, Double.class,
                                     DatasetProperties.builder().setDescription("Kafka Message Spam " +
                                                                                  "Classification").build());
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because Double is an actual class.
      throw new RuntimeException(e);
    }
  }

  /**
   * A {@link Service} handler to get the classification of kafka messages
   */
  public static final class SpamClassifierServiceHandler extends AbstractHttpServiceHandler {

    static final String CLASSIFICATION_PATH = "classification";
    static final String SPAM = "Spam";
    static final String HAM = "Ham";

    @UseDataSet(DATASET)
    private ObjectStore<Double> messageClassificationStore;

    @Path(CLASSIFICATION_PATH + "/{message-id}")
    @GET
    public void centers(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("message-id") String messageId) {

      Double value = messageClassificationStore.read(Bytes.toBytes(messageId));
      if (value == null) {
        responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                             String.format("No message was found with message id: %s", messageId), Charsets.UTF_8);
      } else if (value == 0.0) { // ham
        responder.sendString(HttpURLConnection.HTTP_OK, HAM, Charsets.UTF_8);
      } else { // spam
        responder.sendString(HttpURLConnection.HTTP_OK, SPAM, Charsets.UTF_8);
      }
    }
  }
}
