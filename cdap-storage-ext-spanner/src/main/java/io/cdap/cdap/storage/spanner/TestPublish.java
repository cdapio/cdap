/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestPublish {
  public static void main(String[] args) throws IOException, InterruptedException {
    String projectId = "ardekani-cdf-sandbox2";
    String topicId = "petertesttopic";
    publishWithErrorHandlerExample(projectId, topicId);
  }

  public static void publishWithErrorHandlerExample(String projectId, String topicId)
      throws IOException, InterruptedException {
    TopicName topicName = TopicName.of(projectId, topicId);
    Publisher publisher = null;

    // Create a publisher instance with default settings bound to the topic
    publisher = Publisher.newBuilder(topicName).build();
    int size = 1024 * 1024 ;
    byte[] ar = new byte[size];
    long start = System.currentTimeMillis();
    List<ApiFuture> futures = new ArrayList<>();
    try {
      for (int j = 0; j < 10; j++) {
        futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
          ByteString data = ByteString.copyFrom(ar);
          PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

          // Once published, returns a server-assigned message id (unique within the topic)
          ApiFuture<String> future = publisher.publish(pubsubMessage);
          futures.add(future);

          // Add an asynchronous callback to handle success / failure
          ApiFutures.addCallback(
              future,
              new ApiFutureCallback<String>() {

                @Override
                public void onFailure(Throwable throwable) {
                  if (throwable instanceof ApiException) {
                    ApiException apiException = ((ApiException) throwable);
                    // details on the API exception
                    System.out.println(apiException.getStatusCode().getCode());
                    System.out.println(apiException.isRetryable());
                  }
                  System.out.println("Error publishing message ");
                }

                @Override
                public void onSuccess(String messageId) {
                  // Once published, returns server-assigned message ids (unique within the topic)
                  System.out.println("Published message ID: " + messageId);
                }
              },
              MoreExecutors.directExecutor());
        }

        for (ApiFuture f : futures) {
          try {
            f.get();
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        }
      }
      long duration = System.currentTimeMillis() - start;
      System.out.println("Took " + duration);

    } finally {
      if (publisher != null) {
        // When finished with the publisher, shutdown to free up resources.
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
      }
    }
  }
}
