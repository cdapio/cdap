/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.clearspring.analytics.util.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

/**
 * Tests v3 stream endpoints with default namespace
 */
public class StreamHandlerTestV3 extends StreamHandlerTest {
  @Override
  protected URL createURL(String path) throws URISyntaxException, MalformedURLException {
    return createURL(Constants.DEFAULT_NAMESPACE, path);
  }

  private URL createURL(String namespace, String path) throws URISyntaxException, MalformedURLException {
    return getEndPoint(String.format("/v3/namespaces/%s/%s", namespace, path)).toURL();
  }

  private void createStream(Id.Stream streamId) throws Exception {
    URL url = createURL(streamId.getNamespaceId(), "streams/" + streamId.getName());
    HttpRequest request = HttpRequest.put(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void sendEvent(Id.Stream streamId, String body) throws Exception {
    URL url = createURL(streamId.getNamespaceId(), "streams/" + streamId.getName());
    HttpRequest request = HttpRequest.post(url).withBody(body).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private List<String> fetchEvents(Id.Stream streamId) throws Exception {
    URL url = createURL(streamId.getNamespaceId(), "streams/" + streamId.getName() + "/events");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());


    List<String> events = Lists.newArrayList();
    JsonArray jsonArray = new JsonParser().parse(response.getResponseBodyAsString()).getAsJsonArray();
    for (JsonElement jsonElement : jsonArray) {
      events.add(jsonElement.getAsJsonObject().get("body").getAsString());
    }
    return events;
  }

  @Test
  public void testNamespacedStreamEvents() throws Exception {
    // Create two streams with the same name, in different namespaces.
    String streamName = "testNamespacedMetrics";
    Id.Stream streamId1 = Id.Stream.from("namespace1", streamName);
    Id.Stream streamId2 = Id.Stream.from("namespace2", streamName);

    createStream(streamId1);
    createStream(streamId2);

    List<String> eventsSentToStream1 = Lists.newArrayList();
    // Enqueue 10 entries to the stream in the first namespace
    for (int i = 0; i < 10; ++i) {
      String body = streamId1.getNamespaceId() + Integer.toString(i);
      sendEvent(streamId1, body);
      eventsSentToStream1.add(body);
    }

    List<String> eventsSentToStream2 = Lists.newArrayList();
    // Enqueue only 5 entries to the stream in the second namespace, decrementing the value each time
    for (int i = 0; i > -5; --i) {
      String body = streamId1.getNamespaceId() + Integer.toString(i);
      sendEvent(streamId2, body);
      eventsSentToStream2.add(body);
    }

    // Test that even though the stream names are the same, the events ingested into the individual streams
    // are exactly what are fetched from the individual streams.
    List<String> eventsFetchedFromStream1 = fetchEvents(streamId1);
    Assert.assertEquals(eventsSentToStream1, eventsFetchedFromStream1);

    List<String> eventsFetchedFromStream2 = fetchEvents(streamId2);
    Assert.assertEquals(eventsSentToStream2, eventsFetchedFromStream2);
  }
}
