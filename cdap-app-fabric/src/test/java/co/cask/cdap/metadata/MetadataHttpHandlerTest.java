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

package co.cask.cdap.metadata;

import co.cask.cdap.AppWithDataset;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests for {@link MetadataHttpHandler}
 */
public class MetadataHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final Type LIST_STRING_TYPE = new TypeToken<List<String>>() { }.getType();
  private static String metadataServiceUrl;

  private final Id.Application application =
    Id.Application.from(Id.Namespace.DEFAULT, AppWithDataset.class.getSimpleName());
  private final Id.Service pingService = Id.Service.from(application, "PingService");
  private final Id.DatasetInstance myds = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "myds");
  private final Id.Stream mystream = Id.Stream.from(Id.Namespace.DEFAULT, "mystream");
  private final Id.Application nonExistingApp = Id.Application.from("blah", AppWithDataset.class.getSimpleName());
  private final Id.Service nonExistingService = Id.Service.from(nonExistingApp, "PingService");
  private final Id.DatasetInstance nonExistingDataset = Id.DatasetInstance.from("blah", "myds");
  private final Id.Stream nonExistingStream = Id.Stream.from("blah", "mystream");

  @BeforeClass
  public static void setup() throws MalformedURLException {
    DiscoveryServiceClient discoveryClient = getInjector().getInstance(DiscoveryServiceClient.class);
    ServiceDiscovered metadataHttpDiscovered = discoveryClient.discover(Constants.Service.METADATA_SERVICE);
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(metadataHttpDiscovered);
    Discoverable discoverable = endpointStrategy.pick(1, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);
    int port = discoverable.getSocketAddress().getPort();
    metadataServiceUrl = String.format("http://127.0.0.1:%d", port);
  }

  @Test
  public void testMetadata() throws Exception {
    Assert.assertEquals(200, deploy(AppWithDataset.class).getStatusLine().getStatusCode());
    // should fail because we haven't provided any metadata in the request
    Assert.assertEquals(400, addProperties(application).getResponseCode());
    Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue", "aK", "aV");
    Assert.assertEquals(200, addProperties(application, appProperties).getResponseCode());
    // should fail because we haven't provided any metadata in the request
    Assert.assertEquals(400, addProperties(pingService).getResponseCode());
    Map<String, String> serviceProperties = ImmutableMap.of("sKey", "sValue", "sK", "sV");
    Assert.assertEquals(200, addProperties(pingService, serviceProperties).getResponseCode());
    Assert.assertEquals(400, addProperties(myds).getResponseCode());
    Map<String, String> datasetProperties = ImmutableMap.of("dKey", "dValue", "dK", "dV");
    Assert.assertEquals(200, addProperties(myds, datasetProperties).getResponseCode());
    Assert.assertEquals(400, addProperties(mystream).getResponseCode());
    Map<String, String> streamProperties = ImmutableMap.of("stKey", "stValue", "stK", "stV");
    Assert.assertEquals(200, addProperties(mystream, streamProperties).getResponseCode());
    // retrieve properties and verify
    Map<String, String> properties = getProperties(application);
    Assert.assertEquals(appProperties, properties);
    properties = getProperties(pingService);
    Assert.assertEquals(serviceProperties, properties);
    properties = getProperties(myds);
    Assert.assertEquals(datasetProperties, properties);
    properties = getProperties(mystream);
    Assert.assertEquals(streamProperties, properties);
    // test removal
    removeProperties(application);
    Assert.assertTrue(getProperties(application).isEmpty());
    removeProperties(pingService, "sKey");
    removeProperties(pingService, "sK");
    Assert.assertTrue(getProperties(pingService).isEmpty());
    removeProperties(myds, "dKey");
    Assert.assertEquals(ImmutableMap.of("dK", "dV"), getProperties(myds));
    removeProperties(mystream, "stK");
    Assert.assertEquals(ImmutableMap.of("stKey", "stValue"), getProperties(mystream));
    // cleanup
    removeProperties(application);
    removeProperties(pingService);
    removeProperties(myds);
    removeProperties(mystream);
    Assert.assertTrue(getProperties(application).isEmpty());
    Assert.assertTrue(getProperties(pingService).isEmpty());
    Assert.assertTrue(getProperties(myds).isEmpty());
    Assert.assertTrue(getProperties(mystream).isEmpty());

    // non-existing namespace
    Assert.assertEquals(404, addProperties(nonExistingApp, appProperties).getResponseCode());
    Assert.assertEquals(404, addProperties(nonExistingService, serviceProperties).getResponseCode());
    Assert.assertEquals(404, addProperties(nonExistingDataset, datasetProperties).getResponseCode());
    Assert.assertEquals(404, addProperties(nonExistingStream, streamProperties).getResponseCode());
    // cleanup
    deleteApp(application, 200);
  }

  @Test
  public void testTags() throws Exception {
    Assert.assertEquals(200, deploy(AppWithDataset.class).getStatusLine().getStatusCode());
    // should fail because we haven't provided any metadata in the request
    Assert.assertEquals(400, addTags(application).getResponseCode());
    List<String> appTags = ImmutableList.of("aTag", "aT");
    Assert.assertEquals(200, addTags(application, appTags).getResponseCode());
    // should fail because we haven't provided any metadata in the request
    Assert.assertEquals(400, addTags(pingService).getResponseCode());
    List<String> serviceTags = ImmutableList.of("sTag", "sT");
    Assert.assertEquals(200, addTags(pingService, serviceTags).getResponseCode());
    Assert.assertEquals(400, addTags(myds).getResponseCode());
    List<String> datasetTags = ImmutableList.of("dTag", "dT");
    Assert.assertEquals(200, addTags(myds, datasetTags).getResponseCode());
    Assert.assertEquals(400, addTags(mystream).getResponseCode());
    List<String> streamTags = ImmutableList.of("stTag", "stT");
    Assert.assertEquals(200, addTags(mystream, streamTags).getResponseCode());
    // retrieve tags and verify
    List<String> tags = getTags(application);
    Assert.assertTrue(tags.containsAll(appTags));
    Assert.assertTrue(appTags.containsAll(tags));
    tags = getTags(pingService);
    Assert.assertTrue(tags.containsAll(serviceTags));
    Assert.assertTrue(serviceTags.containsAll(tags));
    tags = getTags(myds);
    Assert.assertTrue(tags.containsAll(datasetTags));
    Assert.assertTrue(datasetTags.containsAll(tags));
    tags = getTags(mystream);
    Assert.assertTrue(tags.containsAll(streamTags));
    Assert.assertTrue(streamTags.containsAll(tags));
    // test removal
    removeTags(application, "aTag");
    Assert.assertEquals(ImmutableList.of("aT"), getTags(application));
    removeTags(pingService);
    Assert.assertTrue(getTags(pingService).isEmpty());
    removeTags(pingService);
    Assert.assertTrue(getTags(pingService).isEmpty());
    removeTags(myds, "dT");
    Assert.assertEquals(ImmutableList.of("dTag"), getTags(myds));
    removeTags(mystream, "stT");
    removeTags(mystream, "stTag");
    Assert.assertTrue(getTags(mystream).isEmpty());
    // cleanup
    removeTags(application);
    removeTags(pingService);
    removeTags(myds);
    removeTags(mystream);
    Assert.assertTrue(getTags(application).isEmpty());
    Assert.assertTrue(getTags(pingService).isEmpty());
    Assert.assertTrue(getTags(myds).isEmpty());
    Assert.assertTrue(getTags(mystream).isEmpty());
    // non-existing namespace
    Assert.assertEquals(404, addTags(nonExistingApp, appTags).getResponseCode());
    Assert.assertEquals(404, addTags(nonExistingService, serviceTags).getResponseCode());
    Assert.assertEquals(404, addTags(nonExistingDataset, datasetTags).getResponseCode());
    Assert.assertEquals(404, addTags(nonExistingStream, streamTags).getResponseCode());
    // cleanup
    deleteApp(application, 200);
  }

  private HttpResponse addProperties(Id.Application app) throws IOException {
    return addProperties(app, null);
  }

  private HttpResponse addProperties(Id.Application app, @Nullable Map<String, String> properties) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/properties", app.getId()), app.getNamespaceId());
    if (properties == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(properties));
  }

  private HttpResponse addProperties(Id.Program program) throws Exception {
    return addProperties(program, null);
  }

  private HttpResponse addProperties(Id.Program program, @Nullable Map<String, String> properties) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/properties", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    if (properties == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(properties));
  }

  private HttpResponse addProperties(Id.DatasetInstance dataset) throws IOException {
    return addProperties(dataset, null);
  }

  private HttpResponse addProperties(Id.DatasetInstance dataset,
                                     @Nullable Map<String, String> properties) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/properties",
                                                    dataset.getId()), dataset.getNamespaceId());
    if (properties == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(properties));
  }

  private HttpResponse addProperties(Id.Stream stream) throws IOException {
    return addProperties(stream, null);
  }

  private HttpResponse addProperties(Id.Stream stream, @Nullable Map<String, String> properties) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/properties",
                                                    stream.getId()), stream.getNamespaceId());
    if (properties == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(properties));
  }

  private Map<String, String> getProperties(Id.Application app) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/properties", app.getId()), app.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  private Map<String, String> getProperties(Id.Program program) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/properties", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  private Map<String, String> getProperties(Id.DatasetInstance dataset) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/properties",
                                                    dataset.getId()), dataset.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  private Map<String, String> getProperties(Id.Stream stream) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/properties",
                                                    stream.getId()), stream.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  private void removeProperties(Id.Application app) throws IOException {
    removeProperties(app, null);
  }

  private void removeProperties(Id.Application app, @Nullable String propertyToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/properties", app.getId()), app.getNamespaceId());
    if (propertyToRemove != null) {
      path = String.format("%s/%s", path, propertyToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void removeProperties(Id.Program program) throws IOException {
    removeProperties(program, null);
  }

  private void removeProperties(Id.Program program, @Nullable String propertyToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/properties", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    if (propertyToRemove != null) {
      path = String.format("%s/%s", path, propertyToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void removeProperties(Id.DatasetInstance dataset) throws IOException {
    removeProperties(dataset, null);
  }

  private void removeProperties(Id.DatasetInstance dataset, @Nullable String propertyToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/properties",
                                                    dataset.getId()), dataset.getNamespaceId());
    if (propertyToRemove != null) {
      path = String.format("%s/%s", path, propertyToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void removeProperties(Id.Stream stream) throws IOException {
    removeProperties(stream, null);
  }

  private void removeProperties(Id.Stream stream, @Nullable String propertyToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/properties",
                                                    stream.getId()), stream.getNamespaceId());
    if (propertyToRemove != null) {
      path = String.format("%s/%s", path, propertyToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private HttpResponse addTags(Id.Application app) throws IOException {
    return addTags(app, null);
  }

  private HttpResponse addTags(Id.Application app, @Nullable List<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/tags", app.getId()), app.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  private HttpResponse addTags(Id.Program program) throws IOException {
    return addTags(program, null);
  }

  private HttpResponse addTags(Id.Program program, @Nullable List<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/tags", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  private HttpResponse addTags(Id.DatasetInstance dataset) throws IOException {
    return addTags(dataset, null);
  }

  private HttpResponse addTags(Id.DatasetInstance dataset, @Nullable List<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/tags",
                                                    dataset.getId()), dataset.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  private HttpResponse addTags(Id.Stream stream) throws IOException {
    return addTags(stream, null);
  }

  private HttpResponse addTags(Id.Stream stream, @Nullable List<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/tags",
                                                    stream.getId()), stream.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  private List<String> getTags(Id.Application app) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/tags", app.getId()), app.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_STRING_TYPE);
  }

  private List<String> getTags(Id.Program program) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/tags", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_STRING_TYPE);
  }

  private List<String> getTags(Id.DatasetInstance dataset) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/tags",
                                                    dataset.getId()), dataset.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_STRING_TYPE);
  }

  private List<String> getTags(Id.Stream stream) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/tags",
                                                    stream.getId()), stream.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_STRING_TYPE);
  }

  private void removeTags(Id.Application app) throws IOException {
    removeTags(app, null);
  }

  private void removeTags(Id.Application app, @Nullable String tagToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/tags", app.getId()), app.getNamespaceId());
    if (tagToRemove != null) {
      path = String.format("%s/%s", path, tagToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void removeTags(Id.Program program) throws IOException {
    removeTags(program, null);
  }

  private void removeTags(Id.Program program, @Nullable String tagToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/tags", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    if (tagToRemove != null) {
      path = String.format("%s/%s", path, tagToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void removeTags(Id.DatasetInstance dataset) throws IOException {
    removeTags(dataset, null);
  }

  private void removeTags(Id.DatasetInstance dataset, @Nullable String tagToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/tags",
                                                    dataset.getId()), dataset.getNamespaceId());
    if (tagToRemove != null) {
      path = String.format("%s/%s", path, tagToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void removeTags(Id.Stream stream) throws IOException {
    removeTags(stream, null);
  }

  private void removeTags(Id.Stream stream, @Nullable String tagToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/tags",
                                                    stream.getId()), stream.getNamespaceId());
    if (tagToRemove != null) {
      path = String.format("%s/%s", path, tagToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  // The following methods are needed because AppFabricTestBase's doGet, doPost, doPut, doDelete are hardwired to
  // AppFabric Service
  private HttpResponse makePostRequest(String resource) throws IOException {
    return makePostRequest(resource, null);
  }

  private HttpResponse makePostRequest(String resource, @Nullable String body) throws IOException {
    HttpRequest.Builder postBuilder = HttpRequest.post(getMetadataUrl(resource));
    if (body != null) {
      postBuilder.withBody(body);
    }
    return HttpRequests.execute(postBuilder.build());
  }

  private HttpResponse makeGetRequest(String resource) throws IOException {
    HttpRequest request = HttpRequest.get(getMetadataUrl(resource)).build();
    return HttpRequests.execute(request);
  }

  private HttpResponse makeDeleteRequest(String resource) throws IOException {
    HttpRequest request = HttpRequest.delete(getMetadataUrl(resource)).build();
    return HttpRequests.execute(request);
  }

  private URL getMetadataUrl(String resource) throws MalformedURLException {
    return new URL(String.format("%s%s", metadataServiceUrl, resource));
  }
}
