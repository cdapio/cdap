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
    Id.Application application = Id.Application.from(Id.Namespace.DEFAULT, AppWithDataset.class.getSimpleName());
    // should fail because we haven't provided any metadata in the request
    Assert.assertEquals(400, addMetadata(application).getResponseCode());
    Map<String, String> appMetadata = ImmutableMap.of("aKey", "aValue", "aK", "aV");
    Assert.assertEquals(200, addMetadata(application, appMetadata).getResponseCode());
    Id.Service pingService = Id.Service.from(application, "PingService");
    // should fail because we haven't provided any metadata in the request
    Assert.assertEquals(400, addMetadata(pingService).getResponseCode());
    Map<String, String> serviceMetadata = ImmutableMap.of("sKey", "sValue", "sK", "sV");
    Assert.assertEquals(200, addMetadata(pingService, serviceMetadata).getResponseCode());
    Id.DatasetInstance myds = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "myds");
    Assert.assertEquals(400, addMetadata(myds).getResponseCode());
    Map<String, String> datasetMetadata = ImmutableMap.of("dKey", "dValue", "dK", "dV");
    Assert.assertEquals(200, addMetadata(myds, datasetMetadata).getResponseCode());
    Id.Stream mystream = Id.Stream.from(Id.Namespace.DEFAULT, "mystream");
    Assert.assertEquals(400, addMetadata(mystream).getResponseCode());
    Map<String, String> streamMetadata = ImmutableMap.of("stKey", "stValue", "stK", "stV");
    Assert.assertEquals(200, addMetadata(mystream, streamMetadata).getResponseCode());
    // retrieve metadata and verify
    Map<String, String> metadata = getMetadata(application);
    // TODO: Enable verification after masking tags from metadata output
    // Assert.assertEquals(appMetadata, metadata);
    Assert.assertEquals("aValue", metadata.get("aKey"));
    Assert.assertEquals("aV", metadata.get("aK"));
    metadata = getMetadata(pingService);
    // TODO: Enable verification after masking tags from metadata output
    // Assert.assertEquals(serviceMetadata, metadata);
    Assert.assertEquals("sValue", metadata.get("sKey"));
    Assert.assertEquals("sV", metadata.get("sK"));
    metadata = getMetadata(myds);
    // TODO: Enable verification after masking tags from metadata output
    // Assert.assertEquals(datasetMetadata, metadata);
    Assert.assertEquals("dValue", metadata.get("dKey"));
    Assert.assertEquals("dV", metadata.get("dK"));
    metadata = getMetadata(mystream);
    // TODO: Enable verification after masking tags from metadata output
    // Assert.assertEquals(streamMetadata, metadata);
    Assert.assertEquals("stValue", metadata.get("stKey"));
    Assert.assertEquals("stV", metadata.get("stK"));
    // TODO: Enable these verifications after re-vamping metadata/tags deletion
    /*HttpResponse response = removeMetadata(application);
    // should fail because we haven't provided any keys to remove
    Assert.assertEquals(400, response.getResponseCode());
    response = removeMetadata(application, ImmutableList.of("aKey"));
    Assert.assertEquals(200, response.getResponseCode());
    metadata = getMetadata(application);
    Assert.assertEquals(1, metadata.size());
    Assert.assertEquals("aV", metadata.get("aK"));
    response = removeMetadata(pingService, ImmutableList.of("sKey", "sK"));
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertTrue(metadata.isEmpty());
    response = removeMetadata(pingService);
    Assert.assertEquals(200, response.getResponseCode());
    metadata = getMetadata(pingService);
    Assert.assertTrue(metadata.isEmpty());*/

    // non-existing namespace
    Id.Application nonExisting = Id.Application.from("blah", AppWithDataset.class.getSimpleName());
    Assert.assertEquals(404, addMetadata(nonExisting, appMetadata).getResponseCode());
    Assert.assertEquals(
      404, addMetadata(Id.Service.from(nonExisting, "PingService"), serviceMetadata).getResponseCode());
    Assert.assertEquals(404, addMetadata(Id.DatasetInstance.from("blah", "myds"), datasetMetadata).getResponseCode());
    Assert.assertEquals(404, addMetadata(Id.Stream.from("blah", "mystream"), streamMetadata).getResponseCode());
    // cleanup
    deleteApp(application, 200);
  }

  @Test
  public void testTags() throws Exception {
    Assert.assertEquals(200, deploy(AppWithDataset.class).getStatusLine().getStatusCode());
    Id.Application application = Id.Application.from(Id.Namespace.DEFAULT, AppWithDataset.class.getSimpleName());
    // should fail because we haven't provided any metadata in the request
    Assert.assertEquals(400, addTags(application).getResponseCode());
    List<String> appTags = ImmutableList.of("aTag", "aT");
    Assert.assertEquals(200, addTags(application, appTags).getResponseCode());
    Id.Service pingService = Id.Service.from(application, "PingService");
    // should fail because we haven't provided any metadata in the request
    Assert.assertEquals(400, addTags(pingService).getResponseCode());
    List<String> serviceTags = ImmutableList.of("sKey", "sK");
    Assert.assertEquals(200, addTags(pingService, serviceTags).getResponseCode());
    Id.DatasetInstance myds = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "myds");
    Assert.assertEquals(400, addTags(myds).getResponseCode());
    List<String> datasetTags = ImmutableList.of("dKey", "dK");
    Assert.assertEquals(200, addTags(myds, datasetTags).getResponseCode());
    Id.Stream mystream = Id.Stream.from(Id.Namespace.DEFAULT, "mystream");
    Assert.assertEquals(400, addTags(mystream).getResponseCode());
    List<String> streamTags = ImmutableList.of("stKey", "stK");
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
    //TODO: Add deletion tests
    // non-existing namespace
    Id.Application nonExisting = Id.Application.from("blah", AppWithDataset.class.getSimpleName());
    Assert.assertEquals(404, addTags(nonExisting, appTags).getResponseCode());
    Assert.assertEquals(404, addTags(Id.Service.from(nonExisting, "PingService"), serviceTags).getResponseCode());
    Assert.assertEquals(404, addTags(Id.DatasetInstance.from("blah", "myds"), datasetTags).getResponseCode());
    Assert.assertEquals(404, addTags(Id.Stream.from("blah", "mystream"), streamTags).getResponseCode());
    // cleanup
    deleteApp(application, 200);
  }

  private HttpResponse addMetadata(Id.Application app) throws IOException {
    return addMetadata(app, null);
  }

  private HttpResponse addMetadata(Id.Application app, @Nullable Map<String, String> metadata) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata", app.getId()), app.getNamespaceId());
    if (metadata == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(metadata));
  }

  private HttpResponse addMetadata(Id.Program program) throws Exception {
    return addMetadata(program, null);
  }

  private HttpResponse addMetadata(Id.Program program, @Nullable Map<String, String> metadata) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    if (metadata == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(metadata));
  }

  private HttpResponse addMetadata(Id.DatasetInstance dataset) throws IOException {
    return addMetadata(dataset, null);
  }

  private HttpResponse addMetadata(Id.DatasetInstance dataset,
                                   @Nullable Map<String, String> metadata) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata", dataset.getId()), dataset.getNamespaceId());
    if (metadata == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(metadata));
  }

  private HttpResponse addMetadata(Id.Stream stream) throws IOException {
    return addMetadata(stream, null);
  }

  private HttpResponse addMetadata(Id.Stream stream, @Nullable Map<String, String> metadata) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata", stream.getId()), stream.getNamespaceId());
    if (metadata == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(metadata));
  }

  private Map<String, String> getMetadata(Id.Application app) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata", app.getId()), app.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  private Map<String, String> getMetadata(Id.Program program) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  private Map<String, String> getMetadata(Id.DatasetInstance dataset) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata", dataset.getId()), dataset.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  private Map<String, String> getMetadata(Id.Stream stream) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata", stream.getId()), stream.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  private HttpResponse addTags(Id.Application app) throws IOException {
    return addTags(app, null);
  }

  private HttpResponse addTags(Id.Application app, @Nullable List<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/tags", app.getId()), app.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  private HttpResponse addTags(Id.Program program) throws IOException {
    return addTags(program, null);
  }

  private HttpResponse addTags(Id.Program program, @Nullable List<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/tags", program.getApplicationId(),
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
    String path = getVersionedAPIPath(String.format("datasets/%s/tags", dataset.getId()), dataset.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  private HttpResponse addTags(Id.Stream stream) throws IOException {
    return addTags(stream, null);
  }

  private HttpResponse addTags(Id.Stream stream, @Nullable List<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/tags", stream.getId()), stream.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  private List<String> getTags(Id.Application app) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/tags", app.getId()), app.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_STRING_TYPE);
  }

  private List<String> getTags(Id.Program program) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/tags", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_STRING_TYPE);
  }

  private List<String> getTags(Id.DatasetInstance dataset) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/tags", dataset.getId()), dataset.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_STRING_TYPE);
  }

  private List<String> getTags(Id.Stream stream) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/tags", stream.getId()), stream.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_STRING_TYPE);
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

  private URL getMetadataUrl(String resource) throws MalformedURLException {
    return new URL(String.format("%s%s", metadataServiceUrl, resource));
  }
}
