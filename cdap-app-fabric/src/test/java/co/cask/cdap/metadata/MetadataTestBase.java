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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Base class for metadata tests.
 */
public abstract class MetadataTestBase extends AppFabricTestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();
  private static final Type SET_STRING_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Type SET_METADATA_SEARCH_RESULT_TYPE =
    new TypeToken<Set<MetadataSearchResultRecord>>() { }.getType();
  private static final Type SET_METADATA_RECORD_TYPE = new TypeToken<Set<MetadataRecord>>() { }.getType();
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

  protected HttpResponse addProperties(Id.Application app, @Nullable Map<String, String> properties)
    throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/properties", app.getId()), app.getNamespaceId());
    if (properties == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(properties));
  }

  protected HttpResponse addProperties(Id.Program program, @Nullable Map<String, String> properties)
    throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/properties", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    if (properties == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(properties));
  }

  protected HttpResponse addProperties(Id.DatasetInstance dataset,
                                       @Nullable Map<String, String> properties) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/properties",
                                                    dataset.getId()), dataset.getNamespaceId());
    if (properties == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(properties));
  }

  protected HttpResponse addProperties(Id.Stream stream, @Nullable Map<String, String> properties) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/properties",
                                                    stream.getId()), stream.getNamespaceId());
    if (properties == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(properties));
  }

  protected Set<MetadataRecord> getMetadata(Id.Application app) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata", app.getId()), app.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_RECORD_TYPE);
  }

  protected Set<MetadataRecord> getMetadata(Id.Program program) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_RECORD_TYPE);
  }

  protected Set<MetadataRecord> getMetadata(Id.DatasetInstance dataset) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata",
                                                    dataset.getId()), dataset.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_RECORD_TYPE);
  }

  protected Set<MetadataRecord> getMetadata(Id.Stream stream) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata",
                                                    stream.getId()), stream.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_RECORD_TYPE);
  }

  protected Map<String, String> getProperties(Id.Application app) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/properties", app.getId()), app.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  protected Map<String, String> getProperties(Id.Program program) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/properties", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  protected Map<String, String> getProperties(Id.DatasetInstance dataset) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/properties",
                                                    dataset.getId()), dataset.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  protected Map<String, String> getProperties(Id.Stream stream) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/properties",
                                                    stream.getId()), stream.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  protected void getPropertiesFromInvalidEntity(Id.Application app) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/properties", app.getId()), app.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(404, response.getResponseCode());
  }

  protected void getPropertiesFromInvalidEntity(Id.Program program) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/properties", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(404, response.getResponseCode());
  }

  protected void getPropertiesFromInvalidEntity(Id.DatasetInstance dataset) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/properties",
                                                    dataset.getId()), dataset.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(404, response.getResponseCode());
  }

  protected void getPropertiesFromInvalidEntity(Id.Stream stream) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/properties",
                                                    stream.getId()), stream.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(404, response.getResponseCode());
  }

  protected void removeMetadata(Id.Application app) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata", app.getId()), app.getNamespaceId());
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected void removeMetadata(Id.Program program) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected void removeMetadata(Id.DatasetInstance dataset) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata",
                                                    dataset.getId()), dataset.getNamespaceId());
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected void removeMetadata(Id.Stream stream) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata",
                                                    stream.getId()), stream.getNamespaceId());
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected void removeProperties(Id.Application app) throws IOException {
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

  protected void removeProperties(Id.Program program) throws IOException {
    removeProperties(program, null);
  }

  protected void removeProperties(Id.Program program, @Nullable String propertyToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/properties", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    if (propertyToRemove != null) {
      path = String.format("%s/%s", path, propertyToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected void removeProperties(Id.DatasetInstance dataset) throws IOException {
    removeProperties(dataset, null);
  }

  protected void removeProperties(Id.DatasetInstance dataset, @Nullable String propertyToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/properties",
                                                    dataset.getId()), dataset.getNamespaceId());
    if (propertyToRemove != null) {
      path = String.format("%s/%s", path, propertyToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected void removeProperties(Id.Stream stream) throws IOException {
    removeProperties(stream, null);
  }

  protected void removeProperties(Id.Stream stream, @Nullable String propertyToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/properties",
                                                    stream.getId()), stream.getNamespaceId());
    if (propertyToRemove != null) {
      path = String.format("%s/%s", path, propertyToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected HttpResponse addTags(Id.Application app, @Nullable Set<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/tags", app.getId()), app.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  protected HttpResponse addTags(Id.Program program, @Nullable Set<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/tags", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  protected HttpResponse addTags(Id.DatasetInstance dataset, @Nullable Set<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/tags",
                                                    dataset.getId()), dataset.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  protected HttpResponse addTags(Id.Stream stream, @Nullable Set<String> tags) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/tags",
                                                    stream.getId()), stream.getNamespaceId());
    if (tags == null) {
      return makePostRequest(path);
    }
    return makePostRequest(path, GSON.toJson(tags));
  }

  protected Set<MetadataSearchResultRecord> searchMetadata(String namespaceId,
                                                           String query, String target) throws IOException {
    String path;
    if (target == null) {
      path = getVersionedAPIPath(String.format("metadata/search?query=%s", query), namespaceId);
    } else {
      path = getVersionedAPIPath(String.format("metadata/search?query=%s&target=%s", query, target), namespaceId);
    }
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_SEARCH_RESULT_TYPE);
  }

  protected Set<String> getTags(Id.Application app) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/tags", app.getId()), app.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_STRING_TYPE);
  }

  protected Set<String> getTags(Id.Program program) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/metadata/tags", program.getApplicationId(),
                                                    program.getType().getCategoryName(), program.getId()),
                                      program.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_STRING_TYPE);
  }

  protected Set<String> getTags(Id.DatasetInstance dataset) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/tags",
                                                    dataset.getId()), dataset.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_STRING_TYPE);
  }

  protected Set<String> getTags(Id.Stream stream) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/tags",
                                                    stream.getId()), stream.getNamespaceId());
    HttpResponse response = makeGetRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_STRING_TYPE);
  }

  protected void removeTags(Id.Application app) throws IOException {
    removeTags(app, null);
  }

  protected void removeTags(Id.Application app, @Nullable String tagToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("apps/%s/metadata/tags", app.getId()), app.getNamespaceId());
    if (tagToRemove != null) {
      path = String.format("%s/%s", path, tagToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected void removeTags(Id.Program program) throws IOException {
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

  protected void removeTags(Id.DatasetInstance dataset) throws IOException {
    removeTags(dataset, null);
  }

  protected void removeTags(Id.DatasetInstance dataset, @Nullable String tagToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("datasets/%s/metadata/tags",
                                                    dataset.getId()), dataset.getNamespaceId());
    if (tagToRemove != null) {
      path = String.format("%s/%s", path, tagToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected void removeTags(Id.Stream stream) throws IOException {
    removeTags(stream, null);
  }

  protected void removeTags(Id.Stream stream, @Nullable String tagToRemove) throws IOException {
    String path = getVersionedAPIPath(String.format("streams/%s/metadata/tags",
                                                    stream.getId()), stream.getNamespaceId());
    if (tagToRemove != null) {
      path = String.format("%s/%s", path, tagToRemove);
    }
    HttpResponse response = makeDeleteRequest(path);
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected HttpResponse fetchLineage(Id.DatasetInstance datasetInstance, long start, long end, int levels)
    throws IOException {
    String path = getVersionedAPIPath(
      String.format("datasets/%s/lineage?start=%d&end=%d&levels=%d", datasetInstance.getId(), start, end, levels),
      datasetInstance.getNamespaceId());
    return makeGetRequest(path);
  }

  protected HttpResponse fetchLineage(Id.DatasetInstance datasetInstance, String start, String end, int levels)
    throws IOException {
    String path = getVersionedAPIPath(
      String.format("datasets/%s/lineage?start=%s&end=%s&levels=%d",
                    datasetInstance.getId(),
                    URLEncoder.encode(start, "UTF-8"),
                    URLEncoder.encode(end, "UTF-8"), levels),
      datasetInstance.getNamespaceId());
    return makeGetRequest(path);
  }

  protected HttpResponse fetchLineage(Id.Stream stream, long start, long end, int levels)
    throws IOException {
    String path = getVersionedAPIPath(
      String.format("streams/%s/lineage?start=%d&end=%d&levels=%d", stream.getId(), start, end, levels),
      stream.getNamespaceId());
    return makeGetRequest(path);
  }

  protected HttpResponse fetchLineage(Id.Stream stream, String start, String end, int levels)
    throws IOException {
    String path = getVersionedAPIPath(
      String.format("streams/%s/lineage?start=%s&end=%s&levels=%d",
                    stream.getId(),
                    URLEncoder.encode(start, "UTF-8"),
                    URLEncoder.encode(end, "UTF-8"), levels),
      stream.getNamespaceId());
    return makeGetRequest(path);
  }

  protected Set<MetadataRecord> fetchRunMetadata(Id.Run run) throws IOException {
    HttpResponse response = fetchRunMetadataResponse(run);
    Assert.assertEquals(200, response.getResponseCode());
    String responseBody = response.getResponseBodyAsString();
    return GSON.fromJson(responseBody, SET_METADATA_RECORD_TYPE);
  }

  protected HttpResponse fetchRunMetadataResponse(Id.Run run) throws IOException {
    Id.Program program = run.getProgram();
    String path = getVersionedAPIPath(String.format("apps/%s/%s/%s/runs/%s/metadata",
                                                    program.getApplicationId(), program.getType().getCategoryName(),
                                                    program.getId(), run.getId()),
                                      program.getNamespaceId());
    return makeGetRequest(path);
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
