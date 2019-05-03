package io.cdap.cdap.data2.metadata.writer;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Metadata service client that allows CDAP Master to make Metadata updates via HTTP.
 */
public class MetadataServiceClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataServiceClient.class);
  private static final Gson GSON = new GsonBuilder().create();

  private final RemoteClient remoteClient;
  private final AuthenticationContext authenticationContext;

  public static final BiMap<String, String> ENTITY_TYPE_TO_API_PART;

  // Metadata endpoints uses names like 'apps' to represent application 'namespaces' to represent namespace so this map
  // is needed to convert one into another so that we can create a MetadataEntity appropriately.
  static {
    BiMap<String, String> map = HashBiMap.create();
    map.put(MetadataEntity.NAMESPACE, "namespaces");
    map.put(MetadataEntity.APPLICATION, "apps");
    map.put(MetadataEntity.DATASET, "datasets");
    map.put(MetadataEntity.VERSION, "versions");
    map.put(MetadataEntity.ARTIFACT, "artifacts");
    map.put(MetadataEntity.PROGRAM, "programs");
    map.put(MetadataEntity.PROGRAM_RUN, "runs");
    ENTITY_TYPE_TO_API_PART = map;
  }

  MetadataServiceClient(final DiscoveryServiceClient discoveryClient,
                                AuthenticationContext authenticationContext) {
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.METADATA_SERVICE,
                                         new DefaultHttpRequestConfig(false),
                                         Constants.Gateway.API_VERSION_3);
    this.authenticationContext = authenticationContext;
  }

  public void create(MetadataOperation.Create createOperation) {
    // Call Tags endpoint to add tags.
    String path = addQueryParams(String.format("%s/metadata/tags", constructPath(createOperation.getEntity())),
                                 createOperation.getEntity(), null);
    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.POST, path)
      .withBody(GSON.toJson(createOperation.getTags()));
    HttpResponse response = execute(builder);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to add tags, i.e. see response for details: %s", response);
    }

    // Call Properties endpoint to add properties.
    path = addQueryParams(String.format("%s/metadata/tags", constructPath(createOperation.getEntity())),
                          createOperation.getEntity(), null);
    builder = remoteClient.requestBuilder(HttpMethod.POST, path).withBody(GSON.toJson(createOperation.getProperties()));
    response = execute(builder);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to add properties, i.e. see response for details: %s", response);
    }
  }

  public void drop(MetadataOperation.Drop dropOperation) {
    String path = addQueryParams(String.format("%s/metadata", constructPath(dropOperation.getEntity())),
                                 dropOperation.getEntity(), null);
    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.DELETE, path);
    HttpResponse response = execute(builder);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to drop all metadata, i.e. see response for details: %s", response);
    }
  }

  public void deleteAll(MetadataOperation.DeleteAll deleteAllOperation) {
    String path = addQueryParams(String.format("%s/metadata", constructPath(deleteAllOperation.getEntity())),
                                 deleteAllOperation.getEntity(), deleteAllOperation.getScope());
    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.DELETE, path);
    HttpResponse response = execute(builder);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to delete metadata, i.e. see response for details: %s", response);
    }
  }

  public void deleteAllTags(MetadataOperation.DeleteAllTags deleteAllTagsOperation) {
    String path = addQueryParams(
      String.format("%s/metadata/tags", constructPath(deleteAllTagsOperation.getEntity())),
      deleteAllTagsOperation.getEntity(), deleteAllTagsOperation.getScope());
    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.DELETE, path);
    HttpResponse response = execute(builder);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to delete tags, i.e. see response for details: %s", response);
    }
  }

  public void deleteAllProperties(MetadataOperation.DeleteAllProperties deleteAllPropertiesOperation) {
    String path = addQueryParams(
      String.format("%s/metadata/properties", constructPath(deleteAllPropertiesOperation.getEntity())),
      deleteAllPropertiesOperation.getEntity(), deleteAllPropertiesOperation.getScope());
    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.DELETE, path);
    HttpResponse response = execute(builder);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to delete properties, i.e. see response for details: %s", response);
    }
  }

  public void put(MetadataOperation.Put putOperation) {
    // Call Tags endpoint to add tags.
    String path = addQueryParams(String.format("%s/metadata/tags", constructPath(putOperation.getEntity())),
                                 putOperation.getEntity(), putOperation.getScope());
    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.POST, path)
      .withBody(GSON.toJson(putOperation.getTags()));
    HttpResponse response = execute(builder);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to add tags, i.e. see response for details: %s", response);
    }

    // Call Properties endpoint to add properties.
    path = addQueryParams(String.format("%s/metadata/tags", constructPath(putOperation.getEntity())),
                          putOperation.getEntity(), putOperation.getScope());
    builder = remoteClient.requestBuilder(HttpMethod.POST, path).withBody(GSON.toJson(putOperation.getProperties()));
    response = execute(builder);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to add properties, i.e. see response for details: %s", response);
    }
  }

  public void delete(MetadataOperation.Delete deleteOperation) {
    // Call Tags endpoint to add tags.
    String path = addQueryParams(String.format("%s/metadata/tags", constructPath(deleteOperation.getEntity())),
                                 deleteOperation.getEntity(), deleteOperation.getScope());
    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.POST, path)
      .withBody(GSON.toJson(deleteOperation.getTags()));
    HttpResponse response = execute(builder);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to add tags, i.e. see response for details: %s", response);
    }

    // Call Properties endpoint to add properties.
    path = addQueryParams(String.format("%s/metadata/tags", constructPath(deleteOperation.getEntity())),
                          deleteOperation.getEntity(), deleteOperation.getScope());
    builder = remoteClient.requestBuilder(HttpMethod.POST, path).withBody(GSON.toJson(deleteOperation.getProperties()));
    response = execute(builder);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to add properties, i.e. see response for details: %s", response);
    }
  }

//  public void addTags(MetadataEntity metadataEntity, Set<String> tags) {
//    String path = String.format("%s/metadata/tags", constructPath(metadataEntity));
//    path = addQueryParams(path, metadataEntity, null);
//    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.POST, path).withBody(GSON.toJson(tags));
//    HttpResponse response = execute(builder);
//
//    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
//      LOG.trace("Failed to add tags, i.e. see response for details: %s", response);
//    }
//  }
//
//  public void removeTag(MetadataEntity metadataEntity, String tagToRemove) {
//    String path = String.format("%s/metadata/tag/%s", constructPath(metadataEntity), tagToRemove);
//    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.DELETE, path);
//    HttpResponse response = execute(builder);
//
//    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
//      LOG.trace("Failed to remove a tag, i.e. see response for details: %s", response);
//    }
//  }
//
//  public void removeTags(MetadataEntity metadataEntity) {
//    String path = String.format("%s/metadata/tags", constructPath(metadataEntity));
//    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.DELETE, path);
//    HttpResponse response = execute(builder);
//
//    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
//      LOG.trace("Failed to remove all tags, i.e. see response for details: %s", response);
//    }
//  }
//
//  public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {
//    String path = String.format("%s/metadata/properties", constructPath(metadataEntity));
//    path = addQueryParams(path, metadataEntity, null);
//    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.POST, path).withBody(GSON.toJson(properties));
//    HttpResponse response = execute(builder);
//
//    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
//      LOG.trace("Failed to add properties, i.e. see response for details: %s", response);
//    }
//  }
//
//  public void removeProperty(MetadataEntity metadataEntity, String propertyToRemove) {
//    String path = String.format("%s/metadata/property/%s", constructPath(metadataEntity), propertyToRemove);
//    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.DELETE, path);
//    HttpResponse response = execute(builder);
//
//    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
//      LOG.trace("Failed to remove a property, i.e. see response for details: %s", response);
//    }
//  }
//
//  public void removeProperties(MetadataEntity metadataEntity) {
//    String path = String.format("%s/metadata/properties", constructPath(metadataEntity));
//    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.DELETE, path);
//    HttpResponse response = execute(builder);
//
//    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
//      LOG.trace("Failed to remove all properties, i.e. see response for details: %s", response);
//    }
//  }
//
//  public void removeMetadata(MetadataEntity metadataEntity) {
//    String path = String.format("%s/metadata", constructPath(metadataEntity));
//    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.DELETE, path);
//    HttpResponse response = execute(builder);
//
//    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
//      LOG.trace("Failed to remove all metadata, i.e. see response for details: %s", response);
//    }
//  }

  protected HttpResponse execute(HttpRequest.Builder requestBuilder) {
    HttpRequest request = addUserIdHeader(requestBuilder).build();
    try {
      LOG.trace("Making metadata mutation request {}", request);
      HttpResponse response = remoteClient.execute(request);
      LOG.trace("Received response {} for request {}", response, request);
      return response;
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute metadata mutation, with request: " + request, e);
    }
  }

  // construct a component of the path, specific to each entity type
  private String constructPath(MetadataEntity metadataEntity) {
    StringBuilder builder = new StringBuilder();
    metadataEntity.iterator().forEachRemaining(keyValue -> {
      if (ENTITY_TYPE_TO_API_PART.containsKey(keyValue.getKey())) {
        builder.append(ENTITY_TYPE_TO_API_PART.get(keyValue.getKey()));
      } else {
        builder.append(keyValue.getKey());
      }
      builder.append("/");
      builder.append(keyValue.getValue());
      builder.append("/");
    });
    // remove the last /
    builder.replace(builder.length() - 1, builder.length(), "");
    return builder.toString();
  }

  private String addQueryParams(String path, MetadataEntity metadataEntity, @Nullable MetadataScope scope) {
    StringBuilder builder = new StringBuilder(path);
    String prefix = "?";
    if (!Iterables.getLast(metadataEntity.getKeys()).equalsIgnoreCase(metadataEntity.getType())) {
      // if last leaf node is not the entity type specify it through query para
      builder.append(prefix);
      builder.append("type=");
      builder.append(metadataEntity.getType());
      prefix = "&";
    }
    if (scope == null) {
      return builder.toString();
    } else {
      builder.append(prefix);
      builder.append("scope=");
      builder.append(scope);
    }
    return builder.toString();
  }

  private HttpRequest.Builder addUserIdHeader(HttpRequest.Builder requestBuilder) {
    return requestBuilder.addHeader(Constants.Security.Headers.USER_ID,
                                    authenticationContext.getPrincipal().getName());
  }
}
