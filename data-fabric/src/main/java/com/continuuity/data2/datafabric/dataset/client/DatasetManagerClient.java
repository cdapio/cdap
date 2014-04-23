package com.continuuity.data2.datafabric.dataset.client;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.datafabric.dataset.service.DatasetInstanceMeta;
import com.continuuity.data2.dataset2.manager.InstanceConflictException;
import com.continuuity.data2.dataset2.manager.ModuleConflictException;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.google.common.io.ByteStreams;
import org.apache.http.client.methods.HttpDelete;
import org.apache.twill.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// to be used by instantiator/accessor
public class DatasetManagerClient {
  private static final Gson GSON = new Gson();

  private EndpointStrategy endpointStrategy;

  @Inject
  public DatasetManagerClient(DiscoveryServiceClient discoveryClient) {

    this.endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_MANAGER)),
      1L, TimeUnit.SECONDS);
  }

  public DatasetInstanceMeta getInstance(String instanceName) throws IOException {
    HttpResponse response = doGet("/instances/" + instanceName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.getStatusLine().getStatusCode()) {
      return null;
    }
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    return GSON.fromJson(reader, DatasetInstanceMeta.class);
  }

  public DatasetTypeMeta getType(String typeName) throws IOException {
    HttpResponse response = doGet("/types/" + typeName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.getStatusLine().getStatusCode()) {
      return null;
    }
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    return GSON.fromJson(reader, DatasetTypeMeta.class);
  }

  public void addInstance(String datasetInstanceName, String datasetType, DatasetInstanceProperties props)
    throws InstanceConflictException, IOException {

    HttpResponse response = doPost("/instances/" + datasetInstanceName,
                                   GSON.toJson(props),
                                   ImmutableMap.of("type-name", datasetType));
    if (HttpResponseStatus.CONFLICT.getCode() == response.getStatusLine().getStatusCode()) {
      throw new InstanceConflictException("Failed to add instance " + datasetInstanceName +
                                            ", reason: " + bodyAsString(response));
    }
    if (HttpResponseStatus.OK.getCode() != response.getStatusLine().getStatusCode()) {
      throw new IOException("Failed to add instance " + datasetInstanceName + ", reason: " + bodyAsString(response));
    }
  }

  public void deleteInstance(String datasetInstanceName) throws InstanceConflictException, IOException {
    HttpResponse response = doDelete("/instances/" + datasetInstanceName);
    if (HttpResponseStatus.CONFLICT.getCode() == response.getStatusLine().getStatusCode()) {
      throw new InstanceConflictException("Failed to delete instance " + datasetInstanceName +
                                            ", reason: " + bodyAsString(response));
    }
    if (HttpResponseStatus.OK.getCode() != response.getStatusLine().getStatusCode()) {
      throw new IOException("Failed to delete instance " + datasetInstanceName + ", reason: " + bodyAsString(response));
    }
  }

  public void addModule(String moduleName, String className, Location jarLocation)
    throws ModuleConflictException, IOException {
    HttpResponse response = doPost("/modules/" + moduleName,
                                   jarLocation,
                                   ImmutableMap.of("class-name", className));

    if (HttpResponseStatus.CONFLICT.getCode() == response.getStatusLine().getStatusCode()) {
      throw new ModuleConflictException("Failed to add module " + moduleName + ", reason: " + bodyAsString(response));
    }
    if (HttpResponseStatus.OK.getCode() != response.getStatusLine().getStatusCode()) {
      throw new IOException("Failed to add module " + moduleName + ", reason: " + bodyAsString(response));
    }
  }


  public void deleteModule(String moduleName) throws ModuleConflictException, IOException {
    HttpResponse response = doDelete("/modules/" + moduleName);

    if (HttpResponseStatus.CONFLICT.getCode() == response.getStatusLine().getStatusCode()) {
      throw new ModuleConflictException("Failed to delete module " + moduleName +
                                          ", reason: " + bodyAsString(response));
    }
    if (HttpResponseStatus.OK.getCode() != response.getStatusLine().getStatusCode()) {
      throw new IOException("Failed to delete module " + moduleName + ", reason: " + bodyAsString(response));
    }
  }

  private HttpResponse doGet(String resource) throws IOException {
    DefaultHttpClient client = new DefaultHttpClient();
    String resolved = resolve(resource);
    HttpGet get = new HttpGet(resolved);
    return client.execute(get);
  }

  private HttpResponse doPost(String resource, Location fileToUpload, Map<String, String> headers)
    throws IOException {

    DefaultHttpClient client = new DefaultHttpClient();
    String resolved = resolve(resource);
    HttpPost post = new HttpPost(resolved);

    if (fileToUpload != null) {
      LocationEntity entity = new LocationEntity(fileToUpload, "application/octet-stream");
      post.setEntity(entity);
    }

    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        post.setHeader(header.getKey(), header.getValue());
      }
    }


    return client.execute(post);
  }

  private HttpResponse doPost(String resource, String body, Map<String, String> headers) throws IOException {
    DefaultHttpClient client = new DefaultHttpClient();
    String resolved = resolve(resource);
    HttpPost post = new HttpPost(resolved);

    if (body != null) {
      post.setEntity(new StringEntity(body));
    }

    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        post.setHeader(header.getKey(), header.getValue());
      }
    }

    return client.execute(post);
  }


  private HttpResponse doDelete(String resource) throws IOException {
    DefaultHttpClient client = new DefaultHttpClient();
    String resolved = resolve(resource);
    HttpDelete delete = new HttpDelete(resolved);

    return client.execute(delete);
  }

  private String bodyAsString(HttpResponse response) throws IOException {
    return Bytes.toString(ByteStreams.toByteArray(response.getEntity().getContent()));
  }

  private String resolve(String resource) {
    InetSocketAddress addr = this.endpointStrategy.pick().getSocketAddress();
    return "http://" + addr.getHostName() + ":" + addr.getPort() +
      "/" + Constants.Dataset.Manager.VERSION + "/datasets" + resource;
  }
}
