package io.cdap.cdap.master.export_import;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates namespaces by calling the internal Namespace HTTP endpoint.
 */
public class RemoteNamespaceClient {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteNamespaceClient.class);
  private static final Gson GSON = new Gson();
  private final RemoteClient remoteClient;

  @Inject
  public RemoteNamespaceClient(RemoteClientFactory remoteClientFactory) {
    // This client talks to the app-fabric service, which hosts the internal namespace APIs.
    this.remoteClient = remoteClientFactory.createRemoteClient(
        Constants.Service.APP_FABRIC_HTTP,
        new DefaultHttpRequestConfig(false),
        Gateway.API_VERSION_3);
  }

  /**
   * Custom exception for namespace creation failures.
   */
  public static class NamespaceCreationException extends Exception {
    public NamespaceCreationException(String message) {
      super(message);
    }
  }

  /**
   * Creates a namespace by calling the internal API.
   * Assumes the endpoint is PUT /namespaces/{namespace-id}
   *
   * @param namespaceId the name of the namespace to create
   * @param namespaceMeta the metadata for the namespace
   */
  public void create(String namespaceId, NamespaceMeta namespaceMeta)
      throws IOException, NamespaceCreationException {

    String path = String.format("namespaces/%s", namespaceId);
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.PUT, path)
        .withBody(GSON.toJson(namespaceMeta));

    HttpResponse response = remoteClient.execute(requestBuilder.build());

    // It's okay if the namespace already exists (HTTP 200 OK).
    // A 409 Conflict would also be acceptable but the handler might just return 200.
    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      String errorMessage = String.format(
          "Failed to create namespace %s. Status: %d, Response: %s",
          namespaceId, response.getResponseCode(), response.getResponseBodyAsString(StandardCharsets.UTF_8)
      );
      throw new NamespaceCreationException(errorMessage);
    }
    LOG.info("Successfully created or verified namespace '{}'", namespaceId);
  }
}
