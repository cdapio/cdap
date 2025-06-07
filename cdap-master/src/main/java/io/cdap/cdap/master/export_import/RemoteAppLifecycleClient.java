package io.cdap.cdap.master.export_import;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.master.upgrade.UpgradeJobMain;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deploys pipelines by calling the internal AppFabric HTTP endpoint.
 */
public class RemoteAppLifecycleClient {

  private static final Gson GSON = new Gson();
  private final RemoteClient remoteClient;
  private static final Logger LOG = LoggerFactory.getLogger(RemoteAppLifecycleClient.class);

  @Inject
  public RemoteAppLifecycleClient(RemoteClientFactory remoteClientFactory) {
    this.remoteClient = remoteClientFactory.createRemoteClient(
        Constants.Service.APP_FABRIC_HTTP,
        new DefaultHttpRequestConfig(false),
        Gateway.API_VERSION_3);
  }

  /**
   * Custom exception for deployment failures.
   */
  public static class DeploymentException extends Exception {
    public DeploymentException(String message) {
      super(message);
    }
  }

  /**
   * Deploys a pipeline configuration. This corresponds to the PUT /apps/{app-id} endpoint,
   * which creates a new version of the application and marks it as the latest.
   *
   * @param namespace the namespace of the application
   * @param appName the name of the application
   * @param appRequest the application deployment request, containing artifact and config
   * @throws IOException if a network error occurs
   * @throws DeploymentException if the deployment returns a non-200 status code
   */
  public void deploy(String namespace, String appName, AppRequest<?> appRequest)
      throws IOException, DeploymentException {

    // Endpoint: PUT /v3/namespaces/{namespace-id}/apps/{app-id}
    String path = String.format("namespaces/%s/apps/%s", namespace, appName);

    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.PUT, path)
        .withBody(GSON.toJson(appRequest));

    HttpResponse response = remoteClient.execute(requestBuilder.build());

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      String errorMessage = String.format(
          "Failed to deploy app %s/%s. Status: %d, Response: %s",
          namespace, appName, response.getResponseCode(), response.getResponseBodyAsString(StandardCharsets.UTF_8)
      );
      throw new DeploymentException(errorMessage);
    }
  }
}