package io.cdap.cdap.master.export_import;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A remote client for fetching CDAP metadata (namespaces, applications) via internal HTTP APIs for export.
 */
public class RemoteFetchDataClient {

  private static final Gson GSON = new Gson();
  private final RemoteClient remoteClient;

  @Inject
  public RemoteFetchDataClient(RemoteClientFactory remoteClientFactory) {
    // This client talks to the app-fabric service, which hosts the internal namespace and app lifecycle APIs.
    this.remoteClient = remoteClientFactory.createRemoteClient(
        Constants.Service.APP_FABRIC_HTTP,
        new DefaultHttpRequestConfig(false),
        Constants.Gateway.API_VERSION_3);
  }

  /**
   * Custom exception for export failures.
   */
  public static class ExportException extends Exception {
    public ExportException(String message) {
      super(message);
    }
  }

  /**
   * A container for a single page of application results.
   */
  public static class PaginatedAppResponse {
    private final List<ApplicationRecord> applications;
    private final String nextPageToken;

    public PaginatedAppResponse(List<ApplicationRecord> applications, @Nullable String nextPageToken) {
      this.applications = applications;
      this.nextPageToken = nextPageToken;
    }

    public List<ApplicationRecord> getApplications() {
      return applications;
    }

    @Nullable
    public String getNextPageToken() {
      return nextPageToken;
    }
  }

  /**
   * Lists all namespaces by calling the GET /namespaces endpoint.
   */
  public List<NamespaceMeta> listNamespaces() throws IOException, ExportException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.GET, "namespaces").build();
    HttpResponse response = remoteClient.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new ExportException(
          String.format("Failed to list namespaces. Status: %d, Response: %s",
              response.getResponseCode(), response.getResponseBodyAsString(StandardCharsets.UTF_8))
      );
    }
    Type listType = new TypeToken<List<NamespaceMeta>>() { }.getType();
    return GSON.fromJson(response.getResponseBodyAsString(StandardCharsets.UTF_8), listType);
  }

  /**
   * Lists one page of applications in a given namespace.
   * Calls GET /namespaces/{namespace-id}/apps?latestOnly=false&pageSize=25
   *
   * @param namespace The namespace to query.
   * @param pageToken The token for the next page, or null to start from the beginning.
   * @return A {@link PaginatedAppResponse} containing the applications for the page and the next token.
   */
  public PaginatedAppResponse listApplications(String namespace, @Nullable String pageToken)
      throws IOException, ExportException {
    final int pageSize = 25;
    String path = String.format("namespaces/%s/apps?latestOnly=false&pageSize=%d", namespace, pageSize);
    if (pageToken != null) {
      path = String.format("%s&pageToken=%s", path, URLEncoder.encode(pageToken, StandardCharsets.UTF_8.name()));
    }

    HttpRequest request = remoteClient.requestBuilder(HttpMethod.GET, path).build();
    HttpResponse response = remoteClient.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new ExportException(
          String.format("Failed to list applications in namespace %s. Status: %d, Response: %s",
              namespace, response.getResponseCode(),
              response.getResponseBodyAsString(StandardCharsets.UTF_8))
      );
    }

    String responseBody = response.getResponseBodyAsString(StandardCharsets.UTF_8);
    com.google.gson.JsonObject responseObject = GSON.fromJson(responseBody, com.google.gson.JsonObject.class);

    List<ApplicationRecord> currentPageApps = Collections.emptyList();
    if (responseObject.has("applications")) {
      Type listType = new TypeToken<List<ApplicationRecord>>() { }.getType();
      currentPageApps = GSON.fromJson(responseObject.get("applications"), listType);
    }

    String nextPageToken = null;
    if (responseObject.has("nextPageToken") && !responseObject.get("nextPageToken").isJsonNull()) {
      nextPageToken = responseObject.get("nextPageToken").getAsString();
    }

    return new PaginatedAppResponse(currentPageApps, nextPageToken);
  }


  /**
   * Gets the full detail of a specific application version.
   * Calls GET /namespaces/{namespace-id}/apps/{app-id}/versions/{version-id}
   */
  public ApplicationDetail getApplicationDetail(String namespace, String appId, String versionId)
      throws IOException, ExportException {
    String path = String.format("namespaces/%s/apps/%s/versions/%s", namespace, appId, versionId);
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.GET, path).build();
    HttpResponse response = remoteClient.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new ExportException(
          String.format("Failed to get detail for app %s/%s version %s. Status: %d, Response: %s",
              namespace, appId, versionId, response.getResponseCode(),
              response.getResponseBodyAsString(StandardCharsets.UTF_8))
      );
    }
    return GSON.fromJson(response.getResponseBodyAsString(StandardCharsets.UTF_8), ApplicationDetail.class);
  }
}

