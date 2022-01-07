/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenRequest;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Singleton;
import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;


/**
 * Internal {@link HttpHandler} for Artifact Localizer.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/worker")
public class ArtifactLocalizerHttpHandlerInternal extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizerHttpHandlerInternal.class);

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
                                                                         new BasicThrowableCodec()).create();
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private final ArtifactLocalizer artifactLocalizer;

  @VisibleForTesting
  public ArtifactLocalizerHttpHandlerInternal(ArtifactLocalizer artifactLocalizer) {
    this.artifactLocalizer = artifactLocalizer;
  }

//  @GET
//  @Path("/computeMetadata/v1/instance/service-accounts/default/token")
//  public void getDefaultToken(HttpRequest request, HttpResponder responder) throws Exception {
//    // https://cloud.google.com/iam/docs/creating-short-lived-service-account-credentials
//    LOG.error("wyzhang: getDefaultToken");
//    IamCredentialsClient iamCredentialsClient;
//
//    try {
//      iamCredentialsClient =  IamCredentialsClient.create();
//    } catch (Throwable e) {
//      e.printStackTrace();
//      LOG.error("wyzhang:getDefault token failed {}", e);
//      throw e;
//    }
//    {
//      String gcpProject = "wyzhang-dev";
//      String gcpServiceAccountName = "wyzhang-sa";
//
//      ServiceAccountName name = ServiceAccountName.of(gcpProject, gcpServiceAccountName);
//      List<String> delegates = new ArrayList<>();
//      List<String> scope = new ArrayList<>();
//      Duration lifetime = Duration.newBuilder().build();
//      GenerateAccessTokenResponse response = iamCredentialsClient.generateAccessToken(name, delegates, scope, lifetime);
//
//      LOG.error("wyzhang: GenerateAccessTokenResponse got {}", response.toString());
//      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response, GenerateAccessTokenResponse.class));
//    }
//  }

  @GET
  @Path("/computeMetadata/v1/instance/service-accounts/default/token")
  public void getDefaultToken(HttpRequest request, HttpResponder responder) throws Exception {
    // https://cloud.google.com/iam/docs/creating-short-lived-service-account-credentials
    LOG.error("wyzhang: getDefaultToken");
    {
      String gcpProject = "wyzhang-dev";
      String gcpServiceAccountName = "wyzhang-sa";

      GenerateAccessTokenResponse resp = fetchAccessToken(gcpProject, gcpServiceAccountName);
      String respJson = GSON.toJson(resp, GenerateAccessTokenResponse.class);
      LOG.error("wyzhang: GenerateAccessTokenResponse got {}", respJson);
      responder.sendJson(HttpResponseStatus.OK, respJson);
    }
  }

  private GenerateAccessTokenResponse fetchAccessToken(String gcpProject, String gcpServiceAccountName) throws IOException {
    String endPoint = String.format(
      "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s@%s.iam.gserviceaccount.com:generateAccessToken",
      gcpProject, gcpServiceAccountName);
    URL url = new URL(endPoint);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    if (connection instanceof HttpsURLConnection) {
      // TODO (CDAP-18047) enable ssl verification
      disableVerifySSL(((HttpsURLConnection) connection));
    }
    connection.connect();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json; utf-8");
    connection.setRequestProperty("Accept", "application/json");
    connection.setDoOutput(true);
    GenerateAccessTokenRequest request = new GenerateAccessTokenRequest();
    List<String> scopes = new ArrayList<>();
    scopes.add("https://www.googleapis.com/auth/cloud-platform");
    request.setScope(scopes);

    String requestJson = GSON.toJson(request, GenerateAccessTokenRequest.class);
    try (OutputStream os = connection.getOutputStream()) {
      byte[] input = requestJson.getBytes(StandardCharsets.UTF_8);
      os.write(input, 0, input.length);
    }
    try (Reader reader = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8)) {
      if (connection.getResponseCode() != HttpResponseStatus.OK.code()) {
        LOG.error("wyzhang: get token failed {}", connection.getResponseMessage());
        throw new IOException(CharStreams.toString(reader));
      }
      GenerateAccessTokenResponse resp = GSON.fromJson(reader, GenerateAccessTokenResponse.class);
      return resp;
    } finally {
      connection.disconnect();
    }
  }

  private void disableVerifySSL(HttpsURLConnection connection) throws IOException {
    try {
      SSLContext sslContextWithNoVerify = SSLContext.getInstance("SSL");
      TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
        public X509Certificate[] getAcceptedIssuers() {
          return null;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] arg0, String arg1) {
          // No-op
        }

        @Override
        public void checkServerTrusted(X509Certificate[] arg0, String arg1) {
          // No-op
        }
      }};
      sslContextWithNoVerify.init(null, trustAllCerts, SECURE_RANDOM);
      connection.setSSLSocketFactory(sslContextWithNoVerify.getSocketFactory());
      connection.setHostnameVerifier((s, sslSession) -> true);
    } catch (Exception e) {
      LOG.error("Unable to initialize SSL context", e);
      throw new IOException(e.getMessage());
    }
  }

  @GET
  @Path("/artifact/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}")
  public void artifact(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("artifact-name") String artifactName,
                       @PathParam("artifact-version") String artifactVersion,
                       @QueryParam("unpack") @DefaultValue("true") boolean unpack) throws Exception {
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersion);
    try {
      File artifactPath = unpack
        ? artifactLocalizer.getAndUnpackArtifact(artifactId)
        : artifactLocalizer.getArtifact(artifactId);
      responder.sendString(HttpResponseStatus.OK, artifactPath.toString());
    } catch (Exception ex) {
      if (ex instanceof HttpErrorStatusProvider) {
        HttpResponseStatus status = HttpResponseStatus.valueOf(((HttpErrorStatusProvider) ex).getStatusCode());
        responder.sendString(status, exceptionToJson(ex));
      } else {
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex));
      }
    }
  }

  /**
   * Return json representation of an exception.
   * Used to propagate exception across network for better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }
}
