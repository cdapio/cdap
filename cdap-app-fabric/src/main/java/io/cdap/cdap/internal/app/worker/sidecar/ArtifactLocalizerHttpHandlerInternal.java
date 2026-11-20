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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Singleton;
import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.authorization.InternalAccessEnforcer;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.util.Collections;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Internal {@link HttpHandler} for Artifact Localizer.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/worker")
public class ArtifactLocalizerHttpHandlerInternal extends AbstractHttpHandler {

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();
  @Nullable
  private final CConfiguration cConf;
  private final ArtifactLocalizer artifactLocalizer;
  @Nullable
  private final InternalAccessEnforcer internalAccessEnforcer;

  /**
   * Constructs {@link ArtifactLocalizerHttpHandlerInternal} for tests without caller validation.
   */
  @VisibleForTesting
  public ArtifactLocalizerHttpHandlerInternal(ArtifactLocalizer artifactLocalizer) {
    this(null, artifactLocalizer, null);
  }

  /**
   * Constructs {@link ArtifactLocalizerHttpHandlerInternal} without {@link CConfiguration}.
   */
  public ArtifactLocalizerHttpHandlerInternal(ArtifactLocalizer artifactLocalizer,
      @Nullable InternalAccessEnforcer internalAccessEnforcer) {
    this(null, artifactLocalizer, internalAccessEnforcer);
  }

  /**
   * Constructs {@link ArtifactLocalizerHttpHandlerInternal}.
   */
  public ArtifactLocalizerHttpHandlerInternal(@Nullable CConfiguration cConf,
      ArtifactLocalizer artifactLocalizer,
      @Nullable InternalAccessEnforcer internalAccessEnforcer) {
    this.cConf = cConf;
    this.artifactLocalizer = artifactLocalizer;
    this.internalAccessEnforcer = internalAccessEnforcer;
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
        HttpResponseStatus status = HttpResponseStatus.valueOf(
            ((HttpErrorStatusProvider) ex).getStatusCode());
        responder.sendString(status, exceptionToJson(ex));
      } else {
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex));
      }
    }
  }

  /**
   * Validates the caller's {@code X-CDAP-Internal-Authorization} header
   * ({@code CDAP-Internal token}) on behalf of the co-located {@code TaskWorkerHttpHandlerInternal}
   * container, which does not mount {@code tokenManagerKey.json}.
   */
  @POST
  @Path("/validate-caller")
  public void validateCaller(HttpRequest request, HttpResponder responder) {
    if (cConf == null || !cConf.getBoolean(Constants.Security.INTERNAL_AUTH_ENABLED)
        || internalAccessEnforcer == null) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
          "Internal authentication and InternalAccessEnforcer must be enabled for caller "
              + "validation.");
      return;
    }
    String authHeader = request.headers().get(Constants.Security.Headers.INTERNAL_AUTHORIZATION);
    if (authHeader == null || authHeader.trim().isEmpty()) {
      responder.sendString(HttpResponseStatus.UNAUTHORIZED,
          String.format("Missing %s header.", Constants.Security.Headers.INTERNAL_AUTHORIZATION));
      return;
    }
    String[] parts = authHeader.trim().split("\\s+", 2);
    if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
      responder.sendString(HttpResponseStatus.UNAUTHORIZED,
          String.format("Malformed %s header.",
              Constants.Security.Headers.INTERNAL_AUTHORIZATION));
      return;
    }
    String credentialTypeStr = parts[0];
    String tokenValue = parts[1];

    try {
      Credential.CredentialType credentialType =
          Credential.CredentialType.fromQualifiedName(credentialTypeStr);
      String userId = request.headers().get(Constants.Security.Headers.USER_ID);
      if (Strings.isNullOrEmpty(userId)) {
        userId = "system";
      }
      Credential credential = new Credential(tokenValue, credentialType);
      Principal principal = new Principal(userId, Principal.PrincipalType.USER, null, credential);
      internalAccessEnforcer.enforce(
          NamespaceId.SYSTEM, principal, Collections.singleton(StandardPermission.GET));
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (AccessException | IllegalArgumentException | IllegalStateException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
          Strings.isNullOrEmpty(e.getMessage()) ? "Caller validation failed." : e.getMessage());
    }
  }

  /**
   * Return json representation of an exception. Used to propagate exception across network for
   * better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }
}
