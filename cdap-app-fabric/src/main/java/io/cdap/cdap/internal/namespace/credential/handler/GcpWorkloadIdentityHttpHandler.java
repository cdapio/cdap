/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.namespace.credential.handler;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.credential.CredentialIdentityManager;
import io.cdap.cdap.internal.credential.CredentialProfileManager;
import io.cdap.cdap.internal.namespace.credential.GcpWorkloadIdentityUtil;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProvider;
import io.cdap.cdap.proto.credential.IdentityValidationException;
import io.cdap.cdap.proto.credential.NamespaceWorkloadIdentity;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.NamespacePermission;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link HttpHandler} for namespace identity providers.
 */
@Singleton
@Path(Gateway.API_VERSION_3)
public class GcpWorkloadIdentityHttpHandler extends AbstractHttpHandler {
  private static final Gson GSON = new Gson();

  private final ContextAccessEnforcer accessEnforcer;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CredentialIdentityManager credentialIdentityManager;
  private final CredentialProfileManager credentialProfileManager;
  private final CredentialProvider credentialProvider;

  @Inject
  GcpWorkloadIdentityHttpHandler(ContextAccessEnforcer accessEnforcer,
      NamespaceQueryAdmin namespaceQueryAdmin,
      CredentialIdentityManager credentialIdentityManager,
      CredentialProfileManager credentialProfileManager,
      CredentialProvider credentialProvider) {
    this.accessEnforcer = accessEnforcer;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.credentialIdentityManager = credentialIdentityManager;
    this.credentialProfileManager = credentialProfileManager;
    this.credentialProvider = credentialProvider;
  }

  /**
   * Validates a credential identity.
   *
   * @param request   The HTTP request.
   * @param responder The HTTP responder.
   * @throws BadRequestException If identity validation fails.
   * @throws NotFoundException   If the associated profile is not found.
   * @throws IOException         If transport errors occur.
   */
  @POST
  @Path("/namespaces/{namespace-id}/credentials/workloadIdentity/validate")
  public void validateIdentity(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace) throws Exception {
    accessEnforcer.enforce(new NamespaceId(namespace), NamespacePermission.PROVISION_CREDENTIAL);
    NamespaceWorkloadIdentity namespaceWorkloadIdentity =
        deserializeRequestContent(request, NamespaceWorkloadIdentity.class);
    if (Strings.isNullOrEmpty(namespaceWorkloadIdentity.getIdentity())) {
      throw new BadRequestException("Identity cannot be null or empty.");
    }
    NamespaceMeta namespaceMeta = getNamespaceMeta(namespace);
    validateNamespaceIdentity(namespaceMeta, namespaceWorkloadIdentity);
    CredentialIdentity credentialIdentity = new CredentialIdentity(
        NamespaceId.SYSTEM.getNamespace(), GcpWorkloadIdentityUtil.SYSTEM_PROFILE_NAME,
        namespaceWorkloadIdentity.getIdentity(),
        namespaceWorkloadIdentity.getServiceAccount());
    switchToInternalUser();
    try {
      credentialProvider.validateIdentity(namespaceMeta, credentialIdentity);
    } catch (IdentityValidationException e) {
      throw new BadRequestException(String.format("Identity validation failed with error: %s",
          e.getCause() == null ? e.getMessage() : e.getCause().getMessage()), e);
    } catch (io.cdap.cdap.proto.credential.NotFoundException e) {
      throw new NotFoundException(e.getMessage());
    }
    responder.sendJson(HttpResponseStatus.OK, "Namespace identity validated successfully");
  }

  /**
   * Fetches a credential identity.
   *
   * @param request      The HTTP request.
   * @param responder    The HTTP responder.
   * @param namespace    The identity namespace.
   * @throws BadRequestException If the identity name is invalid.
   * @throws IOException         If transport errors occur.
   * @throws NotFoundException   If the namespace or identity are not found.
   */
  @GET
  @Path("/namespaces/{namespace-id}/credentials/workloadIdentity")
  public void getIdentity(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace) throws Exception {
    NamespaceMeta namespaceMeta = getNamespaceMeta(namespace);
    CredentialIdentityId credentialIdentityId = createIdentityIdOrPropagate(namespace,
        GcpWorkloadIdentityUtil.getWorkloadIdentityName(namespaceMeta.getIdentity()));
    switchToInternalUser();
    Optional<CredentialIdentity> identity = credentialIdentityManager.get(credentialIdentityId);
    if (!identity.isPresent()) {
      throw new NotFoundException("Namespace identity not found.");
    }
    NamespaceWorkloadIdentity workloadIdentity = new NamespaceWorkloadIdentity(
        identity.get().getIdentity(), identity.get().getSecureValue());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(workloadIdentity));
  }

  /**
   * Creates a new identity.
   *
   * @param request   The HTTP request.
   * @param responder The HTTP responder.
   * @param namespace The identity namespace.
   * @throws AlreadyExistsException If the identity exists.
   * @throws BadRequestException    If the identity name or identity are invalid.
   * @throws IOException            If transport errors occur.
   * @throws NotFoundException      If the namespace is not found.
   */
  @PUT
  @Path("/namespaces/{namespace-id}/credentials/workloadIdentity")
  public void createIdentity(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace) throws Exception {
    accessEnforcer.enforce(new NamespaceId(namespace), NamespacePermission.SET_SERVICE_ACCOUNT);
    NamespaceWorkloadIdentity namespaceWorkloadIdentity =
        deserializeRequestContent(request, NamespaceWorkloadIdentity.class);
    if (Strings.isNullOrEmpty(namespaceWorkloadIdentity.getIdentity())) {
      throw new BadRequestException("Identity cannot be null or empty.");
    }
    NamespaceMeta namespaceMeta = getNamespaceMeta(namespace);
    validateNamespaceIdentity(namespaceMeta, namespaceWorkloadIdentity);
    CredentialIdentityId credentialIdentityId = createIdentityIdOrPropagate(namespace,
        GcpWorkloadIdentityUtil.getWorkloadIdentityName(namespaceMeta.getIdentity()));
    switchToInternalUser();
    Optional<CredentialIdentity> identity = credentialIdentityManager.get(credentialIdentityId);
    CredentialIdentity credentialIdentity = new CredentialIdentity(
        NamespaceId.SYSTEM.getNamespace(), GcpWorkloadIdentityUtil.SYSTEM_PROFILE_NAME,
        namespaceMeta.getIdentity(), namespaceWorkloadIdentity.getServiceAccount());
    if (identity.isPresent()) {
      credentialIdentityManager.update(credentialIdentityId, credentialIdentity);
    } else {
      credentialIdentityManager.create(credentialIdentityId, credentialIdentity);
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Deletes an identity.
   *
   * @param request      The HTTP request.
   * @param responder    The HTTP responder.
   * @param namespace    The identity namespace.
   * @throws BadRequestException If the identity name is invalid.
   * @throws IOException         If transport errors occur.
   * @throws NotFoundException   If the namespace or identity are not found.
   */
  @DELETE
  @Path("/namespaces/{namespace-id}/credentials/workloadIdentity")
  public void deleteIdentity(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace) throws Exception {
    accessEnforcer.enforce(new NamespaceId(namespace), NamespacePermission.UNSET_SERVICE_ACCOUNT);
    NamespaceMeta namespaceMeta = getNamespaceMeta(namespace);
    CredentialIdentityId credentialIdentityId = createIdentityIdOrPropagate(namespace,
        GcpWorkloadIdentityUtil.getWorkloadIdentityName(namespaceMeta.getIdentity()));
    switchToInternalUser();
    credentialIdentityManager.delete(credentialIdentityId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private NamespaceMeta getNamespaceMeta(String namespace) throws Exception {
    if (NamespaceId.SYSTEM.getNamespace().equals(namespace)) {
      return NamespaceMeta.SYSTEM;
    }
    try {
      return namespaceQueryAdmin.get(new NamespaceId(namespace));
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause instanceof NamespaceNotFoundException || cause instanceof UnauthorizedException) {
        throw (Exception) cause;
      }
      throw new IOException(String.format("Failed to get namespace '%s' metadata",
          namespace), e);
    }
  }

  private void switchToInternalUser() {
    SecurityRequestContext.reset();
  }

  private void validateNamespaceIdentity(NamespaceMeta namespaceMeta, NamespaceWorkloadIdentity identity)
      throws BadRequestException {
    if (!namespaceMeta.getIdentity().equals(identity.getIdentity())) {
      throw new BadRequestException("Incorrect value provided for namespace identity.");
    }
  }

  private CredentialIdentityId createIdentityIdOrPropagate(String namespace, String name)
      throws BadRequestException {
    try {
      return new CredentialIdentityId(namespace, name);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
  }

  private <T> T deserializeRequestContent(FullHttpRequest request, Class<T> clazz)
      throws BadRequestException {
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()),
        StandardCharsets.UTF_8)) {
      T content = GSON.fromJson(reader, clazz);
      if (content == null) {
        throw new BadRequestException("No request object provided; expected class "
            + clazz.getName());
      }
      return content;
    } catch (JsonSyntaxException | IOException e) {
      throw new BadRequestException("Unable to parse request: " + e.getMessage(), e);
    }
  }
}

