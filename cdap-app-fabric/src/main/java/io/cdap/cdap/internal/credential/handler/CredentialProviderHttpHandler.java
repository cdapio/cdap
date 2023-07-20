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

package io.cdap.cdap.internal.credential.handler;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.credential.CredentialIdentityManager;
import io.cdap.cdap.internal.credential.CredentialProfileManager;
import io.cdap.cdap.proto.credential.CreateCredentialIdentityRequest;
import io.cdap.cdap.proto.credential.CreateCredentialProfileRequest;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.credential.CredentialProvider;
import io.cdap.cdap.proto.credential.IdentityValidationException;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link HttpHandler} for credential providers.
 */
@Singleton
@Path(Gateway.API_VERSION_3)
public class CredentialProviderHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(CredentialProviderHttpHandler.class);
  private static final Gson GSON = new Gson();

  private final ContextAccessEnforcer accessEnforcer;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CredentialIdentityManager credentialIdentityManager;
  private final CredentialProfileManager credentialProfileManager;
  private final CredentialProvider credentialProvider;

  @Inject
  CredentialProviderHttpHandler(ContextAccessEnforcer accessEnforcer,
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
   * Returns a list of supported credential providers.
   *
   * @param request   The HTTP request.
   * @param responder The HTTP responder.
   */
  @GET
  @Path("/credentials/providers")
  public void listProviders(HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.METHOD_NOT_ALLOWED,
        "Listing providers is unsupported.");
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
  @Path("/credentials/identities/validate")
  public void validateIdentity(FullHttpRequest request, HttpResponder responder)
      throws BadRequestException, NotFoundException, IOException {
    CredentialIdentity identity = deserializeRequestContent(request, CredentialIdentity.class);
    try {
      credentialProvider.validateIdentity(identity);
    } catch (IdentityValidationException e) {
      throw new BadRequestException(String.format("Identity failed validation with error: %s",
          e.getMessage()), e);
    } catch (io.cdap.cdap.proto.credential.NotFoundException e) {
      throw new NotFoundException(e.getMessage());
    }
  }

  /**
   * Returns a list of credential profiles for the namespace.
   *
   * @param request   The HTTP request.
   * @param responder The HTTP responder.
   * @param namespace The namespace.
   * @throws IOException       If transport errors occur.
   * @throws NotFoundException If the namespace is not found.
   */
  @GET
  @Path("/namespaces/{namespace-id}/credentials/profiles")
  public void listProfiles(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace) throws IOException, NotFoundException {
    accessEnforcer.enforceOnParent(EntityType.CREDENTIAL_PROFILE,
        new NamespaceId(namespace), StandardPermission.LIST);
    ensureNamespaceExists(namespace);
    responder.sendJson(HttpResponseStatus.OK,
        GSON.toJson(credentialProfileManager.list(namespace)));
  }

  /**
   * Fetches a credential profile.
   *
   * @param request     The HTTP request.
   * @param responder   The HTTP responder.
   * @param namespace   The profile namespace.
   * @param profileName The profile name.
   * @throws BadRequestException If the provided profile name is invalid.
   * @throws IOException         If transport errors occur.
   * @throws NotFoundException   If the namespace or profile are not found.
   */
  @GET
  @Path("/namespaces/{namespace-id}/credentials/profiles/{profile-name}")
  public void getProfile(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace, @PathParam("profile-name") String profileName)
      throws BadRequestException, IOException, NotFoundException {
    final CredentialProfileId profileId = createProfileIdOrPropagate(namespace, profileName);
    accessEnforcer.enforce(profileId, StandardPermission.GET);
    ensureNamespaceExists(namespace);
    Optional<CredentialProfile> profile;
    profile = credentialProfileManager.get(profileId);
    if (profile.isPresent()) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profile.get()));
    } else {
      throw new NotFoundException(String.format("Credential profile '%s' not found.", profileId));
    }
  }

  /**
   * Creates a new profile.
   *
   * @param request   The HTTP request.
   * @param responder The HTTP responder.
   * @param namespace The profile namespace.
   * @throws AlreadyExistsException If the profile already exists.
   * @throws BadRequestException    If the profile name or profile are invalid.
   * @throws IOException            If transport errors occur.
   * @throws NotFoundException      If the namespace is not found.
   */
  @POST
  @Path("/namespaces/{namespace-id}/credentials/profiles")
  public void createProfile(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace) throws AlreadyExistsException,
      BadRequestException, IOException, NotFoundException {
    final CreateCredentialProfileRequest createRequest = deserializeRequestContent(request,
        CreateCredentialProfileRequest.class);
    final CredentialProfileId profileId = createProfileIdOrPropagate(namespace,
        createRequest.getName());
    if (createRequest.getProfile() == null) {
      throw new BadRequestException("No profile provided for create request");
    }
    accessEnforcer.enforce(profileId, StandardPermission.CREATE);
    ensureNamespaceExists(namespace);
    credentialProfileManager.create(profileId, createRequest.getProfile());
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Updates an existing profile.
   *
   * @param request     The HTTP request.
   * @param responder   The HTTP responder.
   * @param namespace   The profile namespace.
   * @param profileName The profile name.
   * @throws BadRequestException If the profile name or profile are invalid.
   * @throws IOException         If transport errors occur.
   * @throws NotFoundException   If the namespace or profile are not found.
   */
  @PUT
  @Path("/namespaces/{namespace-id}/credentials/profiles/{profile-name}")
  public void updateProfile(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace, @PathParam("profile-name") String profileName)
      throws BadRequestException, IOException, NotFoundException {
    final CredentialProfileId profileId = createProfileIdOrPropagate(namespace, profileName);
    accessEnforcer.enforce(profileId, StandardPermission.UPDATE);
    ensureNamespaceExists(namespace);
    final CredentialProfile profile = deserializeRequestContent(request, CredentialProfile.class);
    credentialProfileManager.update(profileId, profile);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Deletes a profile.
   *
   * @param request     The HTTP request.
   * @param responder   The HTTP responder.
   * @param namespace   The profile namespace.
   * @param profileName The profile name.
   * @throws BadRequestException If the profile name is invalid.
   * @throws ConflictException   If the profile still has existing associated identities.
   * @throws IOException         If transport errors occur.
   * @throws NotFoundException   If the namespace or profile are not found.
   */
  @DELETE
  @Path("/namespaces/{namespace-id}/credentials/profiles/{profile-name}")
  public void deleteProfile(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace, @PathParam("profile-name") String profileName)
      throws BadRequestException, ConflictException, IOException, NotFoundException {
    final CredentialProfileId profileId = createProfileIdOrPropagate(namespace, profileName);
    accessEnforcer.enforce(profileId, StandardPermission.DELETE);
    ensureNamespaceExists(namespace);
    credentialProfileManager.delete(profileId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Returns a list of credential identities for the namespace.
   *
   * @param request   The HTTP request.
   * @param responder The HTTP responder.
   * @param namespace The namespace.
   * @throws IOException       If transport errors occur.
   * @throws NotFoundException If the namespace is not found.
   */
  @GET
  @Path("/namespaces/{namespace-id}/credentials/identities")
  public void listIdentities(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace) throws IOException, NotFoundException {
    accessEnforcer.enforceOnParent(EntityType.CREDENTIAL_IDENTITY, new NamespaceId(namespace),
        StandardPermission.LIST);
    ensureNamespaceExists(namespace);
    responder.sendJson(HttpResponseStatus.OK,
        GSON.toJson(credentialIdentityManager.list(namespace)));
  }

  /**
   * Fetches a credential identity.
   *
   * @param request      The HTTP request.
   * @param responder    The HTTP responder.
   * @param namespace    The identity namespace.
   * @param identityName The identity name.
   * @throws BadRequestException If the identity name is invalid.
   * @throws IOException         If transport errors occur.
   * @throws NotFoundException   If the namespace or identity are not found.
   */
  @GET
  @Path("/namespaces/{namespace-id}/credentials/identities/{identity-name}")
  public void getIdentity(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("identity-name") String identityName)
      throws BadRequestException, IOException, NotFoundException {
    final CredentialIdentityId identityId = createIdentityIdOrPropagate(namespace, identityName);
    accessEnforcer.enforce(new CredentialIdentityId(namespace, identityName),
        StandardPermission.GET);
    ensureNamespaceExists(namespace);
    Optional<CredentialIdentity> identity = credentialIdentityManager.get(identityId);
    if (identity.isPresent()) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(identity.get()));
    } else {
      throw new NotFoundException(String.format("Credential identity '%s' not found.", identityId));
    }
  }

  /**
   * Creates a new identity.
   *
   * @param request   The HTTP request.
   * @param responder The HTTP responder.
   * @param namespace The identity namespace.
   * @throws AlreadyExistsException If the identity already exists.
   * @throws BadRequestException    If the identity name or identity are invalid.
   * @throws IOException            If transport errors occur.
   * @throws NotFoundException      If the namespace is not found.
   */
  @POST
  @Path("/namespaces/{namespace-id}/credentials/identities")
  public void createIdentity(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace) throws AlreadyExistsException,
      BadRequestException, IOException, NotFoundException {
    CreateCredentialIdentityRequest createRequest = deserializeRequestContent(request,
        CreateCredentialIdentityRequest.class);
    final CredentialIdentityId identityId = createIdentityIdOrPropagate(namespace,
        createRequest.getName());
    final CredentialIdentity identity = createRequest.getIdentity();
    if (createRequest.getIdentity() == null) {
      throw new BadRequestException("No identity provided for create request");
    }
    accessEnforcer.enforce(identityId, StandardPermission.CREATE);
    ensureNamespaceExists(namespace);
    validateCredentialIdentity(identity);
    accessEnforcer.enforce(new CredentialProfileId(identity.getProfileNamespace(),
        identity.getProfileName()), StandardPermission.GET);
    credentialIdentityManager.create(identityId, identity);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Updates an existing identity.
   *
   * @param request      The HTTP request.
   * @param responder    The HTTP responder.
   * @param namespace    The identity namespace.
   * @param identityName The identity name.
   * @throws BadRequestException If the identity name or identity are invalid.
   * @throws IOException         If transport errors occur.
   * @throws NotFoundException   If the namespace or identity are not found.
   */
  @PUT
  @Path("/namespaces/{namespace-id}/credentials/identities/{identity-name}")
  public void updateIdentity(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("identity-name") String identityName)
      throws BadRequestException, IOException, NotFoundException {
    final CredentialIdentityId identityId = createIdentityIdOrPropagate(namespace, identityName);
    accessEnforcer.enforce(new CredentialIdentityId(namespace, identityName),
        StandardPermission.UPDATE);
    ensureNamespaceExists(namespace);
    final CredentialIdentity identity = deserializeRequestContent(request,
        CredentialIdentity.class);
    validateCredentialIdentity(identity);
    accessEnforcer.enforce(new CredentialProfileId(identity.getProfileNamespace(),
        identity.getProfileName()), StandardPermission.GET);
    credentialIdentityManager.update(identityId, identity);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Deletes an identity.
   *
   * @param request      The HTTP request.
   * @param responder    The HTTP responder.
   * @param namespace    The identity namespace.
   * @param identityName The identity name.
   * @throws BadRequestException If the identity name is invalid.
   * @throws IOException         If transport errors occur.
   * @throws NotFoundException   If the namespace or identity are not found.
   */
  @DELETE
  @Path("/namespaces/{namespace-id}/credentials/identities/{identity-name}")
  public void deleteIdentity(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("identity-name") String identityName)
      throws BadRequestException, IOException, NotFoundException {
    final CredentialIdentityId identityId = createIdentityIdOrPropagate(namespace, identityName);
    accessEnforcer.enforce(new CredentialIdentityId(namespace, identityName),
        StandardPermission.DELETE);
    ensureNamespaceExists(namespace);
    credentialIdentityManager.delete(identityId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private void ensureNamespaceExists(String namespace)
      throws IOException, NamespaceNotFoundException {
    NamespaceId namespaceId;
    Boolean namespaceFound;
    try {
      namespaceId = new NamespaceId(namespace);
      namespaceFound = namespaceQueryAdmin.exists(namespaceId);
    } catch (Exception e) {
      throw new IOException(String.format("Failed to check if namespace '%s' exists",
          namespace), e);
    }
    if (!namespaceFound) {
      throw new NamespaceNotFoundException(namespaceId);
    }
  }

  private CredentialProfileId createProfileIdOrPropagate(String namespace, String name)
      throws BadRequestException {
    try {
      return new CredentialProfileId(namespace, name);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
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

  private void validateCredentialIdentity(CredentialIdentity identity) throws BadRequestException {
    if (identity.getProfileNamespace() == null || identity.getProfileName() == null) {
      throw new BadRequestException("Identity provided with no associated profile");
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
