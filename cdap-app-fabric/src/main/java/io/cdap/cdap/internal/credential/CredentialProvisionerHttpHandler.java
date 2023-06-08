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

package io.cdap.cdap.internal.credential;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.NamespaceResourceAlreadyExistsException;
import io.cdap.cdap.api.NamespaceResourceManager;
import io.cdap.cdap.api.NamespaceResourceNotFoundException;
import io.cdap.cdap.api.NamespaceResourceReference;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.security.credential.CredentialIdentity;
import io.cdap.cdap.api.security.credential.CredentialProvisionerProfile;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.CredentialIdentityAlreadyExistsException;
import io.cdap.cdap.common.CredentialIdentityNotFoundException;
import io.cdap.cdap.common.CredentialProfileAlreadyExistsException;
import io.cdap.cdap.common.CredentialProfileNotFoundException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.TooManyRequestsException;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.credential.store.Util;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProvisionerProfileId;
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
import java.util.ConcurrentModificationException;
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
 * {@link HttpHandler} for credential provisioning.
 */
@Singleton
@Path(Gateway.API_VERSION_3)
public class CredentialProvisionerHttpHandler extends AbstractHttpHandler  {
  private static final Logger LOG = LoggerFactory.getLogger(CredentialProvisionerHttpHandler.class);
  private static final Gson GSON = new Gson();

  private final ContextAccessEnforcer accessEnforcer;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final NamespaceResourceManager<CredentialIdentity> credentialIdentityManager;
  private final NamespaceResourceManager<CredentialProvisionerProfile> credentialProfileManager;

  @Inject
  public CredentialProvisionerHttpHandler(ContextAccessEnforcer accessEnforcer,
      NamespaceQueryAdmin namespaceQueryAdmin,
      NamespaceResourceManager<CredentialIdentity> credentialIdentityManager,
      NamespaceResourceManager<CredentialProvisionerProfile> credentialProfileManager) {
    this.accessEnforcer = accessEnforcer;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.credentialIdentityManager = credentialIdentityManager;
    this.credentialProfileManager = credentialProfileManager;
  }

  @GET
  @Path("/credentials/provisioners")
  public void listProvisioners(HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.METHOD_NOT_ALLOWED,
        "Listing provisioners is unsupported.");
  }

  @GET
  @Path("/namespaces/{namespace-id}/credentials/profiles")
  public void listProfiles(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace) throws IOException, NotFoundException {
    accessEnforcer.enforceOnParent(EntityType.CREDENTIAL_PROVISIONER_PROFILE,
        new NamespaceId(namespace), StandardPermission.LIST);
    ensureNamespaceExists(namespace);
    responder.sendJson(HttpResponseStatus.OK,
        GSON.toJson(credentialProfileManager.list(namespace)));
  }

  @GET
  @Path("/namespaces/{namespace-id}/credentials/profiles/{profile-name}")
  public void getProfile(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace, @PathParam("profile-name") String profileName)
      throws BadRequestException, IOException, NotFoundException {
    NamespaceResourceReference ref = createAndValidateReference(namespace, profileName);
    accessEnforcer.enforce(new CredentialProvisionerProfileId(namespace, profileName),
        StandardPermission.GET);
    ensureNamespaceExists(namespace);
    Optional<CredentialProvisionerProfile> profile = credentialProfileManager.get(ref);
    if (profile.isPresent()) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profile.get()));
    } else {
      throw new CredentialProfileNotFoundException(ref);
    }
  }

  @POST
  @Path("/namespaces/{namespace-id}/credentials/profiles/{profile-name}")
  public void createProfile(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace, @PathParam("profile-name") String profileName)
      throws BadRequestException, CredentialProfileAlreadyExistsException, IOException,
      NotFoundException, TooManyRequestsException {
    NamespaceResourceReference ref = createAndValidateReference(namespace, profileName);
    accessEnforcer.enforce(new CredentialProvisionerProfileId(namespace, profileName),
        StandardPermission.CREATE);
    ensureNamespaceExists(namespace);
    CredentialProvisionerProfile profile = deserializeRequestContent(request,
        CredentialProvisionerProfile.class);
    validateProfile(profile);
    try {
      credentialProfileManager.create(ref, profile);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NamespaceResourceAlreadyExistsException e) {
      throw new CredentialProfileAlreadyExistsException(ref);
    } catch (ConcurrentModificationException e) {
      throw new TooManyRequestsException(e.getMessage(), e);
    }
  }

  @PUT
  @Path("/namespaces/{namespace-id}/credentials/profiles/{profile-name}")
  public void updateProfile(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace, @PathParam("profile-name") String profileName)
      throws BadRequestException, IOException, NotFoundException, TooManyRequestsException {
    NamespaceResourceReference ref = createAndValidateReference(namespace, profileName);
    accessEnforcer.enforce(new CredentialProvisionerProfileId(namespace, profileName),
        StandardPermission.UPDATE);
    ensureNamespaceExists(namespace);
    CredentialProvisionerProfile profile = deserializeRequestContent(request,
        CredentialProvisionerProfile.class);
    validateProfile(profile);
    try {
      credentialProfileManager.update(ref, profile);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NamespaceResourceNotFoundException e) {
      throw new CredentialProfileNotFoundException(ref);
    } catch (ConcurrentModificationException e) {
      throw new TooManyRequestsException(e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/credentials/profiles/{profile-name}")
  public void deleteProfile(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace, @PathParam("profile-name") String profileName)
      throws BadRequestException, ConflictException, IOException, NotFoundException {
    NamespaceResourceReference ref = createAndValidateReference(namespace, profileName);
    accessEnforcer.enforce(new CredentialProvisionerProfileId(namespace, profileName),
        StandardPermission.DELETE);
    ensureNamespaceExists(namespace);
    try {
      credentialProfileManager.delete(ref);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalStateException e) {
      throw new ConflictException(e.getMessage(), e);
    } catch (NamespaceResourceNotFoundException e) {
      throw new CredentialProfileNotFoundException(ref);
    }
  }

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

  @GET
  @Path("/namespaces/{namespace-id}/credentials/identities/{identity-name}")
  public void getIdentity(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("identity-name") String identityName)
      throws BadRequestException, IOException, NotFoundException {
    NamespaceResourceReference ref = createAndValidateReference(namespace, identityName);
    accessEnforcer.enforce(new CredentialIdentityId(namespace, identityName),
        StandardPermission.GET);
    ensureNamespaceExists(namespace);
    Optional<CredentialIdentity> profile = credentialIdentityManager.get(ref);
    if (profile.isPresent()) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profile.get()));
    } else {
      throw new CredentialIdentityNotFoundException(ref);
    }
  }

  @POST
  @Path("/namespaces/{namespace-id}/credentials/identities/{identity-name}")
  public void createIdentity(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("identity-name") String identityName)
      throws BadRequestException, CredentialIdentityAlreadyExistsException, IOException,
      NotFoundException, TooManyRequestsException {
    NamespaceResourceReference ref = createAndValidateReference(namespace, identityName);
    accessEnforcer.enforce(new CredentialIdentityId(namespace, identityName),
        StandardPermission.CREATE);
    ensureNamespaceExists(namespace);
    CredentialIdentity identity = deserializeRequestContent(request, CredentialIdentity.class);
    validateIdentity(identity);
    try {
      credentialIdentityManager.create(ref, identity);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    } catch (NamespaceResourceAlreadyExistsException e) {
      throw new CredentialIdentityAlreadyExistsException(ref);
    } catch (ConcurrentModificationException e) {
      throw new TooManyRequestsException(e.getMessage(), e);
    }
  }

  @PUT
  @Path("/namespaces/{namespace-id}/credentials/identities/{identity-name}")
  public void updateIdentity(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("identity-name") String identityName)
      throws BadRequestException, IOException, NotFoundException, TooManyRequestsException {
    NamespaceResourceReference ref = createAndValidateReference(namespace, identityName);
    accessEnforcer.enforce(new CredentialIdentityId(namespace, identityName),
        StandardPermission.UPDATE);
    ensureNamespaceExists(namespace);
    CredentialIdentity identity = deserializeRequestContent(request, CredentialIdentity.class);
    validateIdentity(identity);
    try {
      credentialIdentityManager.update(ref, identity);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    } catch (NamespaceResourceNotFoundException e) {
      throw new CredentialIdentityNotFoundException(ref);
    } catch (ConcurrentModificationException e) {
      throw new TooManyRequestsException(e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/credentials/identities/{identity-name}")
  public void deleteIdentity(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("identity-name") String identityName)
      throws BadRequestException, IOException, NotFoundException {
    NamespaceResourceReference ref = createAndValidateReference(namespace, identityName);
    accessEnforcer.enforce(new CredentialIdentityId(namespace, identityName),
        StandardPermission.DELETE);
    ensureNamespaceExists(namespace);
    try {
      credentialIdentityManager.delete(ref);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NamespaceResourceNotFoundException e) {
      throw new CredentialIdentityNotFoundException(ref);
    }
  }

  private void ensureNamespaceExists(String namespace)
      throws IOException, NamespaceNotFoundException {
    try {
      NamespaceId namespaceId = new NamespaceId(namespace);
      if(!namespaceQueryAdmin.exists(namespaceId)) {
        throw new NamespaceNotFoundException(namespaceId);
      }
    } catch (Exception e) {
      throw new IOException(String.format("Failed to check if namespace '%s' exists",
          namespace), e);
    }
  }

  private void validateProfile(CredentialProvisionerProfile profile) throws BadRequestException {
    // Validate all fields are not null.
    if (profile.getMetadata() == null) {
      throw new BadRequestException("Credential provisioner profile metadata cannot be null.");
    }
    if (profile.getMetadata().getProvisionerType() == null ||
        profile.getMetadata().getProvisionerType().isEmpty()) {
      throw new BadRequestException("Credential provisioner profile type cannot be null or empty.");
    }
  }

  private void validateIdentity(CredentialIdentity identity)
      throws AccessException, BadRequestException {
    // Validate all fields are not null.
    if (identity.getMetadata() == null) {
      throw new BadRequestException("Credential identity metadata cannot be null.");
    }
    if (identity.getMetadata().getCredentialProvisionerProfile() == null) {
      throw new BadRequestException("Credential identity associated profile cannot be null.");
    }
    NamespaceResourceReference profileRef = identity.getMetadata()
        .getCredentialProvisionerProfile();
    accessEnforcer.enforce(new CredentialProvisionerProfileId(profileRef.getNamespace(),
        profileRef.getName()), StandardPermission.GET);
  }

  private NamespaceResourceReference createAndValidateReference(String namespace, String name)
      throws BadRequestException {
    NamespaceResourceReference ref = new NamespaceResourceReference(namespace, name);
    try {
      Util.validateResourceName(ref);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
    return ref;
  }

  private <T> T deserializeRequestContent(FullHttpRequest request, Class<T> clazz)
      throws BadRequestException {
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()),
        StandardCharsets.UTF_8)) {
      T content = GSON.fromJson(reader, clazz);
      return content;
    } catch (JsonSyntaxException | IOException e) {
      throw new BadRequestException("Unable to parse request: " + e.getMessage(), e);
    }
  }
}
