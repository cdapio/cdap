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

package io.cdap.cdap.security.authorization;

import com.google.inject.Inject;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.InvalidTokenException;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalAccessEnforcer extends AbstractAccessEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(InternalAccessEnforcer.class);

  private final TokenManager tokenManager;
  private final Codec<AccessToken> accessTokenCodec;

  @Inject
  InternalAccessEnforcer(CConfiguration cConf, TokenManager tokenManager,
      Codec<AccessToken> accessTokenCodec) {
    super(cConf);
    this.tokenManager = tokenManager;
    this.accessTokenCodec = accessTokenCodec;
  }

  @Override
  public void enforce(EntityId entity, Principal principal,
      Set<? extends Permission> permissions) throws AccessException {
    validateAccessTokenAndIdentity(principal.getName(), principal.getFullCredential());
  }

  @Override
  public void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal,
      Permission permission) throws AccessException {
    validateAccessTokenAndIdentity(principal.getName(), principal.getFullCredential());
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds,
      Principal principal) throws AccessException {
    try {
      validateAccessTokenAndIdentity(principal.getName(), principal.getFullCredential());
      return entityIds;
    } catch (AccessException e) {
      return Collections.emptySet();
    }
  }

  private void validateAccessTokenAndIdentity(String principalName, Credential credential)
      throws AccessException {
    if (credential == null) {
      throw new IllegalStateException("Attempted to internally enforce access on null credential");
    }
    if (!credential.getType().equals(Credential.CredentialType.INTERNAL)) {
      throw new IllegalStateException(
          "Attempted to internally enforce access on non-internal credential type");
    }
    AccessToken accessToken;
    try {
      accessToken = accessTokenCodec.decode(Base64.getDecoder().decode(credential.getValue()));
    } catch (IOException e) {
      throw new AccessException("Failed to deserialize access token", e);
    }
    try {
      tokenManager.validateSecret(accessToken);
    } catch (InvalidTokenException e) {
      throw new AccessException("Failed to validate access token", e);
    }
    UserIdentity userIdentity = accessToken.getIdentifier();
    if (!userIdentity.getUsername().equals(principalName)) {
      LOG.debug(
          String.format("Internal access token username differs from principal name; got token "
                  + "name '%s', expected principal name '%s'",
              userIdentity.getUsername(), principalName));
    }
    if (userIdentity.getIdentifierType() == null || !userIdentity.getIdentifierType()
        .equals(UserIdentity.IdentifierType.INTERNAL)) {
      throw new AccessException(
          String.format("Invalid internal access token type; got '%s', want '%s'",
              userIdentity.getIdentifierType(), UserIdentity.IdentifierType.INTERNAL));
    }
  }
}
