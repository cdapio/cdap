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

package io.cdap.cdap.security.auth;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.auth.UserIdentity.IdentifierType;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import java.util.LinkedHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts a {@link UserIdentity} by directly reading it from request headers.
 */
public class ProxyUserIdentityExtractor implements UserIdentityExtractor {

  private static final Logger LOG = LoggerFactory.getLogger(ProxyUserIdentityExtractor.class);
  public static final String NAME = "ProxyUserIdentityExtractor";

  private static final int EXPIRATION_SECS = 1000;

  private final String userIdentityHeader;

  @Inject
  public ProxyUserIdentityExtractor(CConfiguration cConf) {
    this.userIdentityHeader = cConf.get(Constants.Security.Authentication.PROXY_USER_ID_HEADER);
  }

  /**
   * Extracts the user identity from the HTTP request.
   *
   * <p>Expects the user's credential to be in the Authorization header in "Bearer" form. Expects the
   * user's identity to be in the configuration-specified header.
   *
   * @param request The HTTP Request to extract the user identity from
   * @return The extracted {@link UserIdentityPair}
   */
  @Override
  public UserIdentityExtractionResponse extract(HttpRequest request)
      throws UserIdentityExtractionException {
    long now = System.currentTimeMillis();
    if (userIdentityHeader == null) {
      return new UserIdentityExtractionResponse(UserIdentityExtractionState.ERROR_MISSING_IDENTITY,
          "User identity header config missing");
    }
    String userIdentity = request.headers().get(userIdentityHeader);
    if (userIdentity == null) {
      return new UserIdentityExtractionResponse(UserIdentityExtractionState.ERROR_MISSING_IDENTITY,
          "No user identity found");
    }

    UserIdentity identity = new UserIdentity(userIdentity, UserIdentity.IdentifierType.EXTERNAL,
        new LinkedHashSet<>(), now, now + EXPIRATION_SECS);

    // Parse the access token from authorization header. The header will be in
    // {@link io.cdap.cdap.proto.security.Credential.CredentialType#EXTERNAL_BEARER} form, if external
    // or {@link io.cdap.cdap.proto.security.Credential.CredentialType#INTERNAL} if internal
    String auth = request.headers().get(HttpHeaderNames.AUTHORIZATION);
    LOG.trace("Extracted user identity header '{}' and authorization header length '{}'",
        userIdentity,
        auth == null ? "NULL" : String.valueOf(auth.length()));
    String userCredential = null;
    String prefix = "";
    if (auth != null) {
      int idx = auth.trim().indexOf(' ');
      if (idx < 0) {
        return new UserIdentityExtractionResponse(new UserIdentityPair(null, identity));
      }
      prefix = auth.substring(0, idx).trim();
      userCredential = auth.substring(idx + 1).trim();
    }

    if (Credential.CREDENTIAL_TYPE_INTERNAL.equalsIgnoreCase(prefix)) {
      UserIdentity internalIdentity = new UserIdentity(userIdentity, IdentifierType.INTERNAL,
          new LinkedHashSet<>(), now, now + EXPIRATION_SECS);
      return new UserIdentityExtractionResponse(new UserIdentityPair(userCredential, internalIdentity));
    }

    return new UserIdentityExtractionResponse(new UserIdentityPair(userCredential, identity));
  }
}
