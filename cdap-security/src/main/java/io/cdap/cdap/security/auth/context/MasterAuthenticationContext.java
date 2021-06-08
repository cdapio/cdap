/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.security.auth.context;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.auth.AESCipher;
import io.cdap.cdap.security.auth.CipherException;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * An {@link AuthenticationContext} for HTTP requests in the Master. The authentication details in this context are
 * derived from:
 * <ol>
 *   <li>{@link SecurityRequestContext}, when the request; or</li>
 *   <li>{@link UserGroupInformation}, when the master itself is asynchronously updating privileges in the
 *   authorization policy cache.</li>
 * </ol>
 *
 * @see SecurityRequestContext
 * @see UserGroupInformation
 */
public class MasterAuthenticationContext implements AuthenticationContext {
  private final SConfiguration sConf;
  private final AESCipher aesCipher;

  @Inject
  public MasterAuthenticationContext(SConfiguration sConf) {
    this.sConf = sConf;
    this.aesCipher = new AESCipher();
  }

  @Override
  public Principal getPrincipal() {
    // When requests come in via rest endpoints, the userId is updated inside SecurityRequestContext, so give that
    // precedence.
    String userId = SecurityRequestContext.getUserId();
    String userCredential = SecurityRequestContext.getUserCredential(); //

    if (userCredential != null &&
      sConf.getBoolean(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_ENABLED, false)) {
      try {
        String key = sConf.get(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_PASSWORD);
        userCredential = new String(aesCipher.decrypt(key, Base64.getDecoder().decode(userCredential)),
                                    StandardCharsets.UTF_8);
      } catch (CipherException e) {
        throw Throwables.propagate(e);
      }
    }

    // This userId can be null, when the master itself is asynchoronously updating the policy cache, since
    // during that process the router will not set the SecurityRequestContext. In that case, obtain the userId from
    // the UserGroupInformation, which will be the user that the master is running as.
    if (userId == null) {
      try {
        userId = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    return new Principal(userId, Principal.PrincipalType.USER, userCredential);
  }
}
