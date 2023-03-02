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

package io.cdap.cdap.common.internal.remote;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import java.util.function.BiConsumer;

/**
 * A class which sets internal authenticated headers for the remote client using an {@link
 * AuthenticationContext} as the source.
 */
public class DefaultInternalAuthenticator implements InternalAuthenticator {

  private final AuthenticationContext authenticationContext;

  @Inject
  public DefaultInternalAuthenticator(AuthenticationContext authenticationContext) {
    this.authenticationContext = authenticationContext;
  }

  @Override
  public void applyInternalAuthenticationHeaders(BiConsumer<String, String> headerSetter) {
    Principal principal = authenticationContext.getPrincipal();
    String userID = null;
    Credential internalCredentials = null;
    if (principal != null) {
      userID = principal.getName();
      internalCredentials = principal.getFullCredential();
    }
    if (internalCredentials != null) {
      headerSetter.accept(Constants.Security.Headers.RUNTIME_TOKEN,
          String.format("%s %s", internalCredentials.getType().getQualifiedName(),
              internalCredentials.getValue()));
    }
    if (userID != null) {
      headerSetter.accept(Constants.Security.Headers.USER_ID, userID);
    }
  }
}
