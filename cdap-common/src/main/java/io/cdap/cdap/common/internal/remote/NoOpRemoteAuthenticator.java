/*
 * Copyright © 2022 Cask Data, Inc.
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

import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Returns no credentials.
 */
public class NoOpRemoteAuthenticator implements RemoteAuthenticator {

  public static final String NO_OP_REMOTE_AUTHENTICATOR_NAME = "no-op-remote-authenticator";

  @Override
  public String getName() {
    return NO_OP_REMOTE_AUTHENTICATOR_NAME;
  }

  @Nullable
  @Override
  public Credential getCredentials() throws IOException {
    return null;
  }

  /**
   * Returns the credentials for the authentication with scopes.
   */
  @Nullable
  @Override
  public Credential getCredentials(String scopes) throws IOException {
    return null;
  }
}
