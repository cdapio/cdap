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

package io.cdap.cdap.security.spi.authenticator;

import io.cdap.cdap.proto.security.Credential;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * This interface expose methods for authenticating HTTP calls. It is used to set the {@code
 * Authorization} header in the form of
 * <pre>
 * {@code Authorization: type credentials}
 * </pre>
 */
public interface RemoteAuthenticator {

  /**
   * @return the name of the remote authenticator.
   */
  String getName();

  /**
   * Returns the credentials for the authentication.
   */
  @Nullable
  Credential getCredentials() throws IOException;

  /**
   * Returns the credentials for the authentication with scopes.
   */
  @Nullable
  Credential getCredentials(String scopes) throws IOException;
}
