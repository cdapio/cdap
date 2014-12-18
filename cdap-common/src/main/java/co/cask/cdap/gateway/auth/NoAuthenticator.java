/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.gateway.auth;

import co.cask.cdap.common.conf.Constants;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Authenticator used when authentication is disabled.
 */
public class NoAuthenticator implements Authenticator {

  @Override
  public boolean authenticateRequest(HttpRequest request) {
    return true;
  }

  @Override
  public String getAccountId(HttpRequest httpRequest) {
    return Constants.DEFAULT_NAMESPACE;
  }

  @Override
  public boolean isAuthenticationRequired() {
    return false;
  }

}
