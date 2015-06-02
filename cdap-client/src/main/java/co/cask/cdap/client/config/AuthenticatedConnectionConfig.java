/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.client.config;

import java.net.URI;

/**
 * {@link ConnectionConfig} that stores a username when available. Helps keep connectionConfig thread safe.
 */
public class AuthenticatedConnectionConfig extends ConnectionConfig {
  private final String username;

  public AuthenticatedConnectionConfig(ConnectionConfig connectInfo, String username) {
    super(connectInfo.getNamespace(), connectInfo.getHostname(), connectInfo.getPort(), connectInfo.isSSLEnabled());
    this.username = username + "@";
  }

  @Override
  public URI getURI() {
    return URI.create(String.format("%s://%s%s:%d", super.isSSLEnabled() ? "https" : "http", username,
      super.getHostname(), super.getPort()));
  }
}

