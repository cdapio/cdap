/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.cli;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;

import java.net.URI;

/**
 * Launch configuration for the CDAP CLI.
 */
public class CLILaunchConfig {

  private final String uri;
  private final String accessTokenFile;
  private final boolean verifySSL;
  private final boolean autoconnect;

  /**
   * @param uri URI of the CDAP server to interact with in
   *            the format "[<http|https>://]<hostname>[:<port>[/<namespace>]]"
   *            (e.g. "http://127.0.0.1:10000/default").
   */
  public CLILaunchConfig(String uri, String accessTokenFile, boolean verifySSL, boolean autoconnect) {
    this.uri = uri;
    this.accessTokenFile = accessTokenFile;
    this.verifySSL = verifySSL;
    this.autoconnect = autoconnect;
  }

  public String getUri() {
    return uri;
  }

  public String getAccessTokenFile() {
    return accessTokenFile;
  }

  public boolean isVerifySSL() {
    return verifySSL;
  }

  public boolean isAutoconnect() {
    return autoconnect;
  }

  public ClientConfig createClientConfig() {
    ConnectionConfig.Builder connectionBuilder = new ConnectionConfig.Builder();
    connectionBuilder.setUri(URI.create(uri));
    if (accessTokenFile != null) {
      connectionBuilder.setAccessToken()
    }

    ClientConfig.Builder builder = new ClientConfig.Builder();
    builder.setVerifySSLCert(verifySSL);
//    builder.setConnectionConfig();
    return builder.build();
  }
}
