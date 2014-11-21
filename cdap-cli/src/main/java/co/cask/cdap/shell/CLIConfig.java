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

package co.cask.cdap.shell;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.http.HttpRequestConfig;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import co.cask.cdap.shell.command.VersionCommand;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Configuration for the CDAP CLI.
 */
public class CLIConfig {

  public static final String PROP_VERIFY_SSL_CERT = "verify.ssl.cert";
  public static final String ENV_ACCESSTOKEN = "ACCESS_TOKEN";

  private static final int DEFAULT_PORT = 10000;
  private static final int DEFAULT_SSL_PORT = 10443;
  private static final boolean DEFAULT_SSL = false;

  private final ClientConfig clientConfig;
  private final String version;
  private final boolean verifySSLCert;

  private String hostname;
  private List<HostnameChangeListener> hostnameChangeListeners;
  private int port;
  private int sslPort;
  private URI uri;

  /**
   * @param hostname Hostname of the CDAP server to interact with (e.g. "example.com")
   */
  public CLIConfig(String hostname) {
    this.verifySSLCert = Boolean.valueOf(System.getProperty(PROP_VERIFY_SSL_CERT, "true"));
    this.hostname = Objects.firstNonNull(hostname, "localhost");
    this.port = DEFAULT_PORT;
    this.uri = URI.create(String.format("http://%s:%d", hostname, port));
    this.sslPort = DEFAULT_SSL_PORT;
    AuthenticationClient authenticationClient = new BasicAuthenticationClient();
    authenticationClient.setConnectionInfo(hostname, port, DEFAULT_SSL);
    this.clientConfig = new ClientConfig(hostname, port,
                                         new HttpRequestConfig(15000, 15000, verifySSLCert),
                                         new HttpRequestConfig(0, 0, verifySSLCert), null);
    this.version = tryGetVersion();
    this.hostnameChangeListeners = Lists.newArrayList();
  }

  private String tryGetVersion() {
    try {
      InputSupplier<? extends InputStream> versionFileSupplier = new InputSupplier<InputStream>() {
        @Override
        public InputStream getInput() throws IOException {
          return VersionCommand.class.getClassLoader().getResourceAsStream("VERSION");
        }
      };
      return CharStreams.toString(CharStreams.newReaderSupplier(versionFileSupplier, Charsets.UTF_8));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public boolean isVerifySSLCert() {
    return verifySSLCert;
  }

  public String getHost() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public int getSslPort() {
    return sslPort;
  }

  public ClientConfig getClientConfig() {
    return clientConfig;
  }

  public String getVersion() {
    return version;
  }

  public void setConnection(String hostname, int port, boolean ssl) throws URISyntaxException {
    this.hostname = hostname;
    if (ssl) {
      this.sslPort = port;
    } else {
      this.port = port;
    }
    this.uri = URI.create(String.format("%s://%s:%d", ssl ? "https" : "http", hostname, port));
    this.clientConfig.setHostnameAndPort(hostname, port, ssl);
    for (HostnameChangeListener listener : hostnameChangeListeners) {
      listener.onHostnameChanged(hostname);
    }
  }

  public void addHostnameChangeListener(HostnameChangeListener listener) {
    this.hostnameChangeListeners.add(listener);
  }

  public URI getURI() {
    return uri;
  }

  /**
   * Listener for hostname changes.
   */
  public interface HostnameChangeListener {
    void onHostnameChanged(String newHostname);
  }
}
