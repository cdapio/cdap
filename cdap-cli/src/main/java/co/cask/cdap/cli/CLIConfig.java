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

import co.cask.cdap.cli.command.VersionCommand;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.cli.util.table.AltStyleTableRenderer;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.Credential;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import jline.console.ConsoleReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * Configuration for the CDAP CLI.
 */
public class CLIConfig {

  public static final String ENV_ACCESSTOKEN = "ACCESS_TOKEN";

  private final ClientConfig clientConfig;
  private final FilePathResolver resolver;
  private final String version;
  private final PrintStream output;
  private final TableRenderer tableRenderer;

  private List<ConnectionChangeListener> connectionChangeListeners;
  private ConnectionInfo connectionInfo;

  /**
   * @param clientConfig client configuration
   */
  public CLIConfig(ClientConfig clientConfig, PrintStream output, TableRenderer tableRenderer) {
    this.clientConfig = clientConfig;
    this.output = output;
    this.tableRenderer = tableRenderer;
    this.resolver = new FilePathResolver();
    this.version = tryGetVersion();
    this.connectionChangeListeners = Lists.newArrayList();
  }

  public CLIConfig() {
    this(ClientConfig.builder().build(), System.out, new AltStyleTableRenderer());
  }

  public PrintStream getOutput() {
    return output;
  }

  public TableRenderer getTableRenderer() {
    return tableRenderer;
  }

  public Id.Namespace getCurrentNamespace() {
    return clientConfig.getNamespace();
  }

  public void setCurrentNamespace(Id.Namespace currentNamespace) {
    clientConfig.setNamespace(currentNamespace);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(clientConfig);
    }
  }

  public void tryConnect(ConnectionInfo connectionInfo, PrintStream output, boolean debug) throws Exception {
    this.connectionInfo = connectionInfo;
    try {
      AccessToken accessToken = acquireAccessToken(clientConfig, connectionInfo, output, debug);
      checkConnection(clientConfig, connectionInfo, accessToken);
      setHostname(connectionInfo.getHostname());
      setPort(connectionInfo.getPort());
      setCurrentNamespace(connectionInfo.getNamespace());
      setSSLEnabled(connectionInfo.isSSLEnabled());
      setAccessToken(accessToken);

      output.printf("Successfully connected CDAP instance at %s:%d\n",
                    connectionInfo.getHostname(), connectionInfo.getPort());
    } catch (IOException e) {
      throw new IOException(String.format("Host %s on port %d could not be reached: %s",
                                          connectionInfo.getHostname(), connectionInfo.getPort(),
                                          e.getMessage()));
    }
  }

  public void updateAccessToken(PrintStream output) throws IOException {
    if (connectionInfo != null) {
      setAccessToken(getNewAccessToken(connectionInfo, output, false));
    }
  }

  private void checkConnection(ClientConfig baseClientConfig,
                               ConnectionInfo connectionInfo,
                               AccessToken accessToken) throws IOException, UnauthorizedException {
    ClientConfig clientConfig = new ClientConfig.Builder(baseClientConfig)
      .setHostname(connectionInfo.getHostname())
      .setPort(connectionInfo.getPort())
      .setSSLEnabled(connectionInfo.isSSLEnabled())
      .setAccessToken(accessToken)
      .build();
    MetaClient metaClient = new MetaClient(clientConfig);
    metaClient.ping();
  }

  private boolean isAuthenticationEnabled(ConnectionInfo connectionInfo) throws IOException {
    return getAuthenticationClient(connectionInfo).isAuthEnabled();
  }

  private AccessToken acquireAccessToken(ClientConfig clientConfig, ConnectionInfo connectionInfo,
                                         PrintStream output, boolean debug) throws IOException {

    if (!isAuthenticationEnabled(connectionInfo)) {
      return null;
    }

    try {
      AccessToken savedAccessToken = getSavedAccessToken(connectionInfo.getHostname());
      checkConnection(clientConfig, connectionInfo, savedAccessToken);
      return savedAccessToken;
    } catch (UnauthorizedException ignored) {
      // access token invalid - fall through to try acquiring token manually
    }

    return getNewAccessToken(connectionInfo, output, debug);
  }

  private AccessToken getNewAccessToken(ConnectionInfo connectionInfo, PrintStream output,
                                        boolean debug) throws IOException {

    AuthenticationClient authenticationClient = getAuthenticationClient(connectionInfo);

    Properties properties = new Properties();
    properties.put(BasicAuthenticationClient.VERIFY_SSL_CERT_PROP_NAME, String.valueOf(isVerifySSLCert()));

    // obtain new access token via manual user input
    output.printf("Authentication is enabled in the CDAP instance: %s.\n", connectionInfo.getHostname());
    ConsoleReader reader = new ConsoleReader();
    for (Credential credential : authenticationClient.getRequiredCredentials()) {
      String prompt = "Please, specify " + credential.getDescription() + "> ";
      String credentialValue;
      if (credential.isSecret()) {
        credentialValue = reader.readLine(prompt, '*');
      } else {
        credentialValue = reader.readLine(prompt);
      }
      properties.put(credential.getName(), credentialValue);
    }

    authenticationClient.configure(properties);
    AccessToken accessToken = authenticationClient.getAccessToken();

    if (accessToken != null) {
      if (saveAccessToken(accessToken, connectionInfo.getHostname()) && debug) {
        output.printf("Saved access token to %s\n", getAccessTokenFile(connectionInfo.getHostname()).getAbsolutePath());
      }
    }

    return accessToken;
  }

  private AuthenticationClient getAuthenticationClient(ConnectionInfo connectionInfo) {
    AuthenticationClient authenticationClient = new BasicAuthenticationClient();
    authenticationClient.setConnectionInfo(connectionInfo.getHostname(), connectionInfo.getPort(),
                                           connectionInfo.isSSLEnabled());
    return authenticationClient;
  }

  private AccessToken getSavedAccessToken(String hostname) {
    File file = getAccessTokenFile(hostname);
    if (file.exists() && file.canRead()) {
      try {
        String tokenString = Joiner.on("").join(Files.readLines(file, Charsets.UTF_8));
        return new AccessToken(tokenString, -1L, null);
      } catch (IOException ignored) {
        // Fall through
      }
    }
    return null;
  }

  private boolean saveAccessToken(AccessToken accessToken, String hostname) {
    File accessTokenFile = getAccessTokenFile(hostname);

    try {
      accessTokenFile.createNewFile();
      Files.write(accessToken.getValue(), accessTokenFile, Charsets.UTF_8);
      return true;
    } catch (IOException ignored) {
      // NO-OP
    }

    return false;
  }

  private File getAccessTokenFile(String hostname) {
    String accessTokenEnv = System.getenv(CLIConfig.ENV_ACCESSTOKEN);
    if (accessTokenEnv != null) {
      return resolver.resolvePathToFile(accessTokenEnv);
    }

    return resolver.resolvePathToFile("~/.cdap.accesstoken." + hostname);
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

  public ClientConfig getClientConfig() {
    return clientConfig;
  }

  public String getHost() {
    return clientConfig.getHostname();
  }

  public URI getURI() {
    return clientConfig.getBaseURI();
  }

  public boolean isVerifySSLCert() {
    return clientConfig.isVerifySSLCert();
  }

  public String getVersion() {
    return version;
  }

  public void setHostname(String hostname) {
    clientConfig.setHostname(hostname);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(clientConfig);
    }
  }

  public void setPort(int port) {
    clientConfig.setPort(port);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(clientConfig);
    }
  }

  public void setSSLEnabled(boolean sslEnabled) {
    clientConfig.setSSLEnabled(sslEnabled);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(clientConfig);
    }
  }

  public void setAccessToken(AccessToken accessToken) {
    clientConfig.setAccessToken(accessToken);
  }

  public void addHostnameChangeListener(ConnectionChangeListener listener) {
    this.connectionChangeListeners.add(listener);
  }

  /**
   * Listener for hostname changes.
   */
  public interface ConnectionChangeListener {
    void onConnectionChanged(ClientConfig clientConfig);
  }

  /**
   * Connection information to a CDAP instance.
   */
  public static final class ConnectionInfo {

    private final String hostname;
    private final int port;
    private final boolean sslEnabled;
    private final Id.Namespace namespace;

    public ConnectionInfo(String hostname, int port, boolean sslEnabled, Id.Namespace namespace) {
      this.hostname = hostname;
      this.port = port;
      this.sslEnabled = sslEnabled;
      this.namespace = namespace;
    }

    public ConnectionInfo(String hostname, int port, boolean sslEnabled) {
      this(hostname, port, sslEnabled, Constants.DEFAULT_NAMESPACE_ID);
    }

    public static ConnectionInfo of(ClientConfig clientConfig) {
      return new ConnectionInfo(clientConfig.getHostname(), clientConfig.getPort(), clientConfig.isSSLEnabled(),
                                clientConfig.getNamespace());
    }

    public Id.Namespace getNamespace() {
      return namespace;
    }

    public String getHostname() {
      return hostname;
    }

    public int getPort() {
      return port;
    }

    public boolean isSSLEnabled() {
      return sslEnabled;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(hostname, port, sslEnabled, namespace);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final ConnectionInfo other = (ConnectionInfo) obj;
      return Objects.equal(this.hostname, other.hostname) &&
        Objects.equal(this.port, other.port) &&
        Objects.equal(this.sslEnabled, other.sslEnabled) &&
        Objects.equal(this.namespace, other.namespace);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("hostname", hostname)
        .add("port", port)
        .add("sslEnabled", sslEnabled)
        .add("namespace", namespace)
        .toString();
    }
  }
}
