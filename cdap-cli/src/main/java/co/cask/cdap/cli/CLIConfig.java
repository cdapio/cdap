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
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.Credential;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
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

  private final ClientConfig clientConfig;
  private final FilePathResolver resolver;
  private final String version;
  private final String accessTokenFile;
  private final boolean autoconnect;

  private List<ConnectionChangeListener> connectionChangeListeners;

  /**
   * @param launchConfig initial configuration
   */
  public CLIConfig(CLILaunchConfig launchConfig) {
    this.autoconnect = launchConfig.isAutoconnect();
    this.clientConfig = launchConfig.createClientConfig();
    this.accessTokenFile = launchConfig.getAccessTokenFile();
    this.resolver = new FilePathResolver();
    this.version = tryGetVersion();
    this.connectionChangeListeners = Lists.newArrayList();
  }

  public String getCurrentNamespace() {
    return clientConfig.getNamespace();
  }

  public void setCurrentNamespace(String currentNamespace) {
    clientConfig.setNamespace(currentNamespace);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(currentNamespace, clientConfig.getBaseURI());
    }
  }

  public void tryConnect(PrintStream output, boolean verbose) throws Exception {
    ClientConfig newClientConfig = new ClientConfig.Builder(clientConfig).build();

    try {
      if (!isAuthenticationEnabled(connectionInfo)) {
        return null;
      }
      AccessToken accessToken = acquireAccessToken(clientConfig, connectionInfo, output, verbose);
      checkConnection(clientConfig, connectionInfo, accessToken);
      setHostname(connectionInfo.getHostname());
      setPort(connectionInfo.getPort());
      setCurrentNamespace(Constants.DEFAULT_NAMESPACE);
      setSSLEnabled(connectionInfo.isSSLEnabled());
      setAccessToken(accessToken);

      clientConfig.set(newClientConfig);
      if (verbose) {
        output.printf("Successfully connected CDAP instance at %s:%d\n",
                      connectionInfo.getHostname(), connectionInfo.getPort());
      }
    } catch (IOException e) {
      throw new IOException(String.format("Host %s on port %d could not be reached: %s",
                                          connectionInfo.getHostname(), connectionInfo.getPort(),
                                          e.getMessage()));
    }
  }

  private AuthenticationClient getAuthenticationClient(ClientConfig clientConfig) {
    AuthenticationClient authenticationClient = new BasicAuthenticationClient();
    authenticationClient.setConnectionInfo(clientConfig.getHostname(), clientConfig.getPort(),
                                           clientConfig.isSSLEnabled());
    return authenticationClient;
  }

  public void updateAccessToken(PrintStream output) throws IOException {
    if (connectionInfo != null) {
      setAccessToken(getNewAccessToken(connectionInfo, output, false));
    }
  }

  private void checkConnection(ClientConfig baseClientConfig,
                               ConnectionInfo connectionInfo,
                               AccessToken accessToken) throws IOException, UnAuthorizedAccessTokenException {
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

  private AccessToken acquireAccessToken(ClientConfig clientConfig, ConnectionInfo connectionInfo, PrintStream output,
                                         boolean verbose) throws IOException {



    try {
      AccessToken savedAccessToken = getSavedAccessToken(connectionInfo.getHostname());
      checkConnection(clientConfig, connectionInfo, savedAccessToken);
      return savedAccessToken;
    } catch (UnAuthorizedAccessTokenException ignored) {
      // access token invalid - fall through to try acquiring token manually
    }

    return getNewAccessToken(connectionInfo, output, verbose);
  }

  private AccessToken getNewAccessToken(ConnectionInfo connectionInfo, PrintStream output,
                                        boolean verbose) throws IOException {

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
      if (saveAccessToken(accessToken, connectionInfo.getHostname()) && verbose) {
        output.printf("Saved access token to %s\n", getAccessTokenFile(connectionInfo.getHostname()).getAbsolutePath());
      }
    }

    return accessToken;
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
      listener.onConnectionChanged(clientConfig.getNamespace(), clientConfig.getBaseURI());
    }
  }

  public void setPort(int port) {
    clientConfig.setPort(port);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(clientConfig.getNamespace(), clientConfig.getBaseURI());
    }
  }

  public void setSSLEnabled(boolean sslEnabled) {
    clientConfig.setSSLEnabled(sslEnabled);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(clientConfig.getNamespace(), clientConfig.getBaseURI());
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
    void onConnectionChanged(String currentNamespace, URI newURI);
  }
}
