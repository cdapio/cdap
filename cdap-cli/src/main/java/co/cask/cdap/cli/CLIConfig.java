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
import co.cask.cdap.common.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.Credential;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import jline.console.ConsoleReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.core.HttpHeaders;

/**
 * Configuration for the CDAP CLI.
 */
public class CLIConfig {

  public static final String PROP_VERIFY_SSL_CERT = "verify.ssl.cert";
  public static final String ENV_ACCESSTOKEN = "ACCESS_TOKEN";

  private final ClientConfig clientConfig;
  private final FilePathResolver resolver;
  private final String version;
  private final boolean hostnameProvided;

  private List<ConnectionChangeListener> connectionChangeListeners;
  private ConnectionInfo connectionInfo;

  private String currentNamespace;
  private String currentUser = null;

  /**
   * @param hostname Hostname of the CDAP server to interact with (e.g. "example.com")
   */
  public CLIConfig(String hostname) {
    this.currentNamespace = co.cask.cdap.common.conf.Constants.DEFAULT_NAMESPACE;
    this.hostnameProvided = hostname != null && !hostname.isEmpty();
    this.clientConfig = createClientConfig(hostname);
    this.resolver = new FilePathResolver();
    this.version = tryGetVersion();
    this.connectionChangeListeners = Lists.newArrayList();
  }

  public CLIConfig() {
    this(null);
  }

  private ClientConfig createClientConfig(String hostname) {
    ClientConfig.Builder clientConfigBuilder = new ClientConfig.Builder();
    if (hostname != null) {
      clientConfigBuilder.setHostname(hostname);
    }
    if (System.getProperty(PROP_VERIFY_SSL_CERT) != null) {
      clientConfigBuilder.setVerifySSLCert(Boolean.parseBoolean(System.getProperty(PROP_VERIFY_SSL_CERT)));
    }
    return clientConfigBuilder.build();
  }

  public String getCurrentNamespace() {
    return currentNamespace;
  }

  public void setCurrentNamespace(String currentNamespace) {
    this.currentNamespace = currentNamespace;
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(currentNamespace, clientConfig.getBaseURI());
    }
  }

  public boolean isHostnameProvided() {
    return hostnameProvided;
  }

  public void tryConnect(ConnectionInfo connectionInfo, PrintStream output,
                         boolean useSavedToken, boolean verbose) throws Exception {
    this.connectionInfo = connectionInfo;
    try {
      AccessToken accessToken = acquireAccessToken(clientConfig, connectionInfo, output, useSavedToken, verbose);
      checkConnection(clientConfig, connectionInfo, accessToken);
      setHostname(connectionInfo.getHostname());
      setPort(connectionInfo.getPort());
      setSSLEnabled(connectionInfo.isSSLEnabled());
      setAccessToken(accessToken);
      setCurrentNamespace("default");

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
                                         boolean useSavedToken, boolean verbose) throws IOException {

    if (!isAuthenticationEnabled(connectionInfo)) {
      return null;
    }

    if (useSavedToken) {
      try {
        AccessToken savedAccessToken = getSavedAccessToken(connectionInfo.getHostname());
        checkConnection(clientConfig, connectionInfo, savedAccessToken);
        return savedAccessToken;
      } catch (UnAuthorizedAccessTokenException ignored) {
        // access token invalid - fall through to try acquiring token manually
      }
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

      if (credential.getName().endsWith(".username")) {
        setCurrentUser(credentialValue);
      }
      properties.put(credential.getName(), credentialValue);
    }

    authenticationClient.configure(properties);
    AccessToken accessToken = authenticationClient.getAccessToken();
    clientConfig.setAccessToken(accessToken);

    if (accessToken != null) {
      if (saveAccessToken(accessToken, connectionInfo.getHostname()) && verbose) {
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
      listener.onConnectionChanged(currentNamespace, clientConfig.getBaseURI());
    }
  }

  public void setPort(int port) {
    clientConfig.setPort(port);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(currentNamespace, clientConfig.getBaseURI());
    }
  }

  public void setSSLEnabled(boolean sslEnabled) {
    clientConfig.setSSLEnabled(sslEnabled);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(currentNamespace, clientConfig.getBaseURI());
    }
  }

  public void setCurrentUser(String user) {
    currentUser = user;
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(currentNamespace, clientConfig.getBaseURI());
    }
  }

  public void setAccessToken(AccessToken accessToken) {
    clientConfig.setAccessToken(accessToken);
  }

  public void addHostnameChangeListener(ConnectionChangeListener listener) {
    this.connectionChangeListeners.add(listener);
  }

  public String getPrompt() {
    return "cdap (" + (currentUser != null ? currentUser + " @ " : "") + getURI() + "/" + getCurrentNamespace() + ")> ";
  }

  public Supplier<URI> getURISupplier() {
    return new Supplier<URI>() {
      @Override
      public URI get() {
        return getURI();
      }
    };
  }

  public Supplier<Multimap<String, String>> getHeadersSupplier() {
    return new Supplier<Multimap<String, String>>() {
      @Override
      public Multimap<String, String> get() {
        return getHeaders();
      }
    };
  }

  private Multimap<String, String> getHeaders() {
    HashMultimap<String, String> headers = HashMultimap.create();
    AccessToken accessToken = clientConfig.getAccessToken();
    if (accessToken != null) {
      headers.put(HttpHeaders.AUTHORIZATION, accessToken.getTokenType() + " " + accessToken.getValue());
    }
    return headers;
  }

  /**
   * Listener for hostname changes.
   */
  public interface ConnectionChangeListener {
    void onConnectionChanged(String currentNamespace, URI newURI);
  }

  /**
   * Connection information to a CDAP instance.
   */
  public static final class ConnectionInfo {
    private final String hostname;
    private final int port;
    private final boolean sslEnabled;

    public ConnectionInfo(String hostname, int port, boolean sslEnabled) {
      this.hostname = hostname;
      this.port = port;
      this.sslEnabled = sslEnabled;
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
  }
}
