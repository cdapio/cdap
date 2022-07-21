/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.cli;

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.cli.command.system.VersionCommand;
import io.cdap.cdap.cli.util.FilePathResolver;
import io.cdap.cdap.cli.util.table.AltStyleTableRenderer;
import io.cdap.cdap.cli.util.table.TableRenderer;
import io.cdap.cdap.cli.util.table.TableRendererConfig;
import io.cdap.cdap.client.MetaClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.client.exception.DisconnectedException;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authentication.client.AccessToken;
import io.cdap.cdap.security.authentication.client.AuthenticationClient;
import io.cdap.cdap.security.authentication.client.Credential;
import io.cdap.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import jline.TerminalFactory;
import jline.console.ConsoleReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Configuration for the CDAP CLI.
 */
public class CLIConfig implements TableRendererConfig {

  public static final String ENV_ACCESSTOKEN = "ACCESS_TOKEN";

  private static final int DEFAULT_LINE_WIDTH = 80;
  private static final int MIN_LINE_WIDTH = 40;

  private static final Gson GSON = new Gson();
  private final ClientConfig clientConfig;
  private final FilePathResolver resolver;
  private final String version;
  private final PrintStream output;
  private CLIConnectionConfig connectionConfig;

  private TableRenderer tableRenderer;
  private List<ConnectionChangeListener> connectionChangeListeners;
  private Supplier<Integer> lineWidthSupplier = new Supplier<Integer>() {
    @Override
    public Integer get() {
      try {
        return TerminalFactory.get().getWidth();
      } catch (Exception e) {
        return DEFAULT_LINE_WIDTH;
      }
    }
  };

  /*
   * Wrapper class for reading/writing of username + accessToken
   */
  private class UserAccessToken {
    private final AccessToken accessToken;
    private final String username;

    UserAccessToken(AccessToken accessToken, String username) {
      this.accessToken = accessToken;
      this.username = username;
    }

    public AccessToken getAccessToken() {
      return accessToken;
    }

    public String getUsername() {
      return username;
    }
  }

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

  public NamespaceId getCurrentNamespace() {
    if (connectionConfig == null || connectionConfig.getNamespace() == null) {
      throw new DisconnectedException(connectionConfig);
    }
    return connectionConfig.getNamespace();
  }

  public void setTableRenderer(TableRenderer tableRenderer) {
    this.tableRenderer = tableRenderer;
  }

  public void setConnectionConfig(@Nullable CLIConnectionConfig connectionConfig) {
    this.connectionConfig = connectionConfig;
    clientConfig.setConnectionConfig(connectionConfig);
    notifyConnectionChanged();
  }

  public void tryConnect(CLIConnectionConfig connectionConfig, boolean verifySSLCert,
                         PrintStream output, boolean debug) throws Exception {
    try {
      clientConfig.setVerifySSLCert(verifySSLCert);
      UserAccessToken userToken = acquireAccessToken(clientConfig, connectionConfig, output, debug);
      AccessToken accessToken = null;
      if (userToken != null) {
        accessToken = userToken.getAccessToken();
        connectionConfig = new CLIConnectionConfig(connectionConfig, connectionConfig.getNamespace(),
                                                   userToken.getUsername());
      }
      checkConnection(clientConfig, connectionConfig, accessToken);
      setConnectionConfig(connectionConfig);
      clientConfig.setAccessToken(accessToken);
      output.printf("Successfully connected to CDAP instance at %s", connectionConfig.getURI().toString());
      output.println();
    } catch (IOException e) {
      throw new IOException(String.format("CDAP instance at '%s' could not be reached: %s",
                                          connectionConfig.getURI().toString(), e.getMessage()), e);
    }
  }

  public void updateAccessToken(PrintStream output) throws IOException {
    UserAccessToken newAccessToken = getNewAccessToken(clientConfig.getConnectionConfig(), output, false);
    clientConfig.setAccessToken(newAccessToken.getAccessToken());
  }

  private void checkConnection(ClientConfig baseClientConfig, ConnectionConfig connectionInfo, AccessToken accessToken)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    ClientConfig clientConfig = new ClientConfig.Builder(baseClientConfig)
      .setConnectionConfig(connectionInfo)
      .setAccessToken(accessToken)
      .build();
    MetaClient metaClient = new MetaClient(clientConfig);

    try {
      metaClient.ping();
    } catch (IOException e) {
      throw new IOException("Check connection parameters and/or accesstoken", e);
    }
  }

  private boolean isAuthenticationEnabled(ConnectionConfig connectionInfo) throws IOException {
    return getAuthenticationClient(connectionInfo).isAuthEnabled();
  }

  @Nullable
  private UserAccessToken acquireAccessToken(
    ClientConfig clientConfig, ConnectionConfig connectionInfo, PrintStream output,
    boolean debug) throws IOException, UnauthorizedException {

    //Check for a saved token that works
    UserAccessToken savedToken = getSavedAccessToken(connectionInfo);
    if (savedToken != null) {
      return savedToken;
    }
    //Check if connection can be attempted without auth
    if (!isAuthenticationEnabled(connectionInfo)) {
      return null;
    }
    //auth enabled - try to get a new access token from server
    return getNewAccessToken(connectionInfo, output, debug);
  }

  private UserAccessToken getNewAccessToken(ConnectionConfig connectionInfo,
                                            PrintStream output, boolean debug) throws IOException {
    AuthenticationClient authenticationClient = getAuthenticationClient(connectionInfo);

    Properties properties = new Properties();
    properties.put(BasicAuthenticationClient.VERIFY_SSL_CERT_PROP_NAME,
                   String.valueOf(clientConfig.isVerifySSLCert()));

    String username = "";

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
      if (credential.getName().contains("username")) {
        username = credentialValue;
      }
    }

    authenticationClient.configure(properties);
    AccessToken accessToken = authenticationClient.getAccessToken();
    UserAccessToken userToken = new UserAccessToken(accessToken, username);
    if (accessToken != null) {
      if (saveAccessToken(userToken, connectionInfo.getHostname()) && debug) {
        output.printf("Saved access token to %s\n", getAccessTokenFile(connectionInfo.getHostname()).getAbsolutePath());
      }
    }

    return userToken;
  }

  private AuthenticationClient getAuthenticationClient(ConnectionConfig connectionInfo) {
    AuthenticationClient authenticationClient = new BasicAuthenticationClient();
    authenticationClient.setConnectionInfo(connectionInfo.getHostname(),
                                           connectionInfo.getPort() == null ? -1 : connectionInfo.getPort(),
                                           connectionInfo.isSSLEnabled());
    return authenticationClient;
  }

  @Nullable
  private UserAccessToken getSavedAccessToken(ConnectionConfig connectionInfo) {
    File file = getAccessTokenFile(connectionInfo.getHostname());
    try (BufferedReader reader = Files.newReader(file, Charsets.UTF_8)) {
      UserAccessToken userAccessToken = GSON.fromJson(reader, UserAccessToken.class);
      if (userAccessToken == null) {
        return null;
      }
      checkConnection(clientConfig, connectionInfo, userAccessToken.getAccessToken());
      return userAccessToken;
    } catch (IOException | JsonSyntaxException | UnauthenticatedException | UnauthorizedException ignored) {
      // Fall through
    }

    return null;
  }

  private boolean saveAccessToken(UserAccessToken accessToken, String hostname) {
    File accessTokenFile = getAccessTokenFile(hostname);

    try {
      Files.write(GSON.toJson(accessToken), accessTokenFile, Charsets.UTF_8);
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
      URL version = VersionCommand.class.getClassLoader().getResource("VERSION");
      if (version == null) {
        // This shouldn't happen
        throw new IllegalStateException("VERSION file not found");
      }
      return Resources.toString(version, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void setNamespace(NamespaceId namespace) {
    setConnectionConfig(new CLIConnectionConfig(connectionConfig, namespace));
  }

  public ClientConfig getClientConfig() {
    return clientConfig;
  }

  public CLIConnectionConfig getConnectionConfig() {
    return connectionConfig;
  }

  public String getVersion() {
    return version;
  }

  public void addHostnameChangeListener(ConnectionChangeListener listener) {
    this.connectionChangeListeners.add(listener);
  }

  private void notifyConnectionChanged() {
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(connectionConfig);
    }
  }

  public void setLineWidth(Supplier<Integer> lineWidthSupplier) {
    this.lineWidthSupplier = lineWidthSupplier;
  }

  public int getLineWidth() {
    return Math.max(MIN_LINE_WIDTH, lineWidthSupplier.get());
  }

  /**
   * Listener for hostname changes.
   */
  public interface ConnectionChangeListener {
    void onConnectionChanged(CLIConnectionConfig config);
  }
}
