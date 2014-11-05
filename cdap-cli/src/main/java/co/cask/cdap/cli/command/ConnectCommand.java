/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.client.PingClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.Credential;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import jline.console.ConsoleReader;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Properties;
import javax.inject.Inject;
import javax.net.ssl.SSLHandshakeException;

/**
 * Connects to a CDAP instance.
 */
public class ConnectCommand implements Command {

  private final CLIConfig cliConfig;
  private final FilePathResolver resolver;
  private final CConfiguration cConf;

  @Inject
  public ConnectCommand(CLIConfig cliConfig, FilePathResolver resolver, CConfiguration cConf) {
    this.cConf = cConf;
    this.cliConfig = cliConfig;
    this.resolver = resolver;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String uriString = arguments.get("cdap-instance-uri");
    if (!uriString.contains("://")) {
      uriString = "http://" + uriString;
    }

    URI uri = URI.create(uriString);
    String hostname = uri.getHost();
    boolean sslEnabled = "https".equals(uri.getScheme());
    int port = uri.getPort();

    if (port == -1) {
      port = sslEnabled ?
        cConf.getInt(Constants.Router.ROUTER_SSL_PORT) :
        cConf.getInt(Constants.Router.ROUTER_PORT);
    }

    ConnectionInfo connectionInfo = new ConnectionInfo(hostname, port, sslEnabled);
    tryConnect(cliConfig.getClientConfig(), connectionInfo, output, true);
  }

  @Override
  public String getPattern() {
    return "connect <cdap-instance-uri>";
  }

  @Override
  public String getDescription() {
    return "Connects to a CDAP instance. <credential(s)> "
      + "parameter(s) could be used if authentication is enabled in the gateway server.";
  }

  /**
   * Tries default connection specified by CConfiguration.
   */
  public void tryDefaultConnection(PrintStream output, boolean verbose) {
    CConfiguration cConf = CConfiguration.create();
    boolean sslEnabled = cConf.getBoolean(Constants.Security.SSL_ENABLED);
    String hostname = cConf.get(Constants.Router.ADDRESS);
    int port = sslEnabled ?
      cConf.getInt(Constants.Router.ROUTER_SSL_PORT) :
      cConf.getInt(Constants.Router.ROUTER_PORT);
    ConnectionInfo connectionInfo = new ConnectionInfo(hostname, port, sslEnabled);

    try {
      tryConnect(cliConfig.getClientConfig(), connectionInfo, output, verbose);
    } catch (Exception e) {
      // NO-OP
    }
  }

  private void tryConnect(ClientConfig clientConfig, ConnectionInfo connectionInfo,
                          PrintStream output, boolean verbose) throws Exception {

    try {
      AccessToken accessToken = acquireAccessToken(clientConfig, connectionInfo, output, verbose);
      checkConnection(clientConfig, connectionInfo, accessToken);
      cliConfig.setHostname(connectionInfo.getHostname());
      cliConfig.setPort(connectionInfo.getPort());
      cliConfig.setSSLEnabled(connectionInfo.isSSLEnabled());
      cliConfig.setAccessToken(accessToken);

      if (verbose) {
        output.printf("Successfully connected CDAP instance at %s:%d\n",
                      connectionInfo.getHostname(), connectionInfo.getPort());
      }
    } catch (SSLHandshakeException e) {
      // forward exception caused by invalid SSL certificates
      throw e;
    } catch (IOException e) {
      throw new IOException(String.format("Host %s on port %d could not be reached: %s",
                                          connectionInfo.getHostname(), connectionInfo.getPort(),
                                          e.getMessage()));
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
    PingClient pingClient = new PingClient(clientConfig);
    pingClient.ping();
  }

  private boolean isAuthenticationEnabled(ConnectionInfo connectionInfo) throws IOException {
    return getAuthenticationClient(connectionInfo).isAuthEnabled();
  }

  private AccessToken acquireAccessToken(ClientConfig clientConfig, ConnectionInfo connectionInfo, PrintStream output,
                                         boolean verbose) throws IOException {

    if (!isAuthenticationEnabled(connectionInfo)) {
      return null;
    }

    try {
      AccessToken savedAccessToken = getSavedAccessToken(connectionInfo.getHostname());
      checkConnection(clientConfig, connectionInfo, savedAccessToken);
      return savedAccessToken;
    } catch (UnAuthorizedAccessTokenException e) {
      // access token invalid - fall through to try acquiring token manually
    }

    AuthenticationClient authenticationClient = getAuthenticationClient(connectionInfo);

    Properties properties = new Properties();
    properties.put(BasicAuthenticationClient.VERIFY_SSL_CERT_PROP_NAME, String.valueOf(cliConfig.isVerifySSLCert()));

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

    if (accessToken != null && saveAccessToken(accessToken, connectionInfo.getHostname())) {
      if (verbose) {
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
      } catch (IOException e) {
        // Fall through
      }
    }
    return null;
  }

  private boolean saveAccessToken(AccessToken accessToken, String hostname) {
    File accessTokenFile = getAccessTokenFile(hostname);

    try {
      if (accessTokenFile.createNewFile()) {
        Files.write(accessToken.getValue(), accessTokenFile, Charsets.UTF_8);
        return true;
      }
    } catch (IOException e) {
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

  /**
   * Connection information to a CDAP instance.
   */
  private static final class ConnectionInfo {
    private final String hostname;
    private final int port;
    private final boolean sslEnabled;

    private ConnectionInfo(String hostname, int port, boolean sslEnabled) {
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
