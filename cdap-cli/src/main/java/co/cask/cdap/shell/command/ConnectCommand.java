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

package co.cask.cdap.shell.command;

import co.cask.cdap.client.PingClient;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.Credential;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.CLIConfig;
import co.cask.cdap.shell.util.SocketUtil;
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

/**
 * Connects to a CDAP instance.
 */
public class ConnectCommand extends AbstractCommand {

  private static final String ENV_HOME = System.getProperty("user.home");

  private final CLIConfig cliConfig;

  @Inject
  public ConnectCommand(CLIConfig cliConfig) {
    super("connect", "<cdap-hostname>", "Connects to a CDAP instance. <credential(s)> "
      + "parameter(s) could be used if authentication is enabled in the gateway server.");
    this.cliConfig = cliConfig;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String uriString = args[0];
    if (!uriString.contains("://")) {
      uriString = "http://" + uriString;
    }

    URI uri = URI.create(uriString);
    String hostname = uri.getHost();
    boolean ssl = "https".equals(uri.getScheme());
    int port = uri.getPort();

    if (port == -1) {
      if (ssl) {
        port = cliConfig.getSslPort();
      } else {
        port = cliConfig.getPort();
      }
    }

    tryConnect(output, hostname, port, ssl, getAccessToken(hostname), true);
  }

  /**
   * Tries to read cdap-site.xml to set host and port. Otherwise, uses default values.
   */
  public void tryDefaultConnection(PrintStream output, boolean verbose) {
    CConfiguration cConf = CConfiguration.create();
    boolean sslEnabled = cConf.getBoolean(Constants.Security.SSL_ENABLED, false);
    String hostname = cConf.get(Constants.Router.ADDRESS, "localhost");
    int port = sslEnabled ?
      cConf.getInt(Constants.Router.ROUTER_SSL_PORT, 10443) :
      cConf.getInt(Constants.Router.ROUTER_PORT, 10000);

    try {
      tryConnect(output, hostname, port, sslEnabled, getAccessToken(hostname), verbose);
    } catch (Exception e) {
      // NO-OP
    }
  }

  private void tryConnect(PrintStream output, String hostname, int port, boolean ssl,
                          String accessTokenString, boolean verbose) throws Exception {
    if (!SocketUtil.isAvailable(hostname, port)) {
      throw new IOException(String.format("Host %s on port %d could not be reached", hostname, port));
    }

    AuthenticationClient authenticationClient = new BasicAuthenticationClient();
    authenticationClient.setConnectionInfo(hostname, port, ssl);
    Properties properties = new Properties();
    properties.put(BasicAuthenticationClient.VERIFY_SSL_CERT_PROP_NAME, String.valueOf(cliConfig.isVerifySSLCert()));

    if (authenticationClient.isAuthEnabled()) {
      // try connecting using provided access token first
      if (accessTokenString != null) {
        cliConfig.setConnection(hostname, port, ssl);
        cliConfig.getClientConfig().setAccessToken(new AccessToken(accessTokenString, -1L, null));
        PingClient pingClient = new PingClient(cliConfig.getClientConfig());
        try {
          pingClient.ping();
          // successfully connected using provided access token
          return;
        } catch (UnAuthorizedAccessTokenException e) {
          // fall through to try connecting manually
        }
      }

      // connect via manual user input
      output.printf("Authentication is enabled in the CDAP instance: %s.\n", hostname);
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
      if (accessToken == null) {
        output.printf("Invalid credentials\n");
        return;
      }

      if (saveAccessToken(accessToken, hostname)) {
        if (verbose) {
          output.printf("Saved access token to %s\n", getAccessTokenFile(hostname).getAbsolutePath());
        }
      }
      cliConfig.getClientConfig().setAccessToken(accessToken);
    }

    cliConfig.setConnection(hostname, port, ssl);
    if (verbose) {
      output.printf("Successfully connected CDAP instance at %s:%d\n", hostname, port);
    }
  }

  private String getAccessToken(String hostname) {
    File file = getAccessTokenFile(hostname);
    if (file.exists() && file.canRead()) {
      try {
        return Joiner.on("").join(Files.readLines(file, Charsets.UTF_8));
      } catch (IOException e) {
        // Fall through
      }
    }
    return null;
  }

  private File getAccessTokenFile(String hostname) {
    String accessTokenEnv = System.getenv(CLIConfig.ENV_ACCESSTOKEN);
    if (accessTokenEnv != null) {
      return new File(accessTokenEnv);
    }

    return new File(new File(ENV_HOME), ".cdap.accesstoken." + hostname);
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
}
