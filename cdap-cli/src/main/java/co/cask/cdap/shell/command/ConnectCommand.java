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

import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.Credential;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.CLIConfig;
import co.cask.cdap.shell.util.SocketUtil;
import com.google.gson.Gson;
import jline.console.ConsoleReader;

import java.io.Console;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Properties;
import javax.inject.Inject;

/**
 * Connects to a CDAP instance.
 */
public class ConnectCommand extends AbstractCommand {

  private final CLIConfig cliConfig;

  @Inject
  public ConnectCommand(CLIConfig cliConfig) {
    super("connect", "<cdap-hostname>", "Connects to a CDAP instance. <credential(s)> " +
          "parameter(s) could be used if authentication is enabled in the gateway server.");
    this.cliConfig = cliConfig;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String uriString = args[0];
    URI uri = URI.create(uriString);

    String hostname;
    boolean ssl = false;
    int port = cliConfig.getPort();

    if (uri.getScheme() == null && uri.getHost() == null) {
      hostname = uriString;
    } else {
      hostname = uri.getHost();
      ssl = "https".equals(uri.getScheme());
      port = uri.getPort();
    }

    if (port == -1) {
      if (ssl) {
        port = cliConfig.getSslPort();
      } else {
        port = cliConfig.getPort();
      }
    }

    if (!SocketUtil.isAvailable(hostname, port)) {
      throw new IOException(String.format("Host %s on port %d could not be reached", hostname, port));
    }

    AuthenticationClient authenticationClient = new BasicAuthenticationClient();
    authenticationClient.setConnectionInfo(hostname, port, ssl);
    Properties properties = new Properties();
    properties.put(BasicAuthenticationClient.VERIFY_SSL_CERT_PROP_NAME, String.valueOf(cliConfig.isVerifySSLCert()));

    if (authenticationClient.isAuthEnabled()) {
      output.printf("Authentication is enabled in the gateway server: %s.\n", hostname);
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
      cliConfig.getClientConfig().setAuthenticationClient(authenticationClient);
      authenticationClient.getAccessToken();
    }

    cliConfig.setConnection(hostname, port, ssl);
    output.printf("Successfully connected CDAP instance at %s:%d\n", hostname, port);
  }
}
