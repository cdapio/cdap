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
 * the License
 */

package co.cask.cdap.cli.completer.supplier;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.completer.element.EndpointCompleter;
import co.cask.cdap.cli.completer.element.HttpEndpointPrefixCompleter;
import co.cask.cdap.cli.completer.element.HttpMethodPrefixCompleter;
import co.cask.cdap.client.ServiceClient;
import co.cask.common.cli.supplier.CompleterSupplier;
import com.google.inject.Inject;
import jline.console.completer.Completer;

/**
 *
 */
public class EndpointSupplier implements CompleterSupplier {

  private static final String METHOD_PREFIX = "call service <>";
  private static final String METHOD_PREFIX_WITH_APP_VERSION = "call service <> version <>";

  private static final String ENDPOINT_PREFIX = "call service <> <>";
  private static final String ENDPOINT_PREFIX_WITH_APP_VERSION = "call service <> version <> <>";

  private final ServiceClient serviceClient;
  private final CLIConfig cliConfig;

  @Inject
  private EndpointSupplier(final ServiceClient serviceClient, CLIConfig cliConfig) {
    this.serviceClient = serviceClient;
    this.cliConfig = cliConfig;
  }

  @Override
  public Completer getCompleter(String prefix, Completer completer) {
    if (prefix != null && !prefix.isEmpty()) {
      String prefixMatch = prefix.replaceAll("<.+?>", "<>");
      // Matches prefix "call service <app-id.service-id>" unless completer is a DefaultStringsCompleter containing
      // the non-argument token "version" (Referring to the method call
      // "getCompleter(childPrefix, new DefaultStringsCompleter(nonArgumentTokens)" in co.cask.common.cli.CLI.java).
      // Also matches "call service <app-id.service-id> version <app-version>".
      if ((METHOD_PREFIX.equals(prefixMatch) || METHOD_PREFIX_WITH_APP_VERSION.equals(prefixMatch))
        && completer instanceof EndpointCompleter) {
        return new HttpMethodPrefixCompleter(serviceClient, cliConfig, prefix, (EndpointCompleter) completer);
        // Matches prefix "call service <app-id.service-id> <http-method>" and
        // "call service <app-id.service-id> version <app-version> <http-method>".
      } else if (ENDPOINT_PREFIX.equals(prefixMatch) || ENDPOINT_PREFIX_WITH_APP_VERSION.equals(prefixMatch)) {
        return new HttpEndpointPrefixCompleter(serviceClient, cliConfig, prefix, (EndpointCompleter) completer);
      }
    }
    return null;
  }
}
