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
  private static final String ENDPOINT_PREFIX = "call service <> <>";

  private final ServiceClient serviceClient;

  @Inject
  private EndpointSupplier(final ServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }

  @Override
  public Completer getCompleter(String prefix, Completer completer) {
    if (prefix != null && !prefix.isEmpty()) {
      String prefixMatch = prefix.replaceAll("<.+?>", "<>");
      if (METHOD_PREFIX.equals(prefixMatch)) {
        System.out.println(prefix);
        return new HttpMethodPrefixCompleter(serviceClient, prefix, (EndpointCompleter) completer);
      } else if (ENDPOINT_PREFIX.equals(prefixMatch)) {
        System.out.println(prefix);
        return new HttpEndpointPrefixCompleter(serviceClient, prefix, (EndpointCompleter) completer);
      }
    }
    return null;
  }
}
