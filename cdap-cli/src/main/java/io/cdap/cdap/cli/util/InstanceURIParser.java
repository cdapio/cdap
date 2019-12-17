/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.cli.util;

import com.google.inject.Inject;
import io.cdap.cdap.cli.CLIConnectionConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;

import java.net.URI;

/**
 */
public class InstanceURIParser {

  public static final InstanceURIParser DEFAULT = new InstanceURIParser(CConfiguration.create());
  public static final String DEFAULT_PROTOCOL = "http";
  public static final String CDF_SUFFIX = "datafusion.googleusercontent.com";
  private final CConfiguration cConf;

  @Inject
  public InstanceURIParser(CConfiguration cConf) {
    this.cConf = cConf;
  }

  public CLIConnectionConfig parse(String uriString) {
    if (!uriString.contains("://")) {
      uriString = DEFAULT_PROTOCOL + "://" + uriString;
    }

    URI uri = URI.create(uriString);
    boolean sslEnabled = "https".equals(uri.getScheme());
    int port = uri.getPort();

    if (uriString.contains(CDF_SUFFIX)) {
      ConnectionConfig config = ConnectionConfig.builder()
          .setHostname(uri.getHost())
          .setPort(port)
          .setSSLEnabled(sslEnabled)
          .build();
      return new CLIConnectionConfig(config, NamespaceId.DEFAULT, null);
    }

    NamespaceId namespace =
      (uri.getPath() == null || uri.getPath().isEmpty() || "/".equals(uri.getPath())) ?
      NamespaceId.DEFAULT : new NamespaceId(uri.getPath().substring(1));
    String hostname = uri.getHost();

    if (port == -1) {
      port = sslEnabled ?
        cConf.getInt(Constants.Router.ROUTER_SSL_PORT) :
        cConf.getInt(Constants.Router.ROUTER_PORT);
    }

    ConnectionConfig config = ConnectionConfig.builder()
      .setHostname(hostname)
      .setPort(port)
      .setSSLEnabled(sslEnabled)
      .build();
    return new CLIConnectionConfig(config, namespace, null);
  }

}
