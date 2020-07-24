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

package io.cdap.cdap.cli;

import io.cdap.cdap.client.config.ConnectionConfig;

import javax.annotation.Nullable;

/**
 * Contains the options that the user can pass to the CLI upon launch.
 */
public class LaunchOptions {

  public static final LaunchOptions DEFAULT = builder()
    .setUri(ConnectionConfig.DEFAULT.getURI().toString())
    .build();

  private final String uri;
  private final boolean autoconnect;
  private final boolean debug;
  private final boolean verifySSL;
  private final String namespace;
  private final String instanceURI;

  public LaunchOptions(String uri, boolean autoconnect, boolean debug, boolean verifySSL) {
    this(uri, autoconnect, debug, verifySSL, null, null);
  }

  public LaunchOptions(String uri, boolean autoconnect, boolean debug, boolean verifySSL,
                       String namespace, @Nullable String instanceURI) {
    this.uri = uri;
    this.autoconnect = autoconnect;
    this.debug = debug;
    this.verifySSL = verifySSL;
    this.namespace = namespace;
    this.instanceURI = instanceURI;
  }

  public String getUri() {
    return uri;
  }

  public String getNamespace() {
    return namespace;
  }

  public boolean isAutoconnect() {
    return autoconnect;
  }

  public boolean isDebug() {
    return debug;
  }

  public boolean isVerifySSL() {
    return verifySSL;
  }

  public String getInstanceURI() {
    return instanceURI;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link LaunchOptions}.
   */
  public static class Builder {
    private String uri;
    private boolean autoconnect;
    private boolean debug;
    private boolean verifySSL;
    private String namespace;
    private String instanceURI;

    public Builder setUri(String uri) {
      this.uri = uri;
      return this;
    }

    public Builder setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder setAutoconnect(boolean autoconnect) {
      this.autoconnect = autoconnect;
      return this;
    }

    public Builder setDebug(boolean debug) {
      this.debug = debug;
      return this;
    }

    public Builder setVerifySSL(boolean verifySSL) {
      this.verifySSL = verifySSL;
      return this;
    }

    public Builder setInstanceURI(String instanceURI) {
      this.instanceURI = instanceURI;
      return this;
    }

    public LaunchOptions build() {
      return new LaunchOptions(uri, autoconnect, debug, verifySSL, namespace, instanceURI);
    }
  }
}
