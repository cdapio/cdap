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

package co.cask.cdap.cli;

import co.cask.cdap.client.config.ConnectionConfig;

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
  private final String scriptFile;
  private final boolean scriptFileFirst;

  public LaunchOptions(String uri, boolean autoconnect, boolean debug, 
                       boolean verifySSL, String scriptFile, boolean scriptFileFirst) {
    this.uri = uri;
    this.autoconnect = autoconnect;
    this.debug = debug;
    this.verifySSL = verifySSL;
    this.scriptFile = scriptFile;
    this.scriptFileFirst = scriptFileFirst;
  }

  public String getUri() {
    return uri;
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

  public String getScriptFile() {
    return scriptFile;
  }

  public boolean getScriptFileFirst() {
    return scriptFileFirst;
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
    private String scriptFile;
    private boolean scriptFileFirst;

    public Builder setUri(String uri) {
      this.uri = uri;
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

    public Builder setScriptFile(String scriptFile) {
      this.scriptFile = scriptFile;
      return this;
    }

    public Builder setScriptFileFirst(boolean scriptFileFirst) {
      this.scriptFileFirst = scriptFileFirst;
      return this;
    }

    public LaunchOptions build() {
      return new LaunchOptions(uri, autoconnect, debug, verifySSL, scriptFile, scriptFileFirst);
    }
  }
}
