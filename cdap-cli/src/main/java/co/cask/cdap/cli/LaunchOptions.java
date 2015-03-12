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

/**
 * Contains the options that the user can pass to the CLI upon launch.
 */
public class LaunchOptions {

  public static final LaunchOptions DEFAULT = builder().build();

  private final String uri;
  private final boolean autoconnect;
  private final boolean debug;
  private final boolean verifySSL;

  public LaunchOptions(String uri, boolean autoconnect, boolean debug, boolean verifySSL) {
    this.uri = uri;
    this.autoconnect = autoconnect;
    this.debug = debug;
    this.verifySSL = verifySSL;
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

    public LaunchOptions build() {
      return new LaunchOptions(uri, autoconnect, debug, verifySSL);
    }
  }
}
