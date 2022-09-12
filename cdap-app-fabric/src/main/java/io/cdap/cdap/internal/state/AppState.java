/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.state;

/**
 * Represents an application state.
 */
public class AppState {
  private String namespace;
  private String appName;
  private final long appId;
  private final String stateKey;
  private final byte[] stateValue;

  public AppState(String namespace,
                  String appName,
                  long appId,
                  String stateKey,
                  byte[] stateValue) {
    this.namespace = namespace;
    this.appName = appName;
    this.appId = appId;
    this.stateKey = stateKey;
    this.stateValue = stateValue;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getAppName() {
    return appName;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public long getAppId() {
    return appId;
  }

  public String getStateKey() {
    return stateKey;
  }

  public byte[] getState() {
    return stateValue;
  }
}
