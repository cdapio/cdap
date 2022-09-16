/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store.state;

import javax.annotation.Nullable;

/**
 * Represents an application state.
 */
public class AppState {
  private final String namespace;
  private final String appName;
  private final String stateKey;
  private final byte[] stateValue;

  public AppState(String namespace,
                  String appName) {
    this(namespace, appName, null, null);
  }

  public AppState(String namespace,
                  String appName,
                  String stateKey) {
    this(namespace, appName, stateKey, null);
  }

  public AppState(String namespace,
                  String appName,
                  @Nullable String stateKey,
                  @Nullable byte[] stateValue) {
    this.namespace = namespace;
    this.appName = appName;
    this.stateKey = stateKey;
    this.stateValue = stateValue;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getAppName() {
    return appName;
  }

  @Nullable
  public String getStateKey() {
    return stateKey;
  }

  @Nullable
  public byte[] getState() {
    return stateValue;
  }
}
