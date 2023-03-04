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

import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Represents application state keys.
 */
public class AppStateKey {

  protected final NamespaceId namespaceId;
  protected final String appName;
  protected final String stateKey;

  public AppStateKey(NamespaceId namespaceId,
      String appName,
      String stateKey) {
    this.namespaceId = namespaceId;
    this.appName = appName;
    this.stateKey = stateKey;
  }

  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  public String getAppName() {
    return appName;
  }

  public String getStateKey() {
    return stateKey;
  }
}
