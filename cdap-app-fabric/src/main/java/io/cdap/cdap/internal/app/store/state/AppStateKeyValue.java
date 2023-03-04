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
 * Represents an application state.
 */
public class AppStateKeyValue extends AppStateKey {

  private final byte[] stateValue;

  public AppStateKeyValue(NamespaceId namespaceId,
      String appName,
      String stateKey,
      byte[] stateValue) {
    super(namespaceId, appName, stateKey);
    this.stateValue = stateValue;
  }

  public byte[] getState() {
    return stateValue;
  }
}
