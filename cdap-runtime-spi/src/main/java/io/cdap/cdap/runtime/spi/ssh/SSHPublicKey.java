/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.ssh;

import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;

/**
 * Contains information of a public key.
 *
 * @deprecated SSH-based runtime execution and monitoring are no longer supported; use
 *     {@link RuntimeJobManager} and {@link RuntimeMonitorType#URL} instead.
 */
@Deprecated
public class SSHPublicKey {

  private final String user;
  private final String key;

  public SSHPublicKey(String user, String key) {
    this.user = user;
    this.key = key;
  }

  /**
   * Returns the user name associated with the key.
   */
  public String getUser() {
    return user;
  }

  /**
   * Returns the public key in SSH public key format described in RFC-4253.
   */
  public String getKey() {
    return key;
  }
}
