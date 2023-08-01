/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.security;

import javax.annotation.Nullable;

/**
 * Defines the task context used by GcpMetadataHttpHandlerInternal.
 */
public final class GcpMetadataTaskContext {
  private final String namespace;

  private final String userId;

  private final String userIP;

  private final Credential userCredential;

  /**
   * Constructs a {@link GcpMetadataTaskContext}.
   *
   * @param namespace the namespace id string.
   * @param userId the user Id.
   * @param userIP the user Ip.
   * @param userCredential the user credential.
   */
  public GcpMetadataTaskContext(String namespace, String userId,
      String userIP, @Nullable Credential userCredential) {
    this.namespace = namespace;
    this.userId = userId;
    this.userIP = userIP;
    this.userCredential = userCredential;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getUserId() {
    return userId;
  }

  public String getUserIp() {
    return userIP;
  }

  public Credential getUserCredential() {
    return userCredential;
  }
}
