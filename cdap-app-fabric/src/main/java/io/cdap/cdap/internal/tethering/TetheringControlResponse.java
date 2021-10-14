/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import javax.annotation.Nullable;

/**
 * Heartbeat response sent to tether agent.
 */
public class TetheringControlResponse {
  // id of the last control message sent by the server.
  @Nullable
  private final String lastMessageId;
  // control message sent by server.
  private final TetheringControlMessage controlMessage;

  public TetheringControlResponse(@Nullable String lastMessageId, TetheringControlMessage controlMessage) {
    this.lastMessageId = lastMessageId;
    this.controlMessage = controlMessage;
  }

  public String getLastMessageId() {
    return lastMessageId;
  }

  public TetheringControlMessage getControlMessage() {
    return controlMessage;
  }
}
