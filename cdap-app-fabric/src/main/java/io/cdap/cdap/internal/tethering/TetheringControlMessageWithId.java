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

package io.cdap.cdap.internal.tethering;

/**
 * Control message and its message id. These messages are sent from the tethering server to the
 * client.
 */
public class TetheringControlMessageWithId {

  private TetheringControlMessage controlMessage;
  private String messageId;

  public TetheringControlMessageWithId(TetheringControlMessage controlMessage, String messageId) {
    this.controlMessage = controlMessage;
    this.messageId = messageId;
  }

  public TetheringControlMessage getControlMessage() {
    return controlMessage;
  }

  public String getMessageId() {
    return messageId;
  }
}
