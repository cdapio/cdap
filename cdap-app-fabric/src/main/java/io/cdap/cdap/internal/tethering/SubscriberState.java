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

package io.cdap.cdap.internal.tethering;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracks the last message id received from and sent to each tethered peer.
 */
public class SubscriberState {

  // Id of the last program status update message that was sent to each tethered peer.
  private final Map<String, String> lastMessageIdsSent;
  // Id of the last control message received from each tethered peer.
  private final Map<String, String> lastMessageIdsReceived;

  public SubscriberState() {
    lastMessageIdsSent = new HashMap<>();
    lastMessageIdsReceived = new HashMap<>();
  }

  /**
   * Sets the last message id sent to a tethered peer.
   *
   * @param peer peer name
   * @param messageId message id
   */
  void setLastMessageIdSent(String peer, String messageId) {
    lastMessageIdsSent.put(peer, messageId);
  }

  /**
   * Sets the last message id received from a tethered peer.
   *
   * @param peer peer name
   * @param messageId message id
   */
  void setLastMessageIdReceived(String peer, String messageId) {
    lastMessageIdsReceived.put(peer, messageId);
  }

  /**
   * Returns the last message id sent to a tethered peer.
   *
   * @param peer peer name
   */
  public String getLastMessageIdSent(String peer) {
    return lastMessageIdsSent.get(peer);
  }

  /**
   * Returns the last message id received from a tethered peer.
   *
   * @param peer peer name
   */
  public String getLastMessageIdReceived(String peer) {
    return lastMessageIdsReceived.get(peer);
  }

  /**
   * Returns last message ids sent to each tethered peer.
   */
  public Map<String, String> getLastMessageIdsSent() {
    return lastMessageIdsSent;
  }

  /**
   * Returns last message ids received from each tethered peer.
   */
  public Map<String, String> getLastMessageIdsReceived() {
    return lastMessageIdsReceived;
  }
}
