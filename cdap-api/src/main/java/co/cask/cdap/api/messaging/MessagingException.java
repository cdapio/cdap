/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.api.messaging;

import co.cask.cdap.api.annotation.Beta;

/**
 * Signals that an exception related to the messaging system has occurred.
 * This class is the parent class for exceptions produced by the Transactional Messaging System.
 */
@Beta
public class MessagingException extends Exception {

  private final String namespace;
  private final String topic;

  public MessagingException(String namespace, String topic, String message) {
    this(namespace, topic, message, null);
  }

  public MessagingException(String namespace, String topic, String message, Throwable cause) {
    super(message, cause);
    this.namespace = namespace;
    this.topic = topic;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getTopic() {
    return topic;
  }
}
