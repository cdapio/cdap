/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.notifications.service;

import co.cask.cdap.api.TxRunnable;

/**
 * Context accessible when handling the reception of a Notification by the
 * {@link NotificationHandler#processNotification} method.
 */
public interface NotificationContext {

  /**
   * Execute a set of operations on datasets via a {@link TxRunnable} that are committed as a single transaction.
   *
   * @param runnable The runnable to be executed in the transaction.
   * @param policy {@link TxRetryPolicy} defining the behavior of this method in case the transaction fails.
   * @return True when executed successfully, false otherwise.
   */
  boolean execute(TxRunnable runnable, TxRetryPolicy policy);
}
