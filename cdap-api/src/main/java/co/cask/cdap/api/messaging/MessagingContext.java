/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.annotation.TransactionControl;

/**
 * Provides access to the transactional messaging system.
 */
@Beta
public interface MessagingContext {

  /**
   * Gets an instance of {@link MessagePublisher} for publishing messages.
   * <p>
   * Messages will be published
   * transactionally if any of the {@code publish} methods in the {@link MessagePublisher} is called from a
   * transactional context (either through {@link TransactionControl#IMPLICIT Implicit Transaction Control},
   * or via {@link Transactional#execute(TxRunnable)}
   * when {@link TransactionControl#EXPLICIT Explicit Transaction Control} is used).
   * </p>
   * <p>
   * When those {@code publish} methods are called without a transactional context, message will be published
   * without transaction.
   * </p>
   *
   * @return a new instance of {@link MessagePublisher}. The returned instance cannot be shared across multiple threads.
   */
  MessagePublisher getMessagePublisher();

  /**
   * Gets an instance of {@link MessagePublisher} for publishing messages. Messages published through the
   * resulting {@link MessagePublisher} is always published without using transaction and is immediately
   * available for consumption.
   *
   * @return a new instance of {@link MessagePublisher}. The returned instance is safe to be used from multiple threads.
   */
  MessagePublisher getDirectMessagePublisher();

  /**
   * Gets an instance of {@link MessageFetcher} for fetching messages.
   * <p>
   * Messages will be fetched
   * transactionally if any of the {@code fetch} methods in the {@link MessageFetcher} is called from a
   * transactional context (either through {@link TransactionControl#IMPLICIT Implicit Transaction Control},
   * or via {@link Transactional#execute(TxRunnable)}
   * when {@link TransactionControl#EXPLICIT Explicit Transaction Control} is used).
   * </p>
   * <p>
   * When those {@code fetch} methods are called without a transactional context, message will be fetched
   * without transaction.
   * </p>
   * @return a new instance of {@link MessageFetcher}. The returned instance cannot be shared across multiple threads.
   */
  MessageFetcher getMessageFetcher();
}
