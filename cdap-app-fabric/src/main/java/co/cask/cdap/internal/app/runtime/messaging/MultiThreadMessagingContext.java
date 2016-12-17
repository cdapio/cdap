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

package co.cask.cdap.internal.app.runtime.messaging;

import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.transaction.MultiThreadTransactionAware;
import co.cask.cdap.messaging.MessagingService;
import org.apache.tephra.TransactionAware;

/**
 * The basic implementation of {@link MessagingContext} for supporting message publishing/fetching in both
 * non-transactional context and short transaction context (hence not for MR and Spark).
 *
 * Each program context should have an instance of this class, which is added as an extra {@link TransactionAware}
 * through the {@link DynamicDatasetCache#addExtraTransactionAware(TransactionAware)} method so that it can tap into
 * all transaction lifecycle events across all threads.
 */
public class MultiThreadMessagingContext extends MultiThreadTransactionAware<BasicMessagingContext>
                                         implements MessagingContext {

  private final MessagingService messagingService;

  public MultiThreadMessagingContext(final MessagingService messagingService) {
    this.messagingService = messagingService;
  }

  @Override
  public MessagePublisher getMessagePublisher() {
    return getCurrentThreadTransactionAware().getPublisher();
  }

  @Override
  public MessagePublisher getDirectMessagePublisher() {
    return new DirectMessagePublisher(messagingService);
  }

  @Override
  public MessageFetcher getMessageFetcher() {
    return getCurrentThreadTransactionAware().getFetcher();
  }

  @Override
  protected BasicMessagingContext createTransactionAwareForCurrentThread() {
    return new BasicMessagingContext(messagingService);
  }
}
