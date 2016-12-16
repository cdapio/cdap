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
import co.cask.cdap.messaging.MessagingService;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link TransactionAware} that maintains {@link MessagePublisher},
 * {@link MessageFetcher} and {@link Transaction} information for a thread.
 */
final class BasicMessagingContext implements TransactionAware {

  private final MessagingService messagingService;
  private final String name;
  private Transaction transaction;
  private BasicMessagePublisher publisher;
  private BasicMessageFetcher fetcher;

  BasicMessagingContext(MessagingService messagingService) {
    this.messagingService = messagingService;
    this.name = "MessagingContext-" + Thread.currentThread().getName();
  }

  /**
   * Returns a {@link MessagePublisher}.
   */
  MessagePublisher getPublisher() {
    if (publisher == null) {
      publisher = new BasicMessagePublisher(messagingService);

      // If there is an active transaction, notify the publisher as well
      if (transaction != null) {
        publisher.startTx(transaction);
      }
    }
    return publisher;
  }

  /**
   * Returns a {@link MessageFetcher}.
   */
  MessageFetcher getFetcher() {
    if (fetcher == null) {
      fetcher = new BasicMessageFetcher(messagingService);

      // If there is an active transaction, notify the publisher as well
      if (transaction != null) {
        fetcher.startTx(transaction);
      }
    }
    return fetcher;
  }

  @Override
  public void startTx(Transaction transaction) {
    this.transaction = transaction;
    if (publisher != null) {
      publisher.startTx(transaction);
    }
    if (fetcher != null) {
      fetcher.startTx(transaction);
    }
  }

  @Override
  public void updateTx(Transaction transaction) {
    // Currently CDAP doesn't support checkpoint.
    throw new UnsupportedOperationException("Transaction checkpoints are not supported");
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    // Messaging system is append only, hence never has write conflict.
    // We controlled both DefaultMessageFetcher and DefaultMessagePublisher, hence not bother to call them.
    return Collections.emptySet();
  }

  @Override
  public boolean commitTx() throws Exception {
    // No need to call fetch.commitTx() as it always just return true
    return publisher == null || publisher.commitTx();
  }

  @Override
  public void postTxCommit() {
    transaction = null;

    if (publisher != null) {
      publisher.postTxCommit();
    }
    if (fetcher != null) {
      fetcher.postTxCommit();
    }
  }

  @Override
  public boolean rollbackTx() throws Exception {
    transaction = null;

    // Call the fetcher.rollbackTx() first, as it will never throw and always return true.
    // We need to call it for resetting internal states.
    if (fetcher != null) {
      fetcher.rollbackTx();
    }
    return publisher == null || publisher.rollbackTx();
  }

  @Override
  public String getTransactionAwareName() {
    return name;
  }
}
