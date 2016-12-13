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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * Implementation of {@link MessageFetcher} that implements {@link TransactionAware}. The active transaction will
 * be used for fetching messages if there is one. Otherwise messages will be fetched without transaction.
 */
final class BasicMessageFetcher implements MessageFetcher, TransactionAware {

  private final MessagingService messagingService;
  private final String name;
  private Transaction transaction;

  BasicMessageFetcher(MessagingService messagingService) {
    this.messagingService = messagingService;
    this.name = "MessageFetcher-" + Thread.currentThread().getName();
  }

  @Override
  public CloseableIterator<Message> fetch(String namespace, String topic,
                                          int limit, long timestamp) throws IOException, TopicNotFoundException {
    co.cask.cdap.messaging.MessageFetcher fetcher = messagingService
      .prepareFetch(new NamespaceId(namespace).topic(topic))
      .setLimit(limit)
      .setStartTime(timestamp);

    if (transaction != null) {
      fetcher.setTransaction(transaction);
    }

    return new MessageIterator(fetcher.fetch());
  }

  @Override
  public CloseableIterator<Message> fetch(String namespace, String topic, int limit,
                                          @Nullable String afterMessageId) throws IOException, TopicNotFoundException {
    co.cask.cdap.messaging.MessageFetcher fetcher = messagingService
      .prepareFetch(new NamespaceId(namespace).topic(topic))
      .setLimit(limit)
      .setStartMessage(Bytes.fromHexString(afterMessageId), false);

    if (transaction != null) {
      fetcher.setTransaction(transaction);
    }

    return new MessageIterator(fetcher.fetch());
  }

  @Override
  public void startTx(Transaction transaction) {
    this.transaction = transaction;
  }

  @Override
  public void updateTx(Transaction transaction) {
    // Currently CDAP doesn't support checkpoint.
    throw new UnsupportedOperationException("Transaction checkpoints are not supported");
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return Collections.emptySet();
  }

  @Override
  public boolean commitTx() throws Exception {
    return true;
  }

  @Override
  public void postTxCommit() {
    transaction = null;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    transaction = null;
    return true;
  }

  @Override
  public String getTransactionAwareName() {
    return name;
  }
}
