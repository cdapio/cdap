/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.messaging.context;

import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.RollbackDetail;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.TopicId;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of {@link MessagePublisher} that implements {@link TransactionAware} so that messages will be
 * published transactionally if there is an active transaction. If there is no active transaction, it will
 * delegate to {@link DirectMessagePublisher} for publishing.
 */
final class BasicMessagePublisher extends AbstractMessagePublisher implements TransactionAware {

  private final MessagingService messagingService;
  private final DirectMessagePublisher directMessagePublisher;
  private final Map<TopicId, StoreRequestBuilder> txPublishRequests;
  private final Map<TopicId, RollbackDetail> rollbackDetails;
  private final String name;
  private Transaction transaction;

  BasicMessagePublisher(MessagingService messagingService) {
    this.messagingService = messagingService;
    this.directMessagePublisher = new DirectMessagePublisher(messagingService);
    this.txPublishRequests = new HashMap<>();
    this.rollbackDetails = new HashMap<>();
    this.name = "MessagePublisher-" + Thread.currentThread().getName();
  }

  @Override
  public void publish(TopicId topicId, Iterator<byte[]> payloads) throws IOException, TopicNotFoundException {
    if (transaction == null) {
      directMessagePublisher.publish(topicId, payloads);
      return;
    }

    StoreRequestBuilder builder = txPublishRequests.get(topicId);
    if (builder == null) {
      builder = StoreRequestBuilder.of(topicId);
      builder.setTransaction(transaction.getWritePointer());
      txPublishRequests.put(topicId, builder);
    }
    builder.addPayloads(payloads);
  }

  @Override
  public void startTx(Transaction transaction) {
    this.transaction = transaction;
    txPublishRequests.clear();
    rollbackDetails.clear();
  }

  @Override
  public void updateTx(Transaction transaction) {
    // Currently CDAP doesn't support checkpoint.
    throw new UnsupportedOperationException("Transaction checkpoints are not supported");
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    // Messaging system is append only, hence never has write conflict
    return Collections.emptySet();
  }

  @Override
  public boolean commitTx() throws Exception {
    // One potential improvement in future we can make is to publish concurrently for different topics using a executor.
    for (Map.Entry<TopicId, StoreRequestBuilder> entry : txPublishRequests.entrySet()) {
      // If the payload of publish request is empty, no need to publish
      if (!entry.getValue().hasPayload()) {
        continue;
      }
      rollbackDetails.put(entry.getKey(), messagingService.publish(entry.getValue().build()));
    }
    return true;
  }

  @Override
  public void postTxCommit() {
    transaction = null;
    txPublishRequests.clear();
    rollbackDetails.clear();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    try {
      for (Map.Entry<TopicId, RollbackDetail> entry : rollbackDetails.entrySet()) {
        messagingService.rollback(entry.getKey(), entry.getValue());
      }
      return true;
    } finally {
      transaction = null;
      txPublishRequests.clear();
      rollbackDetails.clear();
    }
  }

  @Override
  public String getTransactionAwareName() {
    return name;
  }
}
