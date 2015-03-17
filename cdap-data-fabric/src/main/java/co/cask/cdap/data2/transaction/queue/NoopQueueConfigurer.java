/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue;

import co.cask.tephra.Transaction;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * A {@link QueueConfigurer} that does nothing.
 */
public final class NoopQueueConfigurer implements QueueConfigurer {

  @Override
  public void configureInstances(long groupId, int instances) throws Exception {
    // No-op
  }

  @Override
  public void configureGroups(Map<Long, Integer> groupInfo) throws Exception {
    // No-op
  }

  @Override
  public void startTx(Transaction tx) {
    // No-op
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return ImmutableList.of();
  }

  @Override
  public boolean commitTx() throws Exception {
    return true;
  }

  @Override
  public void postTxCommit() {
    // No-op
  }

  @Override
  public boolean rollbackTx() throws Exception {
    return true;
  }

  @Override
  public String getTransactionAwareName() {
    return getClass().getSimpleName();
  }

  @Override
  public void close() throws IOException {
    // No-op
  }
}
