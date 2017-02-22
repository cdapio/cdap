/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.visibility;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;

import java.util.Collection;
import java.util.Collections;

/**
 * Implementation of {@link VisibilityFence} used by reader.
 */
class ReadFence implements TransactionAware {
  private final byte[] fenceId;
  private Transaction tx;

  public ReadFence(byte[] fenceId) {
    this.fenceId = fenceId;
  }

  @Override
  public void startTx(Transaction tx) {
    this.tx = tx;
  }

  @Override
  public void updateTx(Transaction tx) {
    // Fences only need original transaction
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    if (tx == null) {
      throw new IllegalStateException("Transaction has not started yet");
    }
    return Collections.singleton(Bytes.concat(fenceId, Longs.toByteArray(tx.getTransactionId())));
  }

  @Override
  public boolean commitTx() throws Exception {
    // Nothing to persist
    return true;
  }

  @Override
  public void postTxCommit() {
    tx = null;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    // Nothing to rollback
    return true;
  }

  @Override
  public String getTransactionAwareName() {
    return getClass().getSimpleName();
  }
}
