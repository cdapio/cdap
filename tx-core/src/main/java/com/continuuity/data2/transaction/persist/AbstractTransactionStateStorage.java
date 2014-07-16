/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction.persist;

import com.continuuity.data2.transaction.snapshot.SnapshotCodecProvider;
import com.google.common.util.concurrent.AbstractIdleService;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Common base class for all transaction storage implementations. This implement logic to prefix a snapshot
 * with a version when encoding, and to select the correct codec for decoding based on this version prefix.
 */
public abstract class AbstractTransactionStateStorage extends AbstractIdleService implements TransactionStateStorage {

  protected final SnapshotCodecProvider codecProvider;

  protected AbstractTransactionStateStorage(SnapshotCodecProvider codecProvider) {
    this.codecProvider = codecProvider;
  }

  @Override
  public void writeSnapshot(OutputStream out, TransactionSnapshot snapshot) throws IOException {
    codecProvider.encode(out, snapshot);
  }
}
