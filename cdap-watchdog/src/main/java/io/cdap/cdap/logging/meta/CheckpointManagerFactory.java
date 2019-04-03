/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.meta;

import co.cask.cdap.spi.data.transaction.TransactionRunner;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

/**
 * Creates {@link CheckpointManager}s.
 */
public class CheckpointManagerFactory {
  private final TransactionRunner transactionRunner;

  @VisibleForTesting
  @Inject
  public CheckpointManagerFactory(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  public CheckpointManager create(String prefix, Type type) {
    switch (type) {
      case KAFKA:
        return new KafkaCheckpointManager(transactionRunner, prefix);
      case LOG_BUFFER:
        return new LogBufferCheckpointManager(transactionRunner, prefix);
    }
    throw new IllegalStateException("Checkpoint manager type " + type + " is not supported.");
  }

  /**
   * Type of checkpoint manager.
   */
  public enum Type {
    KAFKA,
    LOG_BUFFER
  }
}
