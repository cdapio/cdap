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

package co.cask.cdap.logging.save;

import co.cask.tephra.TransactionExecutorFactory;
import com.google.inject.Inject;

/**
 * Creates {@link CheckpointManager}s.
 */
public class CheckpointManagerFactory {
  private final LogSaverTableUtil tableUtil;
  private final TransactionExecutorFactory txExecutorFactory;

  @Inject
  public CheckpointManagerFactory(LogSaverTableUtil tableUtil, TransactionExecutorFactory txExecutorFactory) {
    this.tableUtil = tableUtil;
    this.txExecutorFactory = txExecutorFactory;
  }

  public CheckpointManager create(String topic, int prefix) {
    return new CheckpointManager(tableUtil, txExecutorFactory, topic, prefix);
  }
}
