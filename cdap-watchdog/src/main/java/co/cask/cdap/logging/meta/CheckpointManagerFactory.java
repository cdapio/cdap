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

import co.cask.cdap.data2.dataset2.DatasetFramework;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;

/**
 * Creates {@link DefaultCheckpointManager}s.
 */
public class CheckpointManagerFactory {

  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient txClient;

  @Inject
  CheckpointManagerFactory(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.txClient = txClient;
  }

  public CheckpointManager create(String topic, int prefix) {
    return new DefaultCheckpointManager(datasetFramework, txClient, topic, prefix);
  }
}
