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

package co.cask.cdap.reporting;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collection;
import java.util.Collections;

/**
 * Store to access {@link ProgramHeartbeatDataset}
 */
public class ProgramHeartbeatStore {
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  static final DatasetId PROGRAM_HEARTBEAT_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.ProgramHeartbeat.TABLE);

  @Inject
  public ProgramHeartbeatStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient, PROGRAM_HEARTBEAT_INSTANCE_ID.getParent(),
        Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  public Collection<Notification> scan(byte[] startRow, byte[] endRow) {
    return Transactionals.execute(transactional, context -> {
      ProgramHeartbeatDataset programHeartbeatDataset = ProgramHeartbeatDataset.getOrCreate(context, datasetFramework);
      return programHeartbeatDataset.scan(startRow, endRow);
    });
  }
}
