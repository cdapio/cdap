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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Service to access {@link ProgramHeartbeatDataset} in transaction
 */
public class ProgramHeartbeatService {
  private static final DatasetId PROGRAM_HEARTBEAT_INSTANCE_ID =
    NamespaceId.SYSTEM.dataset(Constants.ProgramHeartbeat.TABLE);

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final CConfiguration cConfiguration;

  @Inject
  public ProgramHeartbeatService(DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                 CConfiguration cConfiguration) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient, PROGRAM_HEARTBEAT_INSTANCE_ID.getParent(),
        Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.cConfiguration = cConfiguration;
  }

  /**
   * Performs the {@link ProgramHeartbeatDataset#scan(long, long, Set)}
   * @param startTimestampInSeconds starting timestamp inclusive
   * @param endTimestampInSeconds ending timestamp exclusive
   * @param namespaces set of namespaces to scan for the timerange
   * @return collection of run record meta
   */
  public Collection<RunRecordMeta> scan(long startTimestampInSeconds,
                                        long endTimestampInSeconds, Set<String> namespaces) {
    return Transactionals.execute(transactional, context -> {
      ProgramHeartbeatDataset programHeartbeatDataset =
        ProgramHeartbeatDataset.getOrCreate(context, datasetFramework, cConfiguration);
      return programHeartbeatDataset.scan(startTimestampInSeconds, endTimestampInSeconds, namespaces);
    });
  }
}
