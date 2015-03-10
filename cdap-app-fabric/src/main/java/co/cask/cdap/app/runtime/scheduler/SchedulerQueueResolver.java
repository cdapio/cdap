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

package co.cask.cdap.app.runtime.scheduler;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;


/**
 * Helper class to resolve scheduler queue name.
 */
public class SchedulerQueueResolver {
  private final String schedulerQueueFromCConf;
  private final Store store;

  public SchedulerQueueResolver(CConfiguration cConf, Store store) {
    this.schedulerQueueFromCConf = cConf.get(Constants.AppFabric.APP_SCHEDULER_QUEUE);
    this.store = store;
  }

  public String getSchedulerQueueFromCConf() {
    return schedulerQueueFromCConf;
  }

  public String getNamespaceResolvedSchedulerQueue(Id.Namespace namespaceId) {
    NamespaceMeta meta = store.getNamespace(namespaceId);
    Preconditions.checkNotNull(meta, "Namespace meta cannot be null");

    NamespaceConfig config = meta.getConfig();
    return config.getSchedulerQueueName() != null ? config.getSchedulerQueueName() : schedulerQueueFromCConf;
  }
}
