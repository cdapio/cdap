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

package io.cdap.cdap.app.runtime.scheduler;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.NamespaceConfig;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import javax.annotation.Nullable;


/**
 * Helper class to resolve scheduler queue name.
 */
public class SchedulerQueueResolver {

  private final String defaultQueue;
  private final String systemQueue;

  /**
   * Construct SchedulerQueueResolver with CConfiguration and Store.
   */
  @Inject
  public SchedulerQueueResolver(CConfiguration cConf) {
    this.defaultQueue = cConf.get(Constants.AppFabric.APP_SCHEDULER_QUEUE, "");
    this.systemQueue = cConf.get(Constants.Service.SCHEDULER_QUEUE, "");
  }

  /**
   * @return default Queue that comes from CConfiguration.
   */
  public String getDefaultQueue() {
    return defaultQueue;
  }

  /**
   * Get queue at namespace level if it is empty returns the default queue.
   *
   * @param meta NamespaceMeta
   * @return schedule queue at namespace level or default queue.
   */
  @Nullable
  public String getQueue(@Nullable NamespaceMeta meta) {
    if (meta != null && meta.getNamespaceId().equals(NamespaceId.SYSTEM)) {
      return systemQueue;
    }
    if (meta != null) {
      NamespaceConfig config = meta.getConfig();
      String namespaceQueue = config.getSchedulerQueueName();
      return Strings.isNullOrEmpty(namespaceQueue) ? getDefaultQueue() : namespaceQueue;
    } else {
      return getDefaultQueue();
    }
  }
}
