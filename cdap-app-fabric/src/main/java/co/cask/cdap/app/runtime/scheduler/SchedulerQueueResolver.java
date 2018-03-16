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

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Strings;
import com.google.inject.Inject;

import java.io.IOException;
import javax.annotation.Nullable;


/**
 * Helper class to resolve scheduler queue name.
 */
public class SchedulerQueueResolver {
  private final String defaultQueue;
  private final String systemQueue;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  /**
   * Construct SchedulerQueueResolver with CConfiguration and Store.
   */
  @Inject
  public SchedulerQueueResolver(CConfiguration cConf, NamespaceQueryAdmin namespaceQueryAdmin) {
    this.defaultQueue = cConf.get(Constants.AppFabric.APP_SCHEDULER_QUEUE, "");
    this.systemQueue = cConf.get(Constants.Service.SCHEDULER_QUEUE, "");
    this.namespaceQueryAdmin = namespaceQueryAdmin;
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
   * @param namespaceId NamespaceId
   * @return schedule queue at namespace level or default queue.
   */
  @Nullable
  public String getQueue(Id.Namespace namespaceId) throws IOException, NamespaceNotFoundException {
    if (namespaceId.equals(Id.Namespace.SYSTEM)) {
      return systemQueue;
    }
    NamespaceMeta meta;
    try {
      meta = namespaceQueryAdmin.get(namespaceId.toEntityId());
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
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
