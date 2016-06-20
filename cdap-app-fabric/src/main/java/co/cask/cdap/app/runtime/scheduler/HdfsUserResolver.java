/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.base.Strings;

import javax.annotation.Nullable;


/**
 * Helper class to resolve HDFS User.
 */
public class HdfsUserResolver {
  private final String defaultHdfsUser;
  private final NamespaceStore store;

  /**
   * Construct SchedulerQueueResolver with CConfiguration and Store.
   */
  public HdfsUserResolver(CConfiguration cConf, NamespaceStore store) {
    this.defaultHdfsUser = cConf.get(Constants.CFG_HDFS_USER);
    this.store = store;
  }

  /**
   * @return default HDFS User that comes from CConfiguration.
   */
  public String getDefaultHdfsUser() {
    return defaultHdfsUser;
  }

  /**
   * Get HDFS user at namespace level if it is empty returns the default HDFS user.
   *
   * @param namespaceId NamespaceId
   * @return HDFS user at namespace level or default HDFS user.
   */
  public String getHdfsUser(Id.Namespace namespaceId) {
    String namespaceUserSetting = getHdfsUserFromNamespace(namespaceId);

    if (!Strings.isNullOrEmpty(namespaceUserSetting)) {
      return namespaceUserSetting;
    }

    // the SecurityRequestContext in here won't work for SchedulerTaskRunner#run()
    String requestUserId = SecurityRequestContext.getUserId();
    if (!Strings.isNullOrEmpty(requestUserId)) {
      return requestUserId;
    }

    return getDefaultHdfsUser();
  }

  @Nullable
  public String getHdfsUserFromNamespace(Id.Namespace namespaceId) {
    NamespaceMeta meta = store.get(namespaceId);
    if (meta == null) {
      return null;
    }
    NamespaceConfig config = meta.getConfig();
    return config.getHdfsUser();
  }
}
