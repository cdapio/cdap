/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.runtime.scheduler.SchedulerQueueResolver;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.security.ImpersonationInfo;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.Id;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.util.Map;

/**
 * Used to provide default user and system properties that can be used while starting a Program.
 */
public class PropertiesResolver {

  private final PreferencesStore prefStore;
  private final CConfiguration cConf;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final SchedulerQueueResolver queueResolver;

  @Inject
  PropertiesResolver(PreferencesStore prefStore, CConfiguration cConf,
                     NamespaceQueryAdmin namespaceQueryAdmin,
                     SchedulerQueueResolver schedulerQueueResolver) {
    this.prefStore = prefStore;
    this.cConf = cConf;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.queueResolver = schedulerQueueResolver;
  }

  public Map<String, String> getUserProperties(Id.Program id) {
    Map<String, String> userArgs = prefStore.getResolvedProperties(id.getNamespaceId(), id.getApplicationId(),
                                                                   id.getType().getCategoryName(), id.getId());
    userArgs.put(ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(System.currentTimeMillis()));
    return userArgs;
  }

  public Map<String, String> getSystemProperties(Id.Program id) throws Exception {
    Map<String, String> systemArgs = Maps.newHashMap();
    systemArgs.put(Constants.CLUSTER_NAME, cConf.get(Constants.CLUSTER_NAME, ""));
    systemArgs.put(Constants.AppFabric.APP_SCHEDULER_QUEUE, queueResolver.getQueue(id.getNamespace()));
    if (SecurityUtil.isKerberosEnabled(cConf)) {
      ImpersonationInfo impersonationInfo = new ImpersonationInfo(
        namespaceQueryAdmin.get(id.toEntityId().getNamespaceId()), cConf);
      systemArgs.put(ProgramOptionConstants.PRINCIPAL, impersonationInfo.getPrincipal());
      systemArgs.put(ProgramOptionConstants.KEYTAB_URI, impersonationInfo.getKeytabURI());
    }
    return systemArgs;
  }
}
