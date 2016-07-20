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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.runtime.scheduler.SchedulerQueueResolver;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.security.ImpersonationInfo;
import co.cask.cdap.data2.security.ImpersonationUserResolver;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.Id;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Map;

/**
 * Used to provide default user and system properties that can be used while starting a Program.
 */
public class PropertiesResolver {

  private final PreferencesStore prefStore;
  private final SchedulerQueueResolver queueResolver;
  private final ImpersonationUserResolver impersonationUserResolver;
  private final boolean kerberosEnabled;

  @Inject
  PropertiesResolver(PreferencesStore prefStore, CConfiguration cConf,
                     SchedulerQueueResolver schedulerQueueResolver,
                     ImpersonationUserResolver impersonationUserResolver) {
    this.prefStore = prefStore;
    this.queueResolver = schedulerQueueResolver;
    this.impersonationUserResolver = impersonationUserResolver;
    this.kerberosEnabled = SecurityUtil.isKerberosEnabled(cConf);
  }

  public Map<String, String> getUserProperties(Id.Program id) {
    Map<String, String> userArgs = prefStore.getResolvedProperties(id.getNamespaceId(), id.getApplicationId(),
                                                                   id.getType().getCategoryName(), id.getId());
    userArgs.put(ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(System.currentTimeMillis()));
    return userArgs;
  }

  public Map<String, String> getSystemProperties(Id.Program id) throws IOException, NamespaceNotFoundException {
    Map<String, String> systemArgs = Maps.newHashMap();
    systemArgs.put(Constants.AppFabric.APP_SCHEDULER_QUEUE, queueResolver.getQueue(id.getNamespace()));
    if (kerberosEnabled) {
      ImpersonationInfo impersonationInfo =
        impersonationUserResolver.getImpersonationInfo(id.getNamespace().toEntityId());
      systemArgs.put(ProgramOptionConstants.PRINCIPAL, impersonationInfo.getPrincipal());
      systemArgs.put(ProgramOptionConstants.KEYTAB_URI, impersonationInfo.getKeytabURI());
    }
    return systemArgs;
  }
}
