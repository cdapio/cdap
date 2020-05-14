/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import com.google.inject.Inject;
import io.cdap.cdap.app.runtime.scheduler.SchedulerQueueResolver;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.impersonation.ImpersonationInfo;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.SecurityUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to provide default user and system properties that can be used while starting a Program.
 */
public class PropertiesResolver {
  private final PreferencesFetcher preferencesFetcher;
  private final CConfiguration cConf;
  private final OwnerAdmin ownerAdmin;
  private final SchedulerQueueResolver queueResolver;

  @Inject
  PropertiesResolver(PreferencesFetcher preferencesFetcher, CConfiguration cConf,
                     OwnerAdmin ownerAdmin,
                     SchedulerQueueResolver schedulerQueueResolver) {
    this.preferencesFetcher = preferencesFetcher;
    this.cConf = cConf;
    this.ownerAdmin = ownerAdmin;
    this.queueResolver = schedulerQueueResolver;
  }

  public Map<String, String> getUserProperties(ProgramId id) throws IOException, NotFoundException {
    PreferencesDetail preferencesDetail = preferencesFetcher.get(id, true);
    Map<String, String> userArgs = new HashMap<>(preferencesDetail.getProperties());
    userArgs.put(ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(System.currentTimeMillis()));
    return userArgs;
  }

  public Map<String, String> getSystemProperties(ProgramId id) throws IOException, NamespaceNotFoundException {
    Map<String, String> systemArgs = new HashMap<>();
    systemArgs.put(Constants.CLUSTER_NAME, cConf.get(Constants.CLUSTER_NAME, ""));
    systemArgs.put(Constants.AppFabric.APP_SCHEDULER_QUEUE, queueResolver.getQueue(id.getNamespaceId()));
    if (SecurityUtil.isKerberosEnabled(cConf)) {
      ImpersonationInfo impersonationInfo = SecurityUtil.createImpersonationInfo(ownerAdmin, cConf, id);
      systemArgs.put(ProgramOptionConstants.PRINCIPAL, impersonationInfo.getPrincipal());
      systemArgs.put(ProgramOptionConstants.APP_PRINCIPAL_EXISTS,
                     String.valueOf(ownerAdmin.exists(id.getParent())));
    }
    return systemArgs;
  }
}
