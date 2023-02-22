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
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.app.runtime.scheduler.SchedulerQueueResolver;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.security.impersonation.ImpersonationInfo;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

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
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  PropertiesResolver(PreferencesFetcher preferencesFetcher, CConfiguration cConf,
                     OwnerAdmin ownerAdmin,
                     SchedulerQueueResolver schedulerQueueResolver,
                     NamespaceQueryAdmin namespaceQueryAdmin) {
    this.preferencesFetcher = preferencesFetcher;
    this.cConf = cConf;
    this.ownerAdmin = ownerAdmin;
    this.queueResolver = schedulerQueueResolver;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  public Map<String, String> getUserProperties(ProgramReference programRef)
    throws IOException, NotFoundException, UnauthorizedException {
    PreferencesDetail preferencesDetail = preferencesFetcher.get(programRef, true);
    Map<String, String> userArgs = new HashMap<>(preferencesDetail.getProperties());
    userArgs.put(ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(System.currentTimeMillis()));
    return userArgs;
  }

  public Map<String, String> getSystemProperties(ProgramReference programRef)
    throws IOException, NamespaceNotFoundException, AccessException {
    Map<String, String> systemArgs = new HashMap<>();
    systemArgs.put(Constants.CLUSTER_NAME, cConf.get(Constants.CLUSTER_NAME, ""));
    addNamespaceConfigs(systemArgs, programRef.getNamespaceId());
    if (SecurityUtil.isKerberosEnabled(cConf)) {
      ImpersonationInfo impersonationInfo = SecurityUtil.createImpersonationInfo(ownerAdmin, cConf, programRef);
      systemArgs.put(ProgramOptionConstants.PRINCIPAL, impersonationInfo.getPrincipal());
      systemArgs.put(ProgramOptionConstants.APP_PRINCIPAL_EXISTS,
                     String.valueOf(ownerAdmin.exists(programRef.getParent())));
    }
    return systemArgs;
  }

  private void addNamespaceConfigs(Map<String, String> args,
                                   NamespaceId namespaceId) throws NamespaceNotFoundException, IOException {
    if (NamespaceId.isReserved(namespaceId.getNamespace())) {
      return;
    }
    try {
      NamespaceMeta namespaceMeta = namespaceQueryAdmin.get(namespaceId);
      SystemArguments.addNamespaceConfigs(args, namespaceMeta.getConfig());
      args.put(Constants.AppFabric.APP_SCHEDULER_QUEUE, queueResolver.getQueue(namespaceMeta));
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
