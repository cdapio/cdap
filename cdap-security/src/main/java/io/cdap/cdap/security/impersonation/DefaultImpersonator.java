/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.security.impersonation;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.security.AuthEnforceUtil;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Default implementation of {@link Impersonator} that impersonate using {@link UGIProvider}.
 */
public class DefaultImpersonator implements Impersonator {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultImpersonator.class);
  // Log once every 100 calls
  private static final Logger IMPERSONATION_FAILTURE_LOG = Loggers.sampling(LOG, LogSamplers.onceEvery(100));
  private final UGIProvider ugiProvider;
  private final boolean kerberosEnabled;
  private final String masterShortUsername;

  @Inject
  @VisibleForTesting
  public DefaultImpersonator(CConfiguration cConf, UGIProvider ugiProvider) {
    this.ugiProvider = ugiProvider;
    this.kerberosEnabled = SecurityUtil.isKerberosEnabled(cConf);
    try {
      // on kerberos disabled cluster the master principal will be null
      this.masterShortUsername = kerberosEnabled
        ? new KerberosName(SecurityUtil.getMasterPrincipal(cConf)).getShortName()
        : null;
    } catch (IOException e) {
      throw new RuntimeException("Failed to get short name from the kerberos principal", e);
    }
  }

  @Override
  public <T> T doAs(NamespacedEntityId entityId, final Callable<T> callable) throws Exception {
    return doAs(entityId, callable, ImpersonatedOpType.OTHER);
  }

  @Override
  public <T> T doAs(NamespacedEntityId entityId, Callable<T> callable,
                    ImpersonatedOpType impersonatedOpType) throws Exception {
    UserGroupInformation ugi = getUGI(entityId, impersonatedOpType);
    if (!UserGroupInformation.getCurrentUser().equals(ugi)) {
      LOG.debug("Performing doAs with UGI {} for entity {} and impersonation operation type {}", ugi, entityId,
                impersonatedOpType);
    }
    return ImpersonationUtils.doAs(ugi, callable);
  }

  @Override
  public UserGroupInformation getUGI(NamespacedEntityId entityId) throws AccessException {
    return getUGI(entityId, ImpersonatedOpType.OTHER);
  }

  private UserGroupInformation getUGI(NamespacedEntityId entityId,
                                      ImpersonatedOpType impersonatedOpType) throws AccessException {
    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      // don't impersonate if kerberos isn't enabled OR if the operation is in the system namespace
      if (!kerberosEnabled || NamespaceId.SYSTEM.equals(entityId.getNamespaceId())) {
        return currentUser;
      }

      ImpersonationRequest impersonationRequest = new ImpersonationRequest(entityId, impersonatedOpType);
      // if the current user is not same as cdap master user then it means we are already impersonating some user
      // and hence we should not allow another impersonation. See CDAP-8641 and CDAP-13123
      // Note that this is just a temporary fix and we will need to revisit the impersonation model in the future.
      if (!currentUser.getShortUserName().equals(masterShortUsername)) {
        LOG.debug("Not impersonating for {} as the call is already impersonated as {}",
                  impersonationRequest, currentUser);
        IMPERSONATION_FAILTURE_LOG.warn("Not impersonating for {} as the call is already impersonated as {}",
                                        impersonationRequest, currentUser);
        return currentUser;
      }
      return ugiProvider.getConfiguredUGI(impersonationRequest).getUGI();
    } catch (IOException e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }
}
