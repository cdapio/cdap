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

package co.cask.cdap.security.impersonation;

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.kerberos.ImpersonatedOpType;
import co.cask.cdap.common.kerberos.ImpersonationRequest;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Default implementation of {@link Impersonator} that impersonate using {@link UGIProvider}.
 */
public class DefaultImpersonator implements Impersonator {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultImpersonator.class);
  private final UGIProvider ugiProvider;
  private final boolean kerberosEnabled;

  @Inject
  @VisibleForTesting
  public DefaultImpersonator(CConfiguration cConf, UGIProvider ugiProvider) {
    this.ugiProvider = ugiProvider;
    this.kerberosEnabled = SecurityUtil.isKerberosEnabled(cConf);
  }

  @Override
  public <T> T doAs(NamespacedEntityId entityId, final Callable<T> callable) throws Exception {
    return doAs(entityId, callable, ImpersonatedOpType.OTHER);
  }

  @Override
  public <T> T doAs(NamespacedEntityId entityId, Callable<T> callable,
                    ImpersonatedOpType impersonatedOpType) throws Exception {
    UserGroupInformation ugi = getUGI(entityId, impersonatedOpType);
    LOG.debug("Performing doAs with UGI {} for entity {} and impersonation operation type", ugi, entityId,
              impersonatedOpType);
    return ImpersonationUtils.doAs(ugi, callable);
  }

  @Override
  public UserGroupInformation getUGI(NamespacedEntityId entityId) throws IOException, NamespaceNotFoundException {
    return getUGI(entityId, ImpersonatedOpType.OTHER);
  }

  private UserGroupInformation getUGI(NamespacedEntityId entityId,
                                      ImpersonatedOpType impersonatedOpType) throws IOException {
    // don't impersonate if kerberos isn't enabled OR if the operation is in the system namespace
    if (!kerberosEnabled || NamespaceId.SYSTEM.equals(entityId.getNamespaceId())) {
      return UserGroupInformation.getCurrentUser();
    }
    return ugiProvider.getConfiguredUGI(new ImpersonationRequest(entityId, impersonatedOpType)).getUGI();
  }
}
