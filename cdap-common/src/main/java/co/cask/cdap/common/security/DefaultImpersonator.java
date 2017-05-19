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

package co.cask.cdap.common.security;

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
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

  private final CConfiguration cConf;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final boolean kerberosEnabled;
  private final UGIProvider ugiProvider;

  @Inject
  @VisibleForTesting
  public DefaultImpersonator(CConfiguration cConf, UGIProvider ugiProvider,
                             NamespaceQueryAdmin namespaceQueryAdmin) {
    this.cConf = cConf;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.ugiProvider = ugiProvider;
    this.kerberosEnabled = SecurityUtil.isKerberosEnabled(cConf);
  }

  @Override
  public <T> T doAs(NamespaceId namespaceId, final Callable<T> callable) throws Exception {
    return ImpersonationUtils.doAs(getUGI(namespaceId), callable);
  }

  @Override
  public <T> T doAs(NamespaceMeta namespaceMeta, final Callable<T> callable) throws Exception {
    return ImpersonationUtils.doAs(getUGI(namespaceMeta), callable);
  }

  @Override
  public UserGroupInformation getUGI(NamespaceId namespaceId) throws IOException, NamespaceNotFoundException {
    // don't impersonate if kerberos isn't enabled OR if the operation is in the system namespace
    if (!kerberosEnabled || NamespaceId.SYSTEM.equals(namespaceId)) {
      return UserGroupInformation.getCurrentUser();
    }
    try {
      return getUGI(namespaceQueryAdmin.get(namespaceId.toId()));
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Retrieve the {@link UserGroupInformation} for the given {@link NamespaceMeta}
   *
   * @param namespaceMeta the {@link NamespaceMeta metadata} of the namespace to lookup the user
   * @return {@link UserGroupInformation}
   * @throws IOException if there was any error fetching the {@link UserGroupInformation}
   */
  private UserGroupInformation getUGI(NamespaceMeta namespaceMeta) throws IOException {
    // don't impersonate if kerberos isn't enabled OR if the operation is in the system namespace
    if (!kerberosEnabled || NamespaceId.SYSTEM.equals(namespaceMeta.getNamespaceId())) {
      return UserGroupInformation.getCurrentUser();
    }
    return getUGI(new ImpersonationInfo(namespaceMeta, cConf));
  }

  private UserGroupInformation getUGI(ImpersonationInfo impersonationInfo) throws IOException {
    // no need to get a UGI if the current UGI is the one we're requesting; simply return it
    String configuredPrincipalShortName = new KerberosName(impersonationInfo.getPrincipal()).getShortName();
    if (UserGroupInformation.getCurrentUser().getShortUserName().equals(configuredPrincipalShortName)) {
      LOG.debug("Requested UGI {} is same as calling UGI. Simply returning current user: {}",
                impersonationInfo.getPrincipal(), UserGroupInformation.getCurrentUser());
      return UserGroupInformation.getCurrentUser();
    }
    return ugiProvider.getConfiguredUGI(impersonationInfo);
  }
}
