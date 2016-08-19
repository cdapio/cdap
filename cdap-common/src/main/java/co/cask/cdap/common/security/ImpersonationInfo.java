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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;

/**
 * Encapsulates information necessary to impersonate a user - principal and keytab path.
 */
public final class ImpersonationInfo {
  private final String principal;
  private final String keytabURI;

  /**
   * Creates {@link ImpersonationInfo} using the specified principal and keytab path.
   */
  public ImpersonationInfo(String principal, String keytabURI) {
    this.principal = principal;
    this.keytabURI = keytabURI;
  }

  /**
   * Creates {@link ImpersonationInfo} for the specified namespace. If the info is not configured at the namespace
   * level is empty, returns the info configured at the cdap level.
   */
  public ImpersonationInfo(NamespaceMeta namespaceMeta, CConfiguration cConf) {
    NamespaceConfig namespaceConfig = namespaceMeta.getConfig();

    String configuredPrincipal = namespaceConfig.getPrincipal();
    String configuredKeytabURI = namespaceConfig.getKeytabURI();

    if (configuredPrincipal != null && configuredKeytabURI != null) {
      this.principal = configuredPrincipal;
      this.keytabURI = configuredKeytabURI;
    } else if (configuredPrincipal == null && configuredKeytabURI == null) {
      this.principal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
      this.keytabURI = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH);
    } else {
      // Ideally, this should never happen, because we make the check upon namespace create.
      // This can technically happen if someone modifies the namespace store directly (i.e. hbase shell PUT).
      throw new IllegalStateException(
        String.format("Either neither or both of the following two configurations must be configured. " +
                        "Configured principal: %s, Configured keytabURI: %s",
                      configuredPrincipal, configuredKeytabURI));
    }
  }

  public String getPrincipal() {
    return principal;
  }

  public String getKeytabURI() {
    return keytabURI;
  }

  @Override
  public String toString() {
    return "ImpersonationInfo{" +
      "principal='" + principal + '\'' +
      ", keytabURI='" + keytabURI + '\'' +
      '}';
  }
}
