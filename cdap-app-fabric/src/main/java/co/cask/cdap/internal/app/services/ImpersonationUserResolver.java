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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;

import javax.annotation.Nullable;

/**
 * Helper class to resolve the principal which CDAP will launch programs as.
 */
class ImpersonationUserResolver {
  private final NamespaceStore store;

  private final String defaultPrincipal;
  private final String defaultKeytabPath;

  @Inject
  ImpersonationUserResolver(CConfiguration cConf, NamespaceStore store) {
    this.defaultPrincipal = cConf.get(Constants.Security.CFG_CDAP_PROGRAMS_KRB_PRINCIPAL);
    this.defaultKeytabPath = cConf.get(Constants.Security.CFG_CDAP_PROGRAMS_KRB_KEYTAB_PATH);
    this.store = store;
  }

  /**
   * Get Impersonated user at namespace level. If it is empty, returns the default principal.
   *
   * @return configured principal.
   */
  String getPrincipal(ProgramId programId) {
    String namespaceUserSetting = getNamespaceConfig(programId.getNamespaceId()).getPrincipal();
    if (!Strings.isNullOrEmpty(namespaceUserSetting)) {
      return namespaceUserSetting;
    }
    Preconditions.checkNotNull(defaultPrincipal, "Configured principal is null.");
    return defaultPrincipal;
  }

  /**
   * Get configured keytab path at namespace level. If it is empty, returns the default keytab path.
   *
   * @return configured keytab path.
   */
  String getKeytabPath(ProgramId programId) {
    String keytabPathSetting = getNamespaceConfig(programId.getNamespaceId()).getKeytabPath();
    if (!Strings.isNullOrEmpty(keytabPathSetting)) {
      return keytabPathSetting;
    }
    Preconditions.checkNotNull(defaultKeytabPath, "Configured keytab path is null.");
    return defaultKeytabPath;
  }

  private NamespaceConfig getNamespaceConfig(NamespaceId namespaceId) {
    NamespaceMeta meta = store.get(namespaceId.toId());
    Preconditions.checkNotNull(meta,
                               "Failed to retrieve namespace meta for namespace id {}", namespaceId.getNamespace());
    return meta.getConfig();
  }
}
