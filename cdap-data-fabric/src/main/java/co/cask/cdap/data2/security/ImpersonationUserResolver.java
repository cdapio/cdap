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

package co.cask.cdap.data2.security;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespacedId;
import com.google.inject.Inject;

/**
 * Helper class to resolve the principal which CDAP will launch programs as.
 * This class should only be used if Kerberos security is enabled and it is for a user namespace.
 */
public class ImpersonationUserResolver {

  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CConfiguration cConf;

  @Inject
  ImpersonationUserResolver(NamespaceQueryAdmin namespaceQueryAdmin, CConfiguration cConf) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.cConf = cConf;
  }

  /**
   * Get impersonation info for a given namespace. If the info configured at the namespace level is empty,
   * returns the info configured at the cdap level.
   *
   * @return configured {@link ImpersonationInfo}.
   */
  public ImpersonationInfo getImpersonationInfo(NamespacedId namespacedId) {
    NamespaceMeta meta;
    try {
      meta = namespaceQueryAdmin.get(Id.Namespace.from(namespacedId.getNamespace()));
    } catch (Exception e) {
      throw new RuntimeException(
        String.format("Failed to retrieve namespace meta for namespace id %s", namespacedId.getNamespace()));
    }
    return getImpersonationInfo(meta);
  }

  /**
   * Get impersonation info for a given namespace. If the info configured at the namespace level is empty,
   * returns the info configured at the cdap level.
   *
   * @return configured {@link ImpersonationInfo}.
   */
  public ImpersonationInfo getImpersonationInfo(NamespaceMeta meta) {
    return new ImpersonationInfo(meta, cConf);
  }
}
