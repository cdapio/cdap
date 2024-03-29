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

import io.cdap.cdap.api.security.AccessException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Facilitates getting UserGroupInformation configured for a given {@link ImpersonationRequest}.
 */
public interface UGIProvider {

  /**
   * Returns a {@link UserGroupInformation} based on the given {@link ImpersonationRequest}.
   *
   * @param impersonationRequest information specifying the entity on which the impersonation is
   *     being performed and the type of the operation
   * @return the {@link UGIWithPrincipal} for the configured user
   */
  UGIWithPrincipal getConfiguredUGI(ImpersonationRequest impersonationRequest)
      throws AccessException;
}
