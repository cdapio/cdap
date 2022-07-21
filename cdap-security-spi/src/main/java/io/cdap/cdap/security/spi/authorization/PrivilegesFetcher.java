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

package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Privilege;

import java.util.Set;

/**
 * Fetches {@link Privilege privileges} of the specified {@link Principal}.
 * @deprecated Use {@link GrantFetcher}
 */
@Deprecated
public interface PrivilegesFetcher {

  /**
   * Returns all the {@link Privilege} for the specified {@link Principal}.
   *
   * @param principal the {@link Principal} for which to return privileges
   * @return a {@link Set} of {@link Privilege} for the specified principal
   */
  Set<Privilege> listPrivileges(Principal principal) throws Exception;
}
