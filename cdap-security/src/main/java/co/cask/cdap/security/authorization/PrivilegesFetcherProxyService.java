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

package co.cask.cdap.security.authorization;

import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.PrivilegesFetcher;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.Service;

/**
 * A service that runs inside a system service to act as a proxy for requests to list privileges from system services
 * (explore, stream service) or program containers.
 */
public interface PrivilegesFetcherProxyService extends Service, PrivilegesFetcher {
  /**
   * Invalidates privileges of all principals matching the specified {@link Predicate}.
   */
  void invalidate(Predicate<Principal> predicate);
}
