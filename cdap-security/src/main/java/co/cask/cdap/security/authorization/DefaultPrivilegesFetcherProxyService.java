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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesFetcher;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link PrivilegesFetcherProxyService}. It maintains a cache of privileges fetched from
 * the master so every request for privileges does not have to go through the master. The cache is updated periodically
 * using {@link Authorizer}.
 */
class DefaultPrivilegesFetcherProxyService extends AbstractAuthorizationService
  implements PrivilegesFetcherProxyService {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPrivilegesFetcherProxyService.class);

  @Inject
  DefaultPrivilegesFetcherProxyService(
    @Named(AuthorizationEnforcementModule.PRIVILEGES_FETCHER_PROXY) PrivilegesFetcher privilegeFetcher,
    CConfiguration cConf, AuthenticationContext authenticationContext) {
    super(cConf, privilegeFetcher, authenticationContext, "privileges-fetcher-proxy");
  }

  @Override
  public Set<Privilege> listPrivileges(Principal principal) throws Exception {
    ImmutableSet.Builder<Privilege> privileges = ImmutableSet.builder();
    for (Map.Entry<EntityId, Set<Action>> entry : getPrivileges(principal).entrySet()) {
      for (Action action : entry.getValue()) {
        privileges.add(new Privilege(entry.getKey(), action));
      }
    }
    Set<Privilege> result = privileges.build();
    LOG.debug("Fetched privileges for principal {} as {}", principal, result);
    return result;
  }

  @Override
  public void invalidate(Predicate<Principal> predicate) {
    doInvalidate(predicate);
  }
}
