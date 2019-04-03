/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteOpsClient;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Privilege;
import io.cdap.cdap.security.spi.authorization.PrivilegesManager;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Set;

/**
 * Class that modifies privileges on {@link EntityId entities} by making HTTP Requests to the Master. This is required
 * because some authorization backends (e.g. Apache Sentry) do not support proxy authentication. Hence system
 * containers like stream and explore service cannot interact with them directly.
 */
public class RemotePrivilegesManager extends RemoteOpsClient implements PrivilegesManager {
  private static final Logger LOG = LoggerFactory.getLogger(RemotePrivilegesManager.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();
  private static final Type SET_PRIVILEGES_TYPE = new TypeToken<Set<Privilege>>() { }.getType();

  @Inject
  RemotePrivilegesManager(DiscoveryServiceClient discoveryClient) {
    super(discoveryClient, Constants.Service.APP_FABRIC_HTTP);
  }

  @Override
  public void grant(Authorizable authorizable, Principal principal, Set<Action> actions) throws Exception {
    LOG.trace("Making request to grant {} on {} to {}", actions, authorizable, principal);
    executeRequest("grant", authorizable, principal, actions);
    LOG.debug("Granted {} on {} to {} successfully", actions, authorizable, principal);
  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal, Set<Action> actions) throws Exception {
    LOG.trace("Making request to revoke {} on {} to {}", actions, authorizable, principal);
    executeRequest("revoke", authorizable, principal, actions);
    LOG.debug("Revoked {} on {} to {} successfully", actions, authorizable, principal);
  }

  @Override
  public void revoke(Authorizable authorizable) throws Exception {
    LOG.trace("Making request to revoke all actions on {}", authorizable);
    executeRequest("revokeAll", authorizable);
    LOG.debug("Revoked all actions on {} successfully", authorizable);
  }

  @Override
  public Set<Privilege> listPrivileges(Principal principal) throws Exception {
    LOG.trace("Listing privileges for {}", principal);
    HttpResponse httpResponse = executeRequest("listPrivileges", principal);
    String responseBody = httpResponse.getResponseBodyAsString();
    LOG.debug("List privileges response for principal {}: {}", principal, responseBody);
    return GSON.fromJson(responseBody, SET_PRIVILEGES_TYPE);
  }
}
