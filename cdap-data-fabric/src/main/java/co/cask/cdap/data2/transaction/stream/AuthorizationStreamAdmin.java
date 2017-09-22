/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.stream;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.SecurityUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.Function;
import com.google.inject.name.Named;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 *
 */
public class AuthorizationStreamAdmin implements StreamAdmin {

  private final StreamAdmin delegate;
  private final AuthenticationContext authenticationContext;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final OwnerAdmin ownerAdmin;

  @Inject
  public AuthorizationStreamAdmin(@Named(StreamAdminModules.NOAUTH_STREAM_ADMIN) StreamAdmin streamAdmin,
                                  AuthenticationContext authenticationContext,
                                  AuthorizationEnforcer authorizationEnforcer,
                                  OwnerAdmin ownerAdmin) {
    this.delegate = streamAdmin;
    this.authenticationContext = authenticationContext;
    this.authorizationEnforcer = authorizationEnforcer;
    this.ownerAdmin = ownerAdmin;
  }

  @Override
  public void dropAllInNamespace(NamespaceId namespace) throws Exception {
    for (StreamSpecification stream : delegate.listStreams(namespace)) {
      ensureAccess(namespace.stream(stream.getName()), Action.ADMIN);
    }
    delegate.dropAllInNamespace(namespace);
  }

  @Override
  public void configureInstances(StreamId streamId, long groupId, int instances) throws Exception {
    delegate.configureInstances(streamId, groupId, instances);
  }

  @Override
  public void configureGroups(StreamId streamId, Map<Long, Integer> groupInfo) throws Exception {
    delegate.configureGroups(streamId, groupInfo);
  }

  @Override
  public void upgrade() throws Exception {
    delegate.upgrade();
  }

  @Override
  public List<StreamSpecification> listStreams(final NamespaceId namespaceId) throws Exception {
   return AuthorizationUtil.isVisible(delegate.listStreams(namespaceId), authorizationEnforcer,
                                      authenticationContext.getPrincipal(),
                                      new Function<StreamSpecification, EntityId>() {
                                        @Override
                                        public EntityId apply(StreamSpecification input) {
                                          return namespaceId.stream(input.getName());
                                        }
                                      }, null);
  }

  @Override
  public StreamConfig getConfig(StreamId streamId) throws IOException {
    return delegate.getConfig(streamId);
  }

  @Override
  public StreamProperties getProperties(StreamId streamId) throws Exception {
    // User should have at least one privilege to read stream properties
    AuthorizationUtil.ensureOnePrivilege(streamId, EnumSet.allOf(Action.class), authorizationEnforcer,
                                         authenticationContext.getPrincipal());
    return delegate.getProperties(streamId);
  }

  @Override
  public void updateConfig(StreamId streamId, StreamProperties properties) throws Exception {
    // User should have admin access on the stream to update its configuration
    ensureAccess(streamId, Action.ADMIN);
    delegate.updateConfig(streamId, properties);
  }

  @Override
  public boolean exists(StreamId streamId) throws Exception {
    AuthorizationUtil.ensureAccess(streamId, authorizationEnforcer, authenticationContext.getPrincipal());
    return delegate.exists(streamId);
  }

  @Nullable
  @Override
  public StreamConfig create(StreamId streamId) throws Exception {
    return create(streamId, new Properties());
  }

  @Nullable
  @Override
  public StreamConfig create(StreamId streamId, @Nullable Properties props) throws Exception {
    String specifiedOwnerPrincipal = props != null && props.containsKey(Constants.Security.PRINCIPAL) ?
      props.getProperty(Constants.Security.PRINCIPAL) : null;

    // need to enforce on the principal id if impersonation is involved
    KerberosPrincipalId effectiveOwner = SecurityUtil.getEffectiveOwner(ownerAdmin, streamId.getNamespaceId(),
                                                                        specifiedOwnerPrincipal);
    Principal requestingUser = authenticationContext.getPrincipal();
    if (effectiveOwner != null) {
      authorizationEnforcer.enforce(effectiveOwner, requestingUser, Action.ADMIN);
    }
    ensureAccess(streamId, Action.ADMIN);
    return delegate.create(streamId, props);
  }

  @Override
  public void truncate(StreamId streamId) throws Exception {
    // User should have ADMIN access to truncate the stream
    ensureAccess(streamId, Action.ADMIN);
    delegate.truncate(streamId);
  }

  @Override
  public void drop(StreamId streamId) throws Exception {
    // User should have ADMIN access to drop the stream
    ensureAccess(streamId, Action.ADMIN);
    delegate.drop(streamId);
  }

  @Override
  public boolean createOrUpdateView(StreamViewId viewId, ViewSpecification spec) throws Exception {
    AuthorizationUtil.ensureAccess(viewId.getParent(), authorizationEnforcer, authenticationContext.getPrincipal());
    return delegate.createOrUpdateView(viewId, spec);
  }

  @Override
  public void deleteView(StreamViewId viewId) throws Exception {
    AuthorizationUtil.ensureAccess(viewId.getParent(), authorizationEnforcer, authenticationContext.getPrincipal());
    delegate.deleteView(viewId);
  }

  @Override
  public List<StreamViewId> listViews(StreamId streamId) throws Exception {
    AuthorizationUtil.ensureAccess(streamId, authorizationEnforcer, authenticationContext.getPrincipal());
    return delegate.listViews(streamId);
  }

  @Override
  public ViewSpecification getView(StreamViewId viewId) throws Exception {
    AuthorizationUtil.ensureAccess(viewId.getParent(), authorizationEnforcer, authenticationContext.getPrincipal());
    return delegate.getView(viewId);
  }

  @Override
  public boolean viewExists(StreamViewId viewId) throws Exception {
    AuthorizationUtil.ensureAccess(viewId.getParent(), authorizationEnforcer, authenticationContext.getPrincipal());
    return delegate.viewExists(viewId);
  }

  @Override
  public void register(Iterable<? extends EntityId> owners, StreamId streamId) {
    delegate.register(owners, streamId);
  }

  @Override
  public void addAccess(ProgramRunId run, StreamId streamId, AccessType accessType) {
    delegate.addAccess(run, streamId, accessType);
  }

  private <T extends EntityId> void ensureAccess(T entityId, Action action) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(entityId, principal, action);
  }
}
