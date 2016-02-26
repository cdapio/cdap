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

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Set;

/**
 * Implementation of {@link AuthorizationEnforcer} that uses a {@link ACLDataset} as its backing data store.
 */
public class DatasetBasedAuthorizationEnforcer implements AuthorizationEnforcer {

  protected final Supplier<ACLDataset> acls;
  protected final Supplier<TransactionExecutor> aclsTx;

  @Inject
  DatasetBasedAuthorizationEnforcer(final DatasetFramework dsFramework,
                                    final TransactionExecutorFactory txExecutorFactory,
                                    TransactionSystemClient txClient) {
    final MultiThreadDatasetCache dsCache = new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(dsFramework, null, null), txClient,
      new NamespaceId(ACLDataset.ID.getNamespace().getId()), null, null, null);
    this.acls =
      new Supplier<ACLDataset>() {
        @Override
        public ACLDataset get() {
          Table table;
          try {
            table = dsCache.getDataset(ACLDataset.ID.getId());
          } catch (DatasetInstantiationException e) {
            try {
              table = DatasetsUtil.getOrCreateDataset(
                dsFramework, ACLDataset.ID, "table",
                DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
            } catch (DatasetManagementException | IOException e1) {
              throw Throwables.propagate(e1);
            }
          }
          return new ACLDataset(table);
        }
      };
    this.aclsTx = new Supplier<TransactionExecutor>() {
      @Override
      public TransactionExecutor get() {
        return txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) acls.get()));
      }
    };
  }

  @Override
  public void enforce(final EntityId entity, final Principal principal,
                      final Action action) throws UnauthorizedException {
    boolean allowed = aclsTx.get().executeUnchecked(new TransactionExecutor.Function<ACLDataset, Boolean>() {
      @Override
      public Boolean apply(ACLDataset acls) throws Exception {
        Set<Action> unfulfilledActions = Sets.newHashSet(action);
        for (EntityId current : entity.getHierarchy()) {
          Set<Action> allowedActions = acls.search(current, principal);
          if (allowedActions.contains(Action.ALL)) {
            return true;
          }
          unfulfilledActions.removeAll(allowedActions);
          if (unfulfilledActions.isEmpty()) {
            return true;
          }
        }
        return unfulfilledActions.isEmpty();
      }
    }, acls.get());
    if (!allowed) {
      throw new UnauthorizedException(String.format("User %s not allowed to perform action %s on entity %s",
                                                    principal, action, entity));
    }
  }
}
