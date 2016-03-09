/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link AuthorizationPlugin} that uses a dataset to manage ACLs.
 */
public class DatasetAuthorizationPlugin implements AuthorizationPlugin {

  private final Supplier<ACLDataset> acls;
  private final Supplier<TransactionExecutor> aclsTx;
  private final MultiThreadDatasetCache dsCache;

  @Inject
  public DatasetAuthorizationPlugin(final DatasetFramework dsFramework,
                                    final TransactionExecutorFactory txExecutorFactory,
                                    TransactionSystemClient txClient) {
    this.dsCache = new MultiThreadDatasetCache(
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
  public boolean authorized(final EntityId entity, final String user, final Set<Action> actions) {
    return aclsTx.get().executeUnchecked(new TransactionExecutor.Function<ACLDataset, Boolean>() {
      @Override
      public Boolean apply(ACLDataset acls) throws Exception {
        Set<Action> unfulfilledActions = new HashSet<>(actions);
        for (EntityId current : entity.getHierarchy()) {
          Set<Action> allowedActions = acls.search(current, user);
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
  }

  @Override
  public void grant(final EntityId entity, final String user, final Set<Action> actions) {
    aclsTx.get().executeUnchecked(new TransactionExecutor.Procedure<ACLDataset>() {
      @Override
      public void apply(ACLDataset acls) throws Exception {
        for (Action action : actions) {
          acls.add(entity, user, action);
        }
      }
    }, acls.get());
  }

  @Override
  public void grant(final EntityId entity, final String user) {
    aclsTx.get().executeUnchecked(new TransactionExecutor.Procedure<ACLDataset>() {
      @Override
      public void apply(ACLDataset acls) throws Exception {
        acls.add(entity, user, Action.ALL);
      }
    }, acls.get());
  }

  @Override
  public void revoke(final EntityId entity, final String user, final Set<Action> actions) {
    aclsTx.get().executeUnchecked(new TransactionExecutor.Procedure<ACLDataset>() {
      @Override
      public void apply(ACLDataset acls) throws Exception {
        for (Action action : actions) {
          acls.remove(entity, user, action);
        }
      }
    }, acls.get());
  }

  @Override
  public void revoke(final EntityId entity, final String user) {
    aclsTx.get().executeUnchecked(new TransactionExecutor.Procedure<ACLDataset>() {
      @Override
      public void apply(ACLDataset acls) throws Exception {
        acls.remove(entity, user);
      }
    }, acls.get());
  }

  @Override
  public void revoke(final EntityId entity) {
    aclsTx.get().executeUnchecked(new TransactionExecutor.Procedure<ACLDataset>() {
      @Override
      public void apply(ACLDataset acls) throws Exception {
        acls.remove(entity);
      }
    }, acls.get());
  }
}
