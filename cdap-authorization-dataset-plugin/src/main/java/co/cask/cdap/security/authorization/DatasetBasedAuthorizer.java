/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.inject.Inject;

import java.util.Set;

/**
 * {@link Authorizer} that uses a dataset to manage ACLs.
 */
public class DatasetBasedAuthorizer extends DatasetBasedAuthorizationEnforcer implements Authorizer {

  @Inject
  DatasetBasedAuthorizer(final DatasetFramework dsFramework,
                         final TransactionExecutorFactory txExecutorFactory,
                         TransactionSystemClient txClient) {
    super(dsFramework, txExecutorFactory, txClient);
  }

  @Override
  public void grant(final EntityId entity, final Principal principal, final Set<Action> actions) {
    aclsTx.get().executeUnchecked(new TransactionExecutor.Procedure<ACLDataset>() {
      @Override
      public void apply(ACLDataset acls) throws Exception {
        for (Action action : actions) {
          acls.add(entity, principal, action);
        }
      }
    }, acls.get());
  }

  @Override
  public void revoke(final EntityId entity, final Principal principal, final Set<Action> actions) {
    aclsTx.get().executeUnchecked(new TransactionExecutor.Procedure<ACLDataset>() {
      @Override
      public void apply(ACLDataset acls) throws Exception {
        for (Action action : actions) {
          acls.remove(entity, principal, action);
        }
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
