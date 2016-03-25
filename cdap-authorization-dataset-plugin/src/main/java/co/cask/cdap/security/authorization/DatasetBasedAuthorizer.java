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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.spi.authorization.AbstractAuthorizer;
import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.RoleAlreadyExistsException;
import co.cask.cdap.security.spi.authorization.RoleNotFoundException;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link Authorizer} that uses a dataset to manage ACLs.
 */
public class DatasetBasedAuthorizer extends AbstractAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetBasedAuthorizer.class);
  private AuthorizationContext context;
  private Supplier<ACLDataset> dsSupplier;

  @Override
  public void initialize(final AuthorizationContext context) throws Exception {
    this.context = context;
    this.dsSupplier = new Supplier<ACLDataset>() {
      @Override
      public ACLDataset get() {
        try {
          context.createDataset(ACLDataset.TABLE_NAME, "table", DatasetProperties.EMPTY);
        } catch (InstanceConflictException e) {
          LOG.info("Dataset {} already exists. Not creating again.", ACLDataset.TABLE_NAME);
        } catch (DatasetManagementException e) {
          throw Throwables.propagate(e);
        }
        Table table = context.getDataset(ACLDataset.TABLE_NAME);
        return new ACLDataset(table);
      }
    };
  }

  @Override
  public void enforce(final EntityId entity, final Principal principal,
                      final Action action) throws UnauthorizedException, TransactionFailureException {
    final AtomicReference<Boolean> result = new AtomicReference<>(false);
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        ACLDataset dataset = dsSupplier.get();
        for (EntityId current : entity.getHierarchy()) {
          Set<Action> allowedActions = dataset.search(current, principal);
          if (allowedActions.contains(Action.ALL) || allowedActions.contains(action)) {
            result.set(true);
            return;
          }
        }
      }
    });
    if (!result.get()) {
      throw new UnauthorizedException(principal, action, entity);
    }
  }

  @Override
  public void grant(final EntityId entity, final Principal principal,
                    final Set<Action> actions) throws TransactionFailureException {
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        ACLDataset dataset = dsSupplier.get();
        for (Action action : actions) {
          dataset.add(entity, principal, action);
        }
      }
    });
  }

  @Override
  public void revoke(final EntityId entity, final Principal principal,
                     final Set<Action> actions) throws TransactionFailureException {
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        ACLDataset dataset = dsSupplier.get();
        for (Action action : actions) {
          dataset.remove(entity, principal, action);
        }
      }
    });
  }

  @Override
  public void revoke(final EntityId entity) throws TransactionFailureException {
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        ACLDataset dataset = dsSupplier.get();
        dataset.remove(entity);
      }
    });
  }

  @Override
  public Set<Privilege> listPrivileges(final Principal principal) throws TransactionFailureException {
    final AtomicReference<Set<Privilege>> result = new AtomicReference<>();
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        ACLDataset dataset = dsSupplier.get();
        result.set(dataset.listPrivileges(principal));
      }
    });
    return result.get();
  }

  @Override
  public void createRole(Role role) throws RoleAlreadyExistsException {
    throw new UnsupportedOperationException("Role based operation is not supported.");
  }

  @Override
  public void dropRole(Role role) throws RoleNotFoundException {
    throw new UnsupportedOperationException("Role based operation is not supported.");
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) throws RoleNotFoundException {
    throw new UnsupportedOperationException("Role based operation is not supported.");
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) throws RoleNotFoundException {
    throw new UnsupportedOperationException("Role based operation is not supported.");
  }

  @Override
  public Set<Role> listRoles(Principal principal) {
    throw new UnsupportedOperationException("Role based operation is not supported.");
  }

  @Override
  public Set<Role> listAllRoles() {
    throw new UnsupportedOperationException("Role based operation is not supported.");
  }
}
